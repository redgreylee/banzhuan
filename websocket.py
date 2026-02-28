import asyncio
import ujson
import zlib
import time
import ssl
import httpx
import os
from collections import defaultdict, deque
import websockets
from python_socks.async_.asyncio import Proxy
from aiohttp import web

# ================= 1. 核心配置 =================
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7891, 7911)]
THRESHOLD = 0.005       # 0.5% 利差告警
SCAN_INTERVAL = 0.5     
STATUS_INTERVAL = 3     
STALE_THRESHOLD = 25.0  
WEB_PORT = 8080         

SUB_LIMITS = {
    "Binance": 50, "OKX": 50, "Bybit": 10,   
    "Gate": 50, "Bitget": 40, "Bitmart": 50
}

# ================= 2. 状态监控 =================
# 数据结构升级：存储 (买价, 买量, 卖价, 卖量, 时间戳)
prices_pool = defaultdict(dict)
arbitrage_opportunities = deque(maxlen=100) 

class GlobalStats:
    def __init__(self):
        self.msg_count = 0
        self.active_conns = 0
        self.start_time = time.time()

class ConnectionTracker:
    def __init__(self):
        self.conns = {}

    def update(self, wid, **kwargs):
        if wid not in self.conns:
            self.conns[wid] = {"status": "INIT", "latency": 0, "last_msg": time.time(), "errors": 0, "ex": ""}
        self.conns[wid].update(kwargs)

stats = GlobalStats()
conn_tracker = ConnectionTracker()

# ================= 3. 连接器 (保持原样) =================
async def create_proxy_ws(uri, proxy_url):
    try:
        proxy = Proxy.from_url(proxy_url)
        host_port = uri.split("://")[1].split("/")[0]
        dest_host, dest_port = host_port.split(":") if ":" in host_port else (host_port, 443)
        
        sock = await asyncio.wait_for(proxy.connect(dest_host, int(dest_port)), timeout=8)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return await websockets.connect(uri, sock=sock, ssl=ssl_context, ping_interval=None)
    except Exception as e:
        raise ConnectionError(f"Proxy Connect Error")

# ================= 4. 协议处理器 (升级为 BBO 频道) =================
class ExchangeHandler:
    def __init__(self, name, symbol_map):
        self.name = name
        self.symbol_map = symbol_map
        self.rev_map = {v: k for k, v in symbol_map.items()}
        self.uris = {
            "Binance": "wss://stream.binance.com:9443/ws",
            "OKX": "wss://ws.okx.com:8443/ws/v5/public",
            "Bybit": "wss://stream.bybit.com/v5/public/spot",
            "Gate": "wss://api.gateio.ws/ws/v4/",
            "Bitget": "wss://ws.bitget.com/v2/ws/public",
            "Bitmart": "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
        }

    def get_sub_payload(self):
        syms = list(self.symbol_map.values())
        # 核心变动：将 ticker 频道更换为能获取数量的 bbo/bookTicker/depth1 频道
        if self.name == "Binance": return {"method": "SUBSCRIBE", "params":[f"{s.lower()}@bookTicker" for s in syms], "id": 1}
        if self.name == "OKX": return {"op": "subscribe", "args":[{"channel": "bbo-tbt", "instId": s} for s in syms]}
        if self.name == "Bybit": return {"op": "subscribe", "args":[f"orderbook.1.{s}" for s in syms]}
        if self.name == "Gate": return {"channel": "spot.book_ticker", "event": "subscribe", "payload": syms}
        if self.name == "Bitget": return {"op": "subscribe", "args":[{"instType": "SPOT", "channel": "books1", "instId": s} for s in syms]}
        if self.name == "Bitmart": return {"op": "subscribe", "args":[f"spot/ticker:{s}" for s in syms]}
        return {}

    def parse(self, raw_msg):
        stats.msg_count += 1
        try:
            if self.name == "Bitmart" and isinstance(raw_msg, bytes):
                raw_msg = zlib.decompress(raw_msg, 16 + zlib.MAX_WBITS).decode('utf-8')
            data = ujson.loads(raw_msg)
            s, bp, bq, ap, aq = None, None, None, None, None

            if self.name == "Binance" and 'u' in data: 
                s, bp, bq, ap, aq = data['s'], data['b'], data['B'], data['a'], data['A']
            elif self.name == "OKX" and 'data' in data:
                d = data['data'][0]
                s, bp, bq, ap, aq = d['instId'], d['bids'][0][0], d['bids'][0][1], d['asks'][0][0], d['asks'][0][1]
            elif self.name == "Bybit" and 'data' in data:
                d = data['data']
                s = d.get('s')
                if 'b' in d and d['b']: bp, bq = d['b'][0][0], d['b'][0][1]
                if 'a' in d and d['a']: ap, aq = d['a'][0][0], d['a'][0][1]
            elif self.name == "Gate" and 'result' in data:
                d = data['result']
                if isinstance(d, dict) and 's' in d: s, bp, bq, ap, aq = d['s'], d['b'], d['B'], d['a'], d['A']
            elif self.name == "Bitget" and 'data' in data:
                d = data['data'][0]
                s, bp, bq, ap, aq = d['instId'], d['bids'][0][0], d['bids'][0][1], d['asks'][0][0], d['asks'][0][1]
            elif self.name == "Bitmart" and 'data' in data:
                items = data['data'] if isinstance(data['data'], list) else [data['data']]
                for item in items: self._update_pool(item['symbol'], item['bid_px'], item['bid_sz'], item['ask_px'], item['ask_sz'])
                return
            
            if s: self._update_pool(s, bp, bq, ap, aq)
        except Exception: pass

    def _update_pool(self, ex_s, bp, bq, ap, aq):
        norm = self.rev_map.get(ex_s)
        if norm and bp and ap:
            # 存储格式: 买价, 买量, 卖价, 卖量, 时间
            prices_pool[norm][self.name] = (float(bp), float(bq), float(ap), float(aq), time.time())

# ================= 5. 工作协程 (严格保持原代理逻辑) =================
async def ws_worker(ex_name, sym_map, p_idx):
    wid = f"{ex_name[:3]}-{p_idx:02d}"
    handler = ExchangeHandler(ex_name, sym_map)
    while True:
        # ！！！这里是你要求的原有代理池逻辑 ！！！
        proxy_url = PROXIES[p_idx % len(PROXIES)]
        conn_tracker.update(wid, ex=ex_name, status="CONN")
        try:
            async with await create_proxy_ws(handler.uris[ex_name], proxy_url) as ws:
                stats.active_conns += 1
                conn_tracker.update(wid, status="ONLINE", last_msg=time.time())
                await ws.send(ujson.dumps(handler.get_sub_payload()))
                last_hb = time.time()
                while True:
                    try:
                        start_t = time.perf_counter()
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        handler.parse(msg)
                        conn_tracker.update(wid, last_msg=time.time(), latency=(time.perf_counter()-start_t)*1000)
                    except asyncio.TimeoutError: pass
                    
                    if time.time() - last_hb > 20:
                        await ws.ping(); last_hb = time.time()
                    if time.time() - conn_tracker.conns[wid]['last_msg'] > STALE_THRESHOLD:
                        raise ConnectionError("Stalled")
        except Exception as e:
            conn_tracker.update(wid, status="LOST", errors=conn_tracker.conns.get(wid, {}).get('errors', 0)+1)
            await asyncio.sleep(5)
        finally:
            stats.active_conns = max(0, stats.active_conns - 1)

# ================= 6. 核心扫描器 (适配买卖逻辑) =================
async def banzhuan_scanner():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now = time.time()
        for sym, ex_dict in list(prices_pool.items()):
            # 过滤 3 秒内有效的数据
            valid = {ex: data for ex, data in ex_dict.items() if now - data[4] < 3.0}
            if len(valid) < 2: continue
            
            # 买入：吃对方的卖单 (Ask Price)；卖出：砸向对方的买单 (Bid Price)
            best_buy_ex = min(valid.keys(), key=lambda x: valid[x][2])  # 最小卖价
            best_sell_ex = max(valid.keys(), key=lambda x: valid[x][0]) # 最大买价
            
            buy_p, buy_q = valid[best_buy_ex][2], valid[best_buy_ex][3]
            sell_p, sell_q = valid[best_sell_ex][0], valid[best_sell_ex][1]
            
            if buy_p <= 0: continue
            diff = (sell_p - buy_p) / buy_p
            
            if diff >= THRESHOLD and best_buy_ex != best_sell_ex:
                arbitrage_opportunities.appendleft({
                    "time": time.strftime("%H:%M:%S"),
                    "symbol": sym,
                    "diff": round(diff * 100, 2),
                    "buy_ex": best_buy_ex, "buy_price": buy_p, "buy_qty": buy_q,
                    "sell_ex": best_sell_ex, "sell_price": sell_p, "sell_qty": sell_q
                })

# ================= 7. 控制台报告 (保持原样) =================
async def status_report():
    while True:
        await asyncio.sleep(STATUS_INTERVAL)
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"═══ 📊 搬砖监控 (代理: {len(PROXIES)}端口) Web: http://127.0.0.1:{WEB_PORT} ═══ TPS: {stats.msg_count/(time.time()-stats.start_time):.1f}")
        print(f"{'WorkerID':<12} | {'Stat':<6} | {'Lat':<7} | {'LastIn':<7} | {'Err'}")
        print("-" * 55)
        for wid, info in list(conn_tracker.conns.items())[:20]:
            mark = "✅" if info['status']=="ONLINE" else "❌"
            print(f"{wid:<12} | {mark} {info['status']:<4} | {info['latency']:>4.0f}ms | {time.time()-info['last_msg']:>5.1f}s | {info['errors']}")

# ================= 8. Web 面板 (新增数量列) =================
async def web_api_data(request):
    return web.json_response({
        "tps": round(stats.msg_count / (time.time() - stats.start_time), 1),
        "uptime": round(time.time() - stats.start_time, 0),
        "online_workers": stats.active_conns,
        "total_workers": len(conn_tracker.conns),
        "opportunities": list(arbitrage_opportunities)
    })

async def web_index(request):
    html = """
    <!DOCTYPE html><html lang="zh" data-bs-theme="dark"><head><meta charset="UTF-8"><title>Arbitrage Radar</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style> body { background:#121212; } .qty { font-size: 0.8em; color: #888; } </style></head>
    <body class="p-4"><div class="container-fluid">
        <h3>⚡ 跨所高频监控 <small class="fs-6 text-muted">TPS: <span id="tps">0</span></small></h3>
        <table class="table table-dark table-striped mt-4">
            <thead><tr><th>时间</th><th>交易对</th><th>利差</th><th>买入平台/价格/数量</th><th>卖出平台/价格/数量</th></tr></thead>
            <tbody id="opp-tbody"></tbody>
        </table>
    </div>
    <script>
        async function update() {
            const r = await fetch('/api/data'); const d = await r.json();
            document.getElementById('tps').innerText = d.tps;
            document.getElementById('opp-tbody').innerHTML = d.opportunities.map(o => `
                <tr><td>${o.time}</td><td><b>${o.symbol}</b></td><td class="text-warning">+${o.diff}%</td>
                <td><span class="badge bg-success">${o.buy_ex}</span> ${o.buy_price} <span class="qty">量:${o.buy_qty}</span></td>
                <td><span class="badge bg-danger">${o.sell_ex}</span> ${o.sell_price} <span class="qty">量:${o.sell_qty}</span></td></tr>
            `).join('');
        }
        setInterval(update, 1000);
    </script></body></html>
    """
    return web.Response(text=html, content_type='text/html')

async def main():
    # 保持原有的多交易所初始化逻辑
    endpoints = {
        "Binance": "https://api.binance.com/api/v3/exchangeInfo",
        "OKX": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "Bybit": "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "Gate": "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "Bitget": "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitmart": "https://api-cloud.bitmart.com/spot/v1/symbols"
    }

    async with httpx.AsyncClient(proxy=PROXIES[0], verify=False) as client:
        results = await asyncio.gather(*[fetch_symbols(client, n, u) for n, u in endpoints.items()])
        ex_data = dict(zip(endpoints.keys(), results))

    all_symbols = set()
    for d in ex_data.values(): all_symbols.update(d.keys())
    watchlist = [s for s in all_symbols if sum(1 for ex in ex_data if s in ex_data[ex]) >= 2]
    
    app = web.Application(); app.router.add_get('/', web_index); app.router.add_get('/api/data', web_api_data)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', WEB_PORT).start()

    tasks = [banzhuan_scanner(), status_report()]
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            # ！！！ 这里继续传递 p_global_idx 确保代理轮询生效 ！！！
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1
    
    await asyncio.gather(*tasks)
async def fetch_symbols(client, name, url):
    try:
        resp = await client.get(url, timeout=15)
        resp.raise_for_status() # 确保状态码为 200
        data = resp.json()
        pairs = {}

        if name == "Binance":
            for s in data.get('symbols', []):
                # 过滤交易状态和报价货币
                if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT':
                    pairs[s['symbol']] = s['symbol'] # 结果示例: {"BTCUSDT": "BTCUSDT"}

        elif name == "OKX":
            for s in data.get('data', []):
                if s['state'] == 'live' and s['quoteCcy'] == 'USDT':
                    # OKX 是 BTC-USDT 格式
                    norm = s['instId'].replace("-", "")
                    pairs[norm] = s['instId']

        elif name == "Bybit":
            for s in data.get('result', {}).get('list', []):
                if s['quoteCoin'] == 'USDT':
                    pairs[s['symbol']] = s['symbol']

        elif name == "Gate":
            # Gate 的 API v4 通常直接返回数组
            for s in data:
                if isinstance(s, dict) and s.get('quote') == 'USDT':
                    norm = s['id'].replace("_", "")
                    pairs[norm] = s['id']

        elif name == "Bitget":
            for s in data.get('data', []):
                if s.get('quoteCoin') == 'USDT':
                    pairs[s['symbol']] = s['symbol']

        elif name == "Bitmart":
            # 适配 Bitmart v1 常见的返回格式
            sym_list = data.get('data', {}).get('symbols', [])
            for s in sym_list:
                if '_USDT' in s:
                    pairs[s.replace("_", "")] = s
                    
        return pairs
    except Exception as e:
        # 至少打印一下哪个交易所初始化失败了，方便排查代理问题
        print(f"⚠️ [Init] {name} fetch failed: {e}")
        return {}
if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())