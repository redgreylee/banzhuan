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
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7891, 7901)] # 示例 10 个代理端口
THRESHOLD = 0.005       # 0.5% 利差告警
SCAN_INTERVAL = 0.5     
STATUS_INTERVAL = 3     
STALE_THRESHOLD = 20.0  
WEB_PORT = 8080         

# 每个 WS 连接订阅的交易对上限
SUB_LIMITS = {
    "Binance": 50, "OKX": 50, "Bybit": 20, 
    "Gate": 50, "Bitget": 40, "Bitmart": 30
}

# ================= 2. 全局状态存储 =================
prices_pool = defaultdict(dict) # { "BTCUSDT": { "Binance": (bp, bq, ap, aq, ts), ... } }
arbitrage_opportunities = deque(maxlen=50) 

class GlobalStats:
    def __init__(self):
        self.msg_count = 0
        self.active_conns = 0
        self.start_time = time.time()

class ConnectionTracker:
    def __init__(self):
        self.conns = {} # { wid: {status, latency, last_msg, errors, ex} }

    def update(self, wid, **kwargs):
        if wid not in self.conns:
            self.conns[wid] = {"status": "INIT", "latency": 0, "last_msg": time.time(), "errors": 0, "ex": ""}
        self.conns[wid].update(kwargs)

stats = GlobalStats()
conn_tracker = ConnectionTracker()

# ================= 3. 底层连接器 =================
async def create_proxy_ws(uri, proxy_url):
    try:
        proxy = Proxy.from_url(proxy_url)
        host_port = uri.split("://")[1].split("/")[0]
        dest_host, dest_port = host_port.split(":") if ":" in host_port else (host_port, 443)
        
        sock = await asyncio.wait_for(proxy.connect(dest_host, int(dest_port)), timeout=10)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return await websockets.connect(uri, sock=sock, ssl=ssl_context, ping_interval=None)
    except Exception as e:
        raise ConnectionError(f"Proxy Connect Failed: {e}")

# ================= 4. 协议处理器 (补全所有解析逻辑) =================
class ExchangeHandler:
    def __init__(self, name, symbol_map):
        self.name = name
        self.symbol_map = symbol_map # { "BTCUSDT": "BTC-USDT" }
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
                for i in items: self._update_pool(i['symbol'], i['bid_px'], i['bid_sz'], i['ask_px'], i['ask_sz'])
                return
            
            if s: self._update_pool(s, bp, bq, ap, aq)
        except: pass

    def _update_pool(self, ex_s, bp, bq, ap, aq):
        norm = self.rev_map.get(ex_s)
        if norm and bp and ap:
            prices_pool[norm][self.name] = (float(bp), float(bq), float(ap), float(aq), time.time())

# ================= 5. 工作协程 =================
async def ws_worker(ex_name, sym_map, p_idx):
    wid = f"{ex_name[:3]}-{p_idx:02d}"
    handler = ExchangeHandler(ex_name, sym_map)
    while True:
        proxy_url = PROXIES[p_idx % len(PROXIES)]
        conn_tracker.update(wid, ex=ex_name, status="CONN")
        try:
            async with await create_proxy_ws(handler.uris[ex_name], proxy_url) as ws:
                stats.active_conns += 1
                conn_tracker.update(wid, status="ONLINE", last_msg=time.time())
                await ws.send(ujson.dumps(handler.get_sub_payload()))
                
                while True:
                    start_t = time.perf_counter()
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        handler.parse(msg)
                        conn_tracker.update(wid, last_msg=time.time(), latency=(time.perf_counter()-start_t)*1000)
                    except asyncio.TimeoutError:
                        await ws.ping()
                    
                    if time.time() - conn_tracker.conns[wid]['last_msg'] > STALE_THRESHOLD:
                        raise ConnectionError("No data flow")
        except Exception:
            conn_tracker.update(wid, status="LOST", errors=conn_tracker.conns[wid]['errors']+1)
            await asyncio.sleep(8)
        finally:
            stats.active_conns = max(0, stats.active_conns - 1)

# ================= 6. 核心套利扫描器 =================
async def banzhuan_scanner():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now = time.time()
        for sym, ex_dict in list(prices_pool.items()):
            # 只比较 3 秒内更新过的数据
            valid = {ex: data for ex, data in ex_dict.items() if now - data[4] < 3.0}
            if len(valid) < 2: continue
            
            # 策略：低价卖一买入 -> 高价买一卖出
            best_buy_ex = min(valid.keys(), key=lambda x: valid[x][2])  # 谁卖得最便宜
            best_sell_ex = max(valid.keys(), key=lambda x: valid[x][0]) # 谁收得最贵
            
            if best_buy_ex == best_sell_ex: continue

            buy_p, buy_q = valid[best_buy_ex][2], valid[best_buy_ex][3]
            sell_p, sell_q = valid[best_sell_ex][0], valid[best_sell_ex][1]
            
            diff = (sell_p - buy_p) / buy_p
            if diff >= THRESHOLD:
                arbitrage_opportunities.appendleft({
                    "time": time.strftime("%H:%M:%S"),
                    "symbol": sym, "diff": round(diff * 100, 2),
                    "buy_ex": best_buy_ex, "buy_price": buy_p, "buy_qty": buy_q,
                    "sell_ex": best_sell_ex, "sell_price": sell_p, "sell_qty": sell_q
                })

# ================= 7. Web 面板与 API =================
async def web_api_data(request):
    return web.json_response({
        "tps": round(stats.msg_count / (time.time() - stats.start_time + 0.1), 1),
        "uptime": int(time.time() - stats.start_time),
        "online_workers": stats.active_conns,
        "total_workers": len(conn_tracker.conns),
        "workers": [ {**v, "wid": k} for k, v in conn_tracker.conns.items() ],
        "opportunities": list(arbitrage_opportunities)
    })

async def web_index(request):
    html = """
    <!DOCTYPE html><html lang="zh" data-bs-theme="dark"><head><meta charset="UTF-8">
    <title>Arbitrage Engine</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background:#0a0a0a; color: #eee; font-family: sans-serif; }
        .stat-card { background: #151515; border: 1px solid #333; border-radius: 8px; padding: 15px; margin-bottom: 20px; }
        .badge-online { background: #28a745; } .badge-offline { background: #dc3545; }
        .qty-text { font-size: 0.8em; color: #888; }
        .scroll-box { max-height: 500px; overflow-y: auto; }
    </style></head>
    <body class="p-4"><div class="container-fluid">
        <div class="row g-3">
            <div class="col-md-2"><div class="stat-card"><h6>TPS</h6><h3 id="tps" class="text-info">-</h3></div></div>
            <div class="col-md-2"><div class="stat-card"><h6>在线/总连接</h6><h3><span id="on_cnt" class="text-success">-</span>/<span id="total_cnt">-</span></h3></div></div>
            <div class="col-md-2"><div class="stat-card"><h6>运行时间</h6><h3 id="uptime">-</h3></div></div>
        </div>
        <div class="row">
            <div class="col-md-4">
                <h5>📡 节点监控</h5>
                <div class="stat-card scroll-box"><table class="table table-dark table-sm table-hover">
                    <thead><tr><th>ID</th><th>交易所</th><th>状态</th><th>延迟</th></tr></thead>
                    <tbody id="worker-tbody"></tbody>
                </table></div>
            </div>
            <div class="col-md-8">
                <h5>⚡ 实时利差机会 (Threshold: 0.5%)</h5>
                <div class="stat-card"><table class="table table-dark table-striped">
                    <thead><tr><th>时间</th><th>币种</th><th>利差</th><th>买入端 (Low)</th><th>卖出端 (High)</th></tr></thead>
                    <tbody id="opp-tbody"></tbody>
                </table></div>
            </div>
        </div>
    </div>
    <script>
        async function update() {
            try {
                const r = await fetch('/api/data'); const d = await r.json();
                document.getElementById('tps').innerText = d.tps;
                document.getElementById('on_cnt').innerText = d.online_workers;
                document.getElementById('total_cnt').innerText = d.total_workers;
                document.getElementById('uptime').innerText = d.uptime + 's';
                
                document.getElementById('worker-tbody').innerHTML = d.workers.map(w => `
                    <tr><td>${w.wid}</td><td>${w.ex}</td>
                    <td><span class="badge ${w.status==='ONLINE'?'badge-online':'badge-offline'}">${w.status}</span></td>
                    <td>${w.latency.toFixed(0)}ms</td></tr>`).join('');
                
                document.getElementById('opp-tbody').innerHTML = d.opportunities.map(o => `
                    <tr><td>${o.time}</td><td><strong>${o.symbol}</strong></td>
                    <td class="text-warning">+${o.diff}%</td>
                    <td><span class="badge bg-success">${o.buy_ex}</span> ${o.buy_price}<br><span class="qty-text">量: ${o.buy_qty}</span></td>
                    <td><span class="badge bg-danger">${o.sell_ex}</span> ${o.sell_price}<br><span class="qty-text">量: ${o.sell_qty}</span></td></tr>`).join('');
            } catch(e) {}
        }
        setInterval(update, 1000);
    </script></body></html>
    """
    return web.Response(text=html, content_type='text/html')

# ================= 8. 初始化与启动 =================
async def fetch_symbols(client, name, url):
    try:
        resp = await client.get(url, timeout=15); resp.raise_for_status(); data = resp.json(); pairs = {}
        if name == "Binance": 
            for s in data['symbols']: 
                if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT': pairs[s['symbol']] = s['symbol']
        elif name == "OKX":
            for s in data['data']:
                if s['state'] == 'live' and s['quoteCcy'] == 'USDT': pairs[s['instId'].replace("-","")] = s['instId']
        elif name == "Bybit":
            for s in data['result']['list']:
                if s['quoteCoin'] == 'USDT': pairs[s['symbol']] = s['symbol']
        elif name == "Gate":
            for s in data:
                if s.get('quote') == 'USDT': pairs[s['id'].replace("_","")] = s['id']
        elif name == "Bitget":
            for s in data['data']:
                if s['quoteCoin'] == 'USDT': pairs[s['symbol']] = s['symbol']
        elif name == "Bitmart":
            syms = data.get('data', {}).get('symbols', [])
            for s in syms:
                if '_USDT' in s: pairs[s.replace("_","")] = s
        return pairs
    except Exception as e:
        print(f"Failed to fetch {name}: {e}")
        return {}

async def main():
    endpoints = {
        "Binance": "https://api.binance.com/api/v3/exchangeInfo",
        "OKX": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "Bybit": "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "Gate": "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "Bitget": "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitmart": "https://api-cloud.bitmart.com/spot/v1/symbols"
    }

    print("🚀 初始化市场数据...")
    async with httpx.AsyncClient(proxy=PROXIES[0], verify=False) as client:
        results = await asyncio.gather(*[fetch_symbols(client, n, u) for n, u in endpoints.items()])
        ex_data = dict(zip(endpoints.keys(), results))

    all_symbols = set()
    for d in ex_data.values(): all_symbols.update(d.keys())
    watchlist = [s for s in all_symbols if sum(1 for ex in ex_data if s in ex_data[ex]) >= 2]
    print(f"✅ 找到 {len(watchlist)} 个共有交易对")

    app = web.Application(); app.router.add_get('/', web_index); app.router.add_get('/api/data', web_api_data)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', WEB_PORT).start()

    tasks = [banzhuan_scanner()]
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1
    
    print(f"🔥 启动 {len(tasks)-1} 个监控工作线程. Web 面板: http://127.0.0.1:{WEB_PORT}")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass