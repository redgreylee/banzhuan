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
from dataclasses import dataclass

# ================= 1. 核心配置 =================
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7890, 7891)] 
THRESHOLD = 0.005       
SCAN_INTERVAL = 0.2     
STATUS_INTERVAL = 3     
STALE_THRESHOLD = 20.0  
WEB_PORT = 8080         

SUB_LIMITS = {
    "Binance": 50, "OKX": 50, "Bybit": 10,   
    "Gate": 50, "Bitget": 40, "Bitmart": 50
}

# 增加 bid_sz (买单量) 和 ask_sz (卖单量)
@dataclass
class BBO:
    bid_p: float
    bid_sz: float
    ask_p: float
    ask_sz: float
    ts: float

# ================= 2. 状态监控 =================
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

# ================= 3. 连接器 =================
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
        raise ConnectionError(f"Proxy Connect Error: {e}")

# ================= 4. 协议处理器 =================
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
        if self.name == "Binance": return {"method": "SUBSCRIBE", "params":[f"{s.lower()}@bookTicker" for s in syms], "id": 1}
        if self.name == "OKX": return {"op": "subscribe", "args":[{"channel": "bbo-tbt", "instId": s} for s in syms]}
        if self.name == "Bybit": return {"op": "subscribe", "args":[f"orderbook.1.{s}" for s in syms]}
        if self.name == "Gate": return {"channel": "spot.book_ticker", "event": "subscribe", "payload": syms}
        if self.name == "Bitget": return {"op": "subscribe", "args":[{"instType": "SPOT", "channel": "books1", "instId": s} for s in syms]}
        if self.name == "Bitmart": return {"op": "subscribe", "args":[f"spot/ticker:{s}" for s in syms]}
        return {}

    def parse(self, raw_msg):
        stats.msg_count += 1
        if raw_msg == "pong": return
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
                # Bybit 增量推送时可能缺少某一边
                if 'b' in d and len(d['b']) > 0: bp, bq = d['b'][0][0], d['b'][0][1]
                if 'a' in d and len(d['a']) > 0: ap, aq = d['a'][0][0], d['a'][0][1]
            elif self.name == "Gate" and 'result' in data:
                d = data['result']
                if isinstance(d, dict) and 's' in d:
                    s, bp, bq, ap, aq = d['s'], d['b'], d['B'], d['a'], d['A']
            elif self.name == "Bitget" and 'data' in data:
                d = data['data'][0]
                s, bp, bq, ap, aq = d['instId'], d['bids'][0][0], d['bids'][0][1], d['asks'][0][0], d['asks'][0][1]
            elif self.name == "Bitmart" and 'data' in data:
                items = data['data'] if isinstance(data['data'], list) else [data['data']]
                for item in items:
                    if 'bid_px' in item and 'ask_px' in item:
                        self._update_pool(item['symbol'], item['bid_px'], item['bid_sz'], item['ask_px'], item['ask_sz'])
                return

            if s: 
                self._update_pool(s, bp, bq, ap, aq)
        except Exception: 
            pass

    def _update_pool(self, ex_s, bp, bq, ap, aq):
        norm = self.rev_map.get(ex_s)
        if norm:
            existing = prices_pool[norm].get(self.name)
            
            # 处理增量推送（某一边为 None 时，沿用旧数据）
            if existing:
                n_bp = float(bp) if bp is not None else existing.bid_p
                n_bq = float(bq) if bq is not None else existing.bid_sz
                n_ap = float(ap) if ap is not None else existing.ask_p
                n_aq = float(aq) if aq is not None else existing.ask_sz
            else:
                # 第一次必须有完整的买卖盘
                if bp is None or ap is None: return 
                n_bp, n_bq, n_ap, n_aq = float(bp), float(bq), float(ap), float(aq)
                
            prices_pool[norm][self.name] = BBO(bid_p=n_bp, bid_sz=n_bq, ask_p=n_ap, ask_sz=n_aq, ts=time.time())

# ================= 5. 工作协程 =================
async def ws_worker(ex_name, sym_map, p_idx):
    wid = f"{ex_name[:3]}-{p_idx:02d}"
    handler = ExchangeHandler(ex_name, sym_map)
    
    while True:
        proxy_url = PROXIES[p_idx % len(PROXIES)]
        conn_tracker.update(wid, ex=ex_name, status="CONN", latency=0)
        
        try:
            async with await create_proxy_ws(handler.uris[ex_name], proxy_url) as ws:
                stats.active_conns += 1
                conn_tracker.update(wid, status="ONLINE", last_msg=time.time())
                
                try:
                    await ws.send(ujson.dumps(handler.get_sub_payload()))
                    last_hb = time.time()
                    
                    while True:
                        try:
                            start_t = time.perf_counter()
                            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            handler.parse(msg)
                            latency = (time.perf_counter() - start_t) * 1000
                            conn_tracker.update(wid, last_msg=time.time(), latency=latency)
                        except asyncio.TimeoutError:
                            pass

                        now = time.time()
                        if now - last_hb > 20:
                            if ex_name in ["Bitget", "OKX"]: await ws.send("ping")
                            elif ex_name == "Bybit": await ws.send(ujson.dumps({"op": "ping"}))
                            elif ex_name == "Gate": await ws.send(ujson.dumps({"time": int(now), "channel": "spot.ping"}))
                            else: await ws.ping()
                            
                            conn_tracker.update(wid, last_msg=now)
                            last_hb = now

                        if now - conn_tracker.conns[wid]['last_msg'] > STALE_THRESHOLD:
                            raise ConnectionError("Stream Stalled")
                
                finally:
                    stats.active_conns = max(0, stats.active_conns - 1)

        except Exception as e:
            err_msg = type(e).__name__ if not str(e) else str(e)[:25]
            conn_tracker.update(wid, status="LOST", errors=conn_tracker.conns.get(wid, {}).get('errors', 0) + 1, ex=err_msg)
            await asyncio.sleep(3 + (p_idx % 5))

# ================= 6. 核心扫描器 =================
async def banzhuan_scanner():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now = time.time()
        for sym, ex_dict in list(prices_pool.items()):
            valid_exs = {ex: bbo for ex, bbo in ex_dict.items() if now - bbo.ts < 3.0}
            if len(valid_exs) < 2: continue
            
            try:
                best_buy_ex = min(valid_exs.keys(), key=lambda x: valid_exs[x].ask_p)
                best_sell_ex = max(valid_exs.keys(), key=lambda x: valid_exs[x].bid_p)
                
                low_ask = valid_exs[best_buy_ex].ask_p
                low_ask_qty = valid_exs[best_buy_ex].ask_sz  # 吃对方的卖单量
                
                high_bid = valid_exs[best_sell_ex].bid_p
                high_bid_qty = valid_exs[best_sell_ex].bid_sz # 砸对方的买单量
                
                if low_ask <= 0: continue 
                    
                diff = (high_bid - low_ask) / low_ask
                if diff >= THRESHOLD and best_buy_ex != best_sell_ex:
                    
                    arbitrage_opportunities.appendleft({
                        "time": time.strftime("%H:%M:%S"),
                        "symbol": sym,
                        "diff": round(diff * 100, 3),
                        "buy_ex": best_buy_ex,
                        "buy_price": low_ask,
                        "buy_qty": low_ask_qty,
                        "sell_ex": best_sell_ex,
                        "sell_price": high_bid,
                        "sell_qty": high_bid_qty
                    })
            except Exception:
                continue

# ================= 7. 控制台报告 =================
async def status_report():
    while True:
        await asyncio.sleep(STATUS_INTERVAL)
        os.system('cls' if os.name == 'nt' else 'clear')
        now = time.time()
        elapsed = now - stats.start_time
        tps = stats.msg_count / elapsed if elapsed > 0 else 0
        
        print(f"═══ 📊 现货 BBO 监控 (代理: {len(PROXIES)}端口) Web 面板: http://127.0.0.1:{WEB_PORT} ═══ TPS: {tps:.1f}")
        print(f"{'WorkerID':<12} | {'Stat':<6} | {'Lat':<7} | {'LastIn':<7} | {'Err'}")
        print("-" * 55)
        
        display_list = []
        for wid, info in conn_tracker.conns.items():
            last_in = now - info['last_msg']
            is_healthy = (info['status'] == "ONLINE" and last_in < 4.0)
            display_list.append((0 if not is_healthy else 1, wid, info, last_in, is_healthy))
            
        display_list.sort(key=lambda x: (x[0], x[1]))
        for _, wid, info, last_in, is_healthy in display_list[:25]:
            mark = "✅" if is_healthy else "❌"
            err_show = f"{info['errors']} ({info.get('ex','')})" if info['errors'] > 0 else "0"
            print(f"{wid:<12} | {mark} {info['status']:<4} | {info['latency']:>4.0f}ms | {last_in:>5.1f}s | {err_show}")
        print("-" * 55)

# ================= 8. Web 面板模块 =================
async def web_api_data(request):
    now = time.time()
    elapsed = now - stats.start_time
    tps = stats.msg_count / elapsed if elapsed > 0 else 0
    
    workers = []
    for wid, info in conn_tracker.conns.items():
        last_in = now - info['last_msg']
        is_healthy = (info['status'] == "ONLINE" and last_in < 4.0)
        workers.append({
            "wid": wid, "status": info['status'], "latency": f"{info['latency']:.0f}",
            "last_in": f"{last_in:.1f}", "errors": info['errors'], "is_healthy": is_healthy
        })
    workers.sort(key=lambda x: (not x["is_healthy"], x["wid"]))

    return web.json_response({
        "tps": round(tps, 1),
        "uptime": round(elapsed, 0),
        "total_workers": len(workers),
        "online_workers": stats.active_conns,
        "workers": workers,
        "opportunities": list(arbitrage_opportunities)
    }, dumps=ujson.dumps)

async def web_index(request):
    html_content = """
    <!DOCTYPE html>
    <html lang="zh" data-bs-theme="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Crypto Arbitrage Radar ⚡</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #121212; }
            .card { background-color: #1e1e1e; border: none; border-radius: 12px; }
            .text-green { color: #00e676; } .text-red { color: #ff1744; } .text-yellow { color: #ffea00; }
            .table-dark { --bs-table-bg: #1e1e1e; }
            .badge-ex { font-size: 0.85em; padding: 5px 8px; border-radius: 6px; }
            .scrollable-table { max-height: 75vh; overflow-y: auto; }
            .pulse { animation: pulse 2s infinite; }
            @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
            .qty-text { font-size: 0.85em; color: #a0a0a0; }
        </style>
    </head>
    <body>
        <div class="container-fluid py-4">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h3 class="mb-0 text-white">⚡ BBO 盘口高频套利雷达</h3>
                <div class="d-flex gap-4">
                    <div class="text-center"><div class="text-muted small">系统运行</div><strong class="fs-5" id="uptime">0s</strong></div>
                    <div class="text-center"><div class="text-muted small">实时 TPS</div><strong class="fs-5 text-info" id="tps">0</strong></div>
                    <div class="text-center"><div class="text-muted small">连接状态</div><strong class="fs-5 text-green" id="conns">0/0</strong></div>
                </div>
            </div>

            <div class="row g-4">
                <div class="col-lg-8">
                    <div class="card shadow-sm h-100">
                        <div class="card-header border-0 bg-transparent pt-3 pb-0">
                            <h5 class="text-yellow mb-0">🔥 实时盘口机会 (高于阈值)</h5>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive scrollable-table">
                                <table class="table table-dark table-hover align-middle">
                                    <thead class="sticky-top bg-dark">
                                        <tr>
                                            <th>发现时间</th><th>交易对</th><th>瞬时利差</th>
                                            <th>买入平台 (吃卖单)</th><th>卖出平台 (砸买单)</th>
                                        </tr>
                                    </thead>
                                    <tbody id="opp-tbody">
                                        <tr><td colspan="5" class="text-center text-muted py-4">监控中，等待机会...</td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="col-lg-4">
                    <div class="card shadow-sm h-100">
                        <div class="card-header border-0 bg-transparent pt-3 pb-0 d-flex justify-content-between">
                            <h5 class="text-white mb-0">🔌 Worker 探针状态</h5>
                            <span class="badge bg-success pulse">Live</span>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive scrollable-table">
                                <table class="table table-dark table-sm align-middle">
                                    <thead class="sticky-top bg-dark">
                                        <tr><th>探针 ID</th><th>状态</th><th>延迟</th><th>上次数据</th><th>错误</th></tr>
                                    </thead>
                                    <tbody id="worker-tbody"></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            function getExColor(ex) {
                const colors = { 'Binance': 'bg-warning text-dark', 'OKX': 'bg-light text-dark', 'Bybit': 'bg-dark border', 'Gate': 'bg-danger text-white', 'Bitget': 'bg-info text-dark', 'Bitmart': 'bg-secondary text-white' };
                return colors[ex] || 'bg-secondary text-white';
            }

            async function fetchData() {
                try {
                    const res = await fetch('/api/data');
                    const data = await res.json();
                    
                    document.getElementById('tps').innerText = data.tps.toLocaleString();
                    document.getElementById('uptime').innerText = Math.floor(data.uptime / 60) + 'm ' + (data.uptime % 60) + 's';
                    document.getElementById('conns').innerText = data.online_workers + ' / ' + data.total_workers;

                    let wHtml = '';
                    data.workers.forEach(w => {
                        const statusDot = w.is_healthy ? '<span class="text-green">●</span>' : '<span class="text-red">●</span>';
                        wHtml += `<tr>
                            <td class="font-monospace">${w.wid}</td>
                            <td>${statusDot} ${w.status}</td>
                            <td class="${w.latency > 1000 ? 'text-warning' : ''}">${w.latency}ms</td>
                            <td class="${w.last_in > 4 ? 'text-red' : ''}">${w.last_in}s</td>
                            <td class="${w.errors > 0 ? 'text-red' : ''}">${w.errors}</td>
                        </tr>`;
                    });
                    document.getElementById('worker-tbody').innerHTML = wHtml;

                    let oppHtml = '';
                    if (data.opportunities.length === 0) {
                        oppHtml = '<tr><td colspan="5" class="text-center text-muted py-4">雷达扫描中，暂未发现高于阈值的利差...</td></tr>';
                    } else {
                        data.opportunities.forEach(opp => {
                            // 增加数量显示字段
                            oppHtml += `<tr>
                                <td class="text-muted">${opp.time}</td>
                                <td class="fw-bold fs-5">${opp.symbol}</td>
                                <td class="text-yellow fw-bold">+${opp.diff}%</td>
                                <td>
                                    <span class="badge badge-ex ${getExColor(opp.buy_ex)}">${opp.buy_ex}</span> 
                                    <span class="text-green">${opp.buy_price}</span> 
                                    <span class="qty-text">/ 数量: ${opp.buy_qty}</span>
                                </td>
                                <td>
                                    <span class="badge badge-ex ${getExColor(opp.sell_ex)}">${opp.sell_ex}</span> 
                                    <span class="text-red">${opp.sell_price}</span> 
                                    <span class="qty-text">/ 数量: ${opp.sell_qty}</span>
                                </td>
                            </tr>`;
                        });
                    }
                    document.getElementById('opp-tbody').innerHTML = oppHtml;

                } catch (e) { console.error("Refresh failed", e); }
            }

            setInterval(fetchData, 1000);
            fetchData();
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type='text/html')

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', web_index)
    app.router.add_get('/api/data', web_api_data)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    print(f"🚀 Web 看板已启动: http://127.0.0.1:{WEB_PORT}")

# ================= 9. 入口逻辑 =================
async def fetch_symbols(client, name, url):
    try:
        resp = await client.get(url, timeout=15)
        data = resp.json()
        pairs = {}
        if name == "Binance":
            for s in data['symbols']:
                if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT': pairs[s['symbol'].upper()] = s['symbol']
        elif name == "OKX":
            for s in data['data']:
                if s['state'] == 'live' and s['quoteCcy'] == 'USDT': pairs[s['instId'].replace("-","").upper()] = s['instId']
        elif name == "Bybit":
            for s in data['result']['list']:
                if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT': pairs[s['symbol'].upper()] = s['symbol']
        elif name == "Gate":
            for s in data:
                if s.get('quote') == 'USDT': pairs[s['id'].replace("_","").upper()] = s['id']
        elif name == "Bitget":
            for s in data['data']:
                if s['status'] == 'online' and s['quoteCoin'] == 'USDT': pairs[s['symbol'].upper()] = s['symbol']
        elif name == "Bitmart":
            for s in data['data']['symbols']:
                if '_USDT' in s: pairs[s.replace("_","").upper()] = s
        return pairs
    except Exception as e: 
        print(f"Failed fetching {name} symbols: {e}")
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

    print("🔍 正在通过 HTTP 同步全交易所交易对...")
    async with httpx.AsyncClient(proxy=PROXIES[0], verify=False) as client:
        results = await asyncio.gather(*[fetch_symbols(client, n, u) for n, u in endpoints.items()])
        ex_data = dict(zip(endpoints.keys(), results))

    all_symbols = set()
    for d in ex_data.values(): all_symbols.update(d.keys())
    watchlist = [s for s in all_symbols if sum(1 for ex in ex_data if s in ex_data[ex]) >= 2]
    
    tasks = [start_web_server(), banzhuan_scanner(), status_report()]
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1
    
    print(f"✅ 初始化完成！监控共同币种: {len(watchlist)} 个，启动 WebSocket 探针: {p_global_idx} 个")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    if os.name == 'nt': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已手动停止。")