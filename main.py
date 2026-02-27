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
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ================= 1. 核心配置 =================
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7891, 7911)]
THRESHOLD = 0.01        # 1% 告警
SCAN_INTERVAL = 0.5     # 扫描频率
STALE_THRESHOLD = 25.0  # 僵尸链接检测阈值
API_PORT = 8080         # FastAPI 端口

SUB_LIMITS = {
    "Binance": 50, "OKX": 50, "Bybit": 10,   
    "Gate": 50, "Bitget": 40, "Bitmart": 50
}

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
    except Exception:
        raise ConnectionError("Proxy Connect Error")

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
        if self.name == "Binance": return {"method": "SUBSCRIBE", "params":[f"{s.lower()}@ticker" for s in syms], "id": 1}
        if self.name == "OKX": return {"op": "subscribe", "args":[{"channel": "tickers", "instId": s} for s in syms]}
        if self.name == "Bybit": return {"op": "subscribe", "args":[f"tickers.{s}" for s in syms]}
        if self.name == "Gate": return {"time": int(time.time()), "channel": "spot.tickers", "event": "subscribe", "payload": syms}
        if self.name == "Bitget": return {"op": "subscribe", "args":[{"instType": "SPOT", "channel": "ticker", "instId": s} for s in syms]}
        if self.name == "Bitmart": return {"op": "subscribe", "args":[f"spot/ticker:{s}" for s in syms]}
        return {}

    def parse(self, raw_msg):
        stats.msg_count += 1
        if raw_msg == "pong": return
        try:
            if self.name == "Bitmart" and isinstance(raw_msg, bytes):
                raw_msg = zlib.decompress(raw_msg, 16 + zlib.MAX_WBITS).decode('utf-8')
            data = ujson.loads(raw_msg)
            s, p = None, None
            
            if self.name == "Binance" and 's' in data: s, p = data['s'], data['c']
            elif self.name == "OKX" and 'data' in data: s, p = data['data'][0]['instId'], data['data'][0]['last']
            elif self.name == "Bybit" and 'data' in data: 
                d = data['data']
                if 'symbol' in d and 'lastPrice' in d: s, p = d['symbol'], d['lastPrice']
            elif self.name == "Gate" and 'result' in data: 
                if isinstance(data['result'], dict) and 'currency_pair' in data['result']: s, p = data['result']['currency_pair'], data['result']['last']
            elif self.name == "Bitget" and 'data' in data: s, p = data['data'][0]['instId'], data['data'][0]['lastPr']
            elif self.name == "Bitmart" and 'data' in data:
                items = data['data'] if isinstance(data['data'], list) else [data['data']]
                for item in items: self._update_pool(item['symbol'], item['last_price'])
                return
            if s and p: self._update_pool(s, p)
        except Exception: pass

    def _update_pool(self, ex_s, p):
        norm = self.rev_map.get(ex_s)
        if norm: prices_pool[norm][self.name] = (float(p), time.time())

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
                        last_hb = now

                    if now - conn_tracker.conns[wid]['last_msg'] > STALE_THRESHOLD:
                        raise ConnectionError("Stream Stalled")
        except Exception as e:
            err_msg = type(e).__name__
            conn_tracker.update(wid, status="LOST", errors=conn_tracker.conns.get(wid, {}).get('errors', 0) + 1, ex=err_msg)
            if "stats" in globals(): stats.active_conns = max(0, stats.active_conns - 1)
            await asyncio.sleep(5)

# ================= 6. 套利扫描器 =================
async def banzhuan_scanner():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now = time.time()
        for sym, ex_dict in list(prices_pool.items()):
            valid = {ex: p for ex, (p, ts) in ex_dict.items() if now - ts < 3.0}
            if len(valid) < 2: continue
            
            sorted_v = sorted(valid.items(), key=lambda x: x[1])
            (l_ex, lp), (h_ex, hp) = sorted_v[0], sorted_v[-1]
            if lp <= 0: continue 
                
            diff = (hp - lp) / lp
            if diff >= THRESHOLD:
                arbitrage_opportunities.appendleft({
                    "time": time.strftime("%H:%M:%S"),
                    "symbol": sym,
                    "diff": round(diff * 100, 2),
                    "buy_ex": l_ex,
                    "buy_price": lp,
                    "sell_ex": h_ex,
                    "sell_price": hp
                })

# ================= 7. FastAPI 接口 =================
app = FastAPI(title="Crypto Arbitrage API")

# 启用 CORS 跨域，允许 Vue 前端访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/stats")
async def get_dashboard_data():
    now = time.time()
    elapsed = now - stats.start_time
    tps = stats.msg_count / elapsed if elapsed > 0 else 0
    
    workers = []
    for wid, info in conn_tracker.conns.items():
        last_in = now - info['last_msg']
        is_healthy = (info['status'] == "ONLINE" and last_in < 5.0)
        workers.append({
            "wid": wid, 
            "status": info['status'], 
            "latency": round(info.get('latency', 0)),
            "last_in": round(last_in, 1), 
            "errors": info['errors'], 
            "is_healthy": is_healthy
        })
    
    # 按照健康状态和ID排序
    workers.sort(key=lambda x: (not x["is_healthy"], x["wid"]))

    return {
        "tps": round(tps, 1),
        "uptime": round(elapsed),
        "total_workers": len(workers),
        "online_workers": stats.active_conns,
        "workers": workers,
        "opportunities": list(arbitrage_opportunities)
    }

# ================= 8. 主入口逻辑 =================
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

async def run_api():
    config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    endpoints = {
        "Binance": "https://api.binance.com/api/v3/exchangeInfo",
        "OKX": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "Bybit": "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "Gate": "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "Bitget": "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitmart": "https://api-cloud.bitmart.com/spot/v1/symbols"
    }

    # 1. 启动初始化
    print("正在获取各大交易所交易对信息...")
    async with httpx.AsyncClient(proxy=PROXIES[0] if PROXIES else None, verify=False) as client:
        results = await asyncio.gather(*[fetch_symbols(client, n, u) for n, u in endpoints.items()])
        ex_data = dict(zip(endpoints.keys(), results))

    all_symbols = set()
    for d in ex_data.values(): all_symbols.update(d.keys())
    watchlist = [s for s in all_symbols if sum(1 for ex in ex_data if s in ex_data[ex]) >= 2]
    
    # 2. 准备并行任务
    tasks = [run_api(), banzhuan_scanner()]
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1
    
    print(f"初始化完成！共分配 {p_global_idx} 个 Worker。API 端口: {API_PORT}")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass