import asyncio
import ujson
import zlib
import time
import ssl
import httpx
import os
from collections import defaultdict
import websockets
from python_socks.async_.asyncio import Proxy

# ================= 1. 核心配置 =================
# 扩展至 20 个端口: 7891 - 7910
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7891, 7911)]
THRESHOLD = 0.01        # 1% 告警
SCAN_INTERVAL = 0.5     # 扫描频率
STATUS_INTERVAL = 3     # 仪表盘刷新频率
STALE_THRESHOLD = 4.0   # 超过4秒无数据强制重连

# 调整订阅上限以分摊单链接压力
SUB_LIMITS = {
    "Binance": 50, 
    "OKX": 50, 
    "Bybit": 10,   # 降低 Bybit 单链接负担
    "Gate": 50, 
    "Bitget": 40,  # 稍微降低 Bitget 单链接负担
    "Bitmart": 50
}

# ================= 2. 状态监控 =================
prices_pool = defaultdict(dict)

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
        if self.name == "Binance": return {"method": "SUBSCRIBE", "params": [f"{s.lower()}@ticker" for s in syms], "id": 1}
        if self.name == "OKX": return {"op": "subscribe", "args": [{"channel": "tickers", "instId": s} for s in syms]}
        if self.name == "Bybit": return {"op": "subscribe", "args": [f"tickers.{s}" for s in syms]}
        if self.name == "Gate": return {"time": int(time.time()), "channel": "spot.tickers", "event": "subscribe", "payload": syms}
        if self.name == "Bitget": return {"op": "subscribe", "args": [{"instType": "SPOT", "channel": "ticker", "instId": s} for s in syms]}
        if self.name == "Bitmart": return {"op": "subscribe", "args": [f"spot/ticker:{s}" for s in syms]}
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
                if 'symbol' in d: s, p = d['symbol'], d['lastPrice']
            elif self.name == "Gate" and 'result' in data: s, p = data['result']['currency_pair'], data['result']['last']
            elif self.name == "Bitget" and 'data' in data: s, p = data['data'][0]['instId'], data['data'][0]['lastPr']
            elif self.name == "Bitmart" and 'data' in data:
                items = data['data'] if isinstance(data['data'], list) else [data['data']]
                for item in items: self._update_pool(item['symbol'], item['last_price'])
                return
            if s and p: self._update_pool(s, p)
        except: pass

    def _update_pool(self, ex_s, p):
        norm = self.rev_map.get(ex_s)
        if norm: prices_pool[norm][self.name] = (float(p), time.time())

# ================= 5. 工作协程 =================
async def ws_worker(ex_name, sym_map, p_idx):
    wid = f"{ex_name[:3]}-{p_idx:02d}"
    handler = ExchangeHandler(ex_name, sym_map)
    
    while True:
        # 负载均衡：将 p_idx 取模 PROXIES 长度，确保均匀分布
        proxy_url = PROXIES[p_idx % len(PROXIES)]
        conn_tracker.update(wid, ex=ex_name, status="CONN", latency=0)
        
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
                    # 主动心跳 20s
                    if now - last_hb > 20:
                        if ex_name in ["Bitget", "OKX"]: await ws.send("ping")
                        elif ex_name == "Bybit": await ws.send(ujson.dumps({"op": "ping"}))
                        last_hb = now

                    # 僵尸链接检测：如果数据停滞超过阈值，强制报错重连
                    if now - conn_tracker.conns[wid]['last_msg'] > STALE_THRESHOLD:
                        raise ConnectionError("Stream Stalled")

        except Exception:
            conn_tracker.update(wid, status="LOST", errors=conn_tracker.conns.get(wid, {}).get('errors', 0) + 1)
            if 'ws' in locals(): stats.active_conns = max(0, stats.active_conns - 1)
            # 增加失败后的等待抖动，防止瞬时并发请求
            await asyncio.sleep(3 + (p_idx % 5))

# ================= 6. 扫描与报告 =================
async def banzhuan_scanner():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now = time.time()
        for sym, ex_dict in list(prices_pool.items()):
            # 扫描时自动忽略过时（超过3秒没更新）的价格
            valid = {ex: p for ex, (p, ts) in ex_dict.items() if now - ts < 3.0}
            if len(valid) < 2: continue
            
            sorted_v = sorted(valid.items(), key=lambda x: x[1])
            (l_ex, lp), (h_ex, hp) = sorted_v[0], sorted_v[-1]
            diff = (hp - lp) / lp
            if diff >= THRESHOLD:
                print(f"🔥 [利差] {sym: <10} | {diff:.2%} | 买:{l_ex}({lp}) -> 卖:{h_ex}({hp})")

async def status_report():
    while True:
        await asyncio.sleep(STATUS_INTERVAL)
        os.system('cls' if os.name == 'nt' else 'clear')
        now = time.time()
        print(f"═══ 📊 搬砖监控 (代理: {len(PROXIES)}端口) ═══ TPS: {stats.msg_count/(now-stats.start_time):.1f}")
        print(f"{'WorkerID':<12} | {'Stat':<6} | {'Lat':<7} | {'LastIn':<7} | {'Err'}")
        print("-" * 55)
        # 仪表盘仅显示前 30 个 Worker 或异常 Worker，防止屏幕刷不动
        for wid, info in sorted(conn_tracker.conns.items()):
            last_in = now - info['last_msg']
            mark = "✅" if (info['status'] == "ONLINE" and last_in < 2.0) else "❌"
            print(f"{wid:<12} | {mark} {info['status']:<4} | {info['latency']:>4.0f}ms | {last_in:>5.1f}s | {info['errors']}")
        print("-" * 55)

# ================= 7. 入口 =================
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
    except Exception: return {}

async def main():
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
    
    tasks = [banzhuan_scanner(), status_report()]
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            # 使用 p_global_idx 确保 Worker 均匀分配到 20 个代理端口
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass