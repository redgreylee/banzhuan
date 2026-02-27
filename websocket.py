import asyncio
import json
import zlib
import time
import ssl
import httpx
from collections import defaultdict
import websockets
from python_socks.async_.asyncio import Proxy

# ================= 1. 核心配置 =================
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7891, 7896)]
THRESHOLD = 0.003        # 0.3% 告警（利差阈值）
SCAN_INTERVAL = 0.5      # 扫描频率
STATUS_INTERVAL = 10     # 状态汇报频率

# 每条连接的订阅上限（遵守交易所 API 限制）
SUB_LIMITS = {
    "Binance": 50, "OKX": 50, "Bybit": 10, 
    "Gate": 50, "Bitget": 50, "Bitmart": 50
}

# ================= 2. 全局状态监控 =================
prices_pool = defaultdict(dict)

class GlobalStats:
    def __init__(self):
        self.msg_count = 0
        self.active_conns = 0
        self.start_time = time.time()
        self.ex_stats = defaultdict(int)

stats = GlobalStats()

# ================= 3. 底层代理连接器 (修复 SSL & 404) =================
async def create_proxy_ws(uri, proxy_url):
    """
    手动建立代理隧道并注入忽略 SSL 验证的上下文
    """
    try:
        proxy = Proxy.from_url(proxy_url)
        # 解析主机和端口
        host_port = uri.split("://")[1].split("/")[0]
        if ":" in host_port:
            dest_host, dest_port = host_port.split(":")
            dest_port = int(dest_port)
        else:
            dest_host = host_port
            dest_port = 443 if uri.startswith("wss") else 80
        
        # 1. 建立代理 Socket
        sock = await asyncio.wait_for(proxy.connect(dest_host, dest_port), timeout=12)
        
        # 2. 创建忽略证书校验的 SSL 上下文 (解决 Bitmart 报错)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # 3. 握手 WebSocket
        ws = await websockets.connect(uri, sock=sock, ssl=ssl_context, ping_interval=20)
        return ws
    except Exception as e:
        raise ConnectionError(f"握手失败: {e}")

# ================= 4. 交易所协议处理器 =================
class ExchangeHandler:
    def __init__(self, name, symbol_map):
        self.name = name
        self.symbol_map = symbol_map
        # 更新了 Bitget 和 Bitmart 的接入点
        self.uris = {
            "Binance": "wss://stream.binance.com:9443/ws",
            "OKX": "wss://ws.okx.com:8443/ws/v5/public",
            "Bybit": "wss://stream.bybit.com/v5/public/spot",
            "Gate": "wss://api.gateio.ws/ws/v4/",
            "Bitget": "wss://ws.bitget.com/v5/public/realTime", 
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
        try:
            # Bitmart 特有的 zlib 解压
            if self.name == "Bitmart" and isinstance(raw_msg, bytes):
                raw_msg = zlib.decompress(raw_msg, 16 + zlib.MAX_WBITS).decode('utf-8')
            
            data = json.loads(raw_msg)
            s, p = None, None
            
            if self.name == "Binance" and 's' in data: s, p = data['s'], data['c']
            elif self.name == "OKX" and 'data' in data: s, p = data['data'][0]['instId'], data['data'][0]['last']
            elif self.name == "Bybit" and 'data' in data: s, p = data['data']['symbol'], data['data']['lastPrice']
            elif self.name == "Gate" and 'result' in data: s, p = data['result']['currency_pair'], data['result']['last']
            elif self.name == "Bitget" and 'data' in data: s, p = data['data'][0]['instId'], data['data'][0]['lastPr']
            elif self.name == "Bitmart" and 'data' in data:
                # Bitmart 可能一次推多个 ticker
                items = data['data'] if isinstance(data['data'], list) else [data['data']]
                for item in items:
                    s, p = item['symbol'], item['last_price']
                    self._update_pool(s, p)
                return

            if s and p: self._update_pool(s, p)
        except: pass

    def _update_pool(self, s, p):
        norm = s.replace("-", "").replace("_", "").replace("/", "").upper()
        prices_pool[norm][self.name] = float(p)
        stats.ex_stats[self.name] += 1

# ================= 5. 核心任务协程 =================
async def ws_worker(ex_name, sym_map, p_idx):
    proxy_url = PROXIES[p_idx % len(PROXIES)]
    handler = ExchangeHandler(ex_name, sym_map)
    
    while True:
        try:
            async with await create_proxy_ws(handler.uris[ex_name], proxy_url) as ws:
                stats.active_conns += 1
                print(f"✅ {ex_name} 连接成功 | 代理: {proxy_url} | 订阅数: {len(sym_map)}")
                
                await ws.send(json.dumps(handler.get_sub_payload()))
                
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=40)
                    handler.parse(msg)
                    # 心跳保持
                    if "ping" in str(msg).lower():
                        await ws.send(json.dumps({"op": "pong"}))
        except Exception as e:
            if 'ws' in locals(): stats.active_conns -= 1
            print(f"⚠️ {ex_name} 连接异常: {e} | 5s后重试")
            await asyncio.sleep(5)

async def banzhuan_scanner():
    print("🚀 利差扫描器启动...")
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        for sym, ex_dict in list(prices_pool.items()):
            if len(ex_dict) < 2: continue
            
            # 排序获取最低/最高价
            sorted_items = sorted(ex_dict.items(), key=lambda x: x[1])
            low_ex, lp = sorted_items[0]
            high_ex, hp = sorted_items[-1]
            diff = (hp - lp) / lp
            
            if diff >= THRESHOLD:
                print(f"🔥 [利差] {sym: <10} | {diff:.2%} | 买:{low_ex}({lp}) -> 卖:{high_ex}({hp})")

async def status_report():
    while True:
        await asyncio.sleep(STATUS_INTERVAL)
        uptime = int(time.time() - stats.start_time)
        tps = stats.msg_count / uptime if uptime > 0 else 0
        print(f"\n{'='*60}")
        print(f"📊 运行报告 | Uptime: {uptime}s | 总TPS: {tps:.1f}")
        print(f"🔗 活跃连接: {stats.active_conns} | 内存价格数: {len(prices_pool)}")
        print(f"🏛️ 命中分布: {dict(stats.ex_stats)}")
        print(f"{'='*60}\n")

# ================= 6. 主程序入口 =================
async def fetch_symbols(client, name, url):
    """通用交易对抓取函数"""
    try:
        resp = await client.get(url, timeout=12)
        data = resp.json()
        pairs = {}
        if name == "Binance":
            for s in data['symbols']:
                if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT': pairs[s['symbol'].upper()] = s['symbol']
        elif name == "OKX":
            for s in data['data']:
                if s['state'] == 'live' and s['quoteAsset'] == 'USDT': pairs[s['instId'].replace("-","").upper()] = s['instId']
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
        print(f"❌ {name} API 拉取失败: {e}")
        return {}
# ================= 6. 修正后的主程序入口 =================
async def main():
    # 交易所 API 列表
    endpoints = {
        "Binance": "https://api.binance.com/api/v3/exchangeInfo",
        "OKX": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "Bybit": "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "Gate": "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "Bitget": "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitmart": "https://api-cloud.bitmart.com/spot/v1/symbols"
    }

    # 1. 尝试使用代理初始化 httpx 客户端
    # 我们从 PROXIES 列表中取第一个作为主请求代理
    main_proxy = PROXIES[0]
    
    # 注意：httpx 需要安装 [socks] 扩展才能支持 socks5 代理
    # 如果你的代理是 http，则无需额外安装
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    
    async with httpx.AsyncClient(
        proxy=main_proxy, 
        verify=False, 
        limits=limits,
        timeout=15.0
    ) as client:
        print(f"🔍 [1/3] 正在通过代理 {main_proxy} 拉取全市场实时交易对...")
        
        # 并发执行所有 API 请求
        tasks = [fetch_symbols(client, name, url) for name, url in endpoints.items()]
        results = await asyncio.gather(*tasks)
        
        # 将结果映射回交易所名称
        ex_data = dict(zip(endpoints.keys(), results))

    # 2. 计算交集：找出至少在两个交易所都存在的币种
    all_symbols = set()
    for d in ex_data.values():
        all_symbols.update(d.keys())
    
    # 统计每个币种出现的次数
    watchlist = []
    for s in all_symbols:
        count = sum(1 for ex in ex_data if s in ex_data[ex])
        if count >= 2:
            watchlist.append(s)
            
    if not watchlist:
        print("❌ 错误：未找到任何共同交易对。请检查网络代理是否工作，或 API 地址是否失效。")
        return

    print(f"✅ [2/3] 监控币种确认：共 {len(watchlist)} 个交叉交易对。")

    # 3. 启动分布式 WebSocket 连接
    print(f"🚀 [3/3] 启动分布式 WebSocket 监听任务...")
    worker_tasks = []
    p_idx = 0
    
    for ex_name, limit in SUB_LIMITS.items():
        # 过滤出当前交易所支持的、且在监控列表中的币种
        supported = {
            s: ex_data[ex_name][s] 
            for s in watchlist 
            if s in ex_data[ex_name]
        }
        
        items = list(supported.items())
        if not items:
            continue
            
        # 根据 SUB_LIMITS 进行分片订阅（防止单条连接订阅过多导致封禁）
        for i in range(0, len(items), limit):
            chunk = dict(items[i : i + limit])
            # 每个 worker 分配一个代理索引，实现代理轮询
            worker_tasks.append(ws_worker(ex_name, chunk, p_idx))
            p_idx += 1
            # 启动缓冲，避免瞬间并发连接过载
            await asyncio.sleep(0.2)

    # 4. 聚合所有长连接协程和扫描协程
    print(f"📊 系统已就绪，正在进入实时监控模式...")
    try:
        await asyncio.gather(
            banzhuan_scanner(),
            status_report(),
            *worker_tasks
        )
    except Exception as e:
        print(f"🚨 系统运行异常: {e}")

if __name__ == "__main__":
    try:
        # 设置事件循环（部分 Windows 环境下可能需要）
        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 用户手动停止，程序正在退出...")