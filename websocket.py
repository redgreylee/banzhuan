import asyncio
import os
import ssl
import time
import zlib
from collections import defaultdict, deque

import httpx
import ujson
import websockets
from aiohttp import web
from python_socks.async_.asyncio import Proxy

# ================= 1. 核心配置 =================
PROXIES = [f"http://127.0.0.1:{port}" for port in range(7894, 7909)]
WEB_PORT = 8080

# --- 交易策略限制 ---
MIN_NET_PROFIT = 0.008  # 最小净利润要求 (1%)
EXCHANGE_FEE = 0.001  # 预估单边手续费 (0.1%)
MIN_DEPTH_USD = 50.0  # 最小买卖盘深度限制 (单位: USDT)
MAX_ABNORMAL_DIFF = 0.05  # 异常值过滤 (利差超过 5% 视为异常数据，不记录)

# 每个 WS 连接订阅的交易对上限
SUB_LIMITS = {
    "Binance": 100,
    "OKX": 100,
    "Bybit": 10,
    "Gate": 100,
    "Bitget": 50,
    "Bitmart": 30,
}

# ================= 2. 全局状态存储 =================
# { "BTCUSDT": { "Binance": (bp, bq, ap, aq, ts), ... } }
prices_pool = defaultdict(dict)
arbitrage_opportunities = deque(maxlen=50)


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
            self.conns[wid] = {
                "status": "INIT",
                "latency": 0,
                "last_msg": time.time(),
                "errors": 0,
                "ex": "",
            }
        self.conns[wid].update(kwargs)


stats = GlobalStats()
conn_tracker = ConnectionTracker()


# ================= 3. 底层连接器 =================
async def create_proxy_ws(uri, proxy_url):
    try:
        proxy = Proxy.from_url(proxy_url)
        host_port = uri.split("://")[1].split("/")[0]
        dest_host, dest_port = (
            host_port.split(":") if ":" in host_port else (host_port, 443)
        )

        sock = await asyncio.wait_for(
            proxy.connect(dest_host, int(dest_port)), timeout=10
        )
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return await websockets.connect(
            uri, sock=sock, ssl=ssl_context, ping_interval=None
        )
    except Exception as e:
        raise ConnectionError(f"Proxy Connect Failed: {e}")


# ================= 4. 协议处理器 (事件驱动核心) =================
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
            "Bitmart": "wss://ws-manager-compress.bitmart.com/api?protocol=1.1",
        }

    def get_sub_payload(self):
        syms = list(self.symbol_map.values())
        if self.name == "Binance":
            return {
                "method": "SUBSCRIBE",
                "params": [f"{s.lower()}@bookTicker" for s in syms],
                "id": 1,
            }
        if self.name == "OKX":
            return {
                "op": "subscribe",
                "args": [{"channel": "bbo-tbt", "instId": s} for s in syms],
            }
        if self.name == "Bybit":
            return {"op": "subscribe", "args": [f"orderbook.1.{s}" for s in syms]}
        if self.name == "Gate":
            return {
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": syms,
            }
        if self.name == "Bitget":
            return {
                "op": "subscribe",
                "args": [
                    {"instType": "SPOT", "channel": "books1", "instId": s} for s in syms
                ],
            }
        if self.name == "Bitmart":
            return {"op": "subscribe", "args": [f"spot/ticker:{s}" for s in syms]}
        return {}

    def parse(self, raw_msg):
        stats.msg_count += 1
        try:
            if self.name == "Bitmart" and isinstance(raw_msg, bytes):
                raw_msg = zlib.decompress(raw_msg, 16 + zlib.MAX_WBITS).decode("utf-8")

            data = ujson.loads(raw_msg)
            s, bp, bq, ap, aq = None, None, None, None, None

            if self.name == "Binance" and "u" in data:
                s, bp, bq, ap, aq = (
                    data["s"],
                    data["b"],
                    data["B"],
                    data["a"],
                    data["A"],
                )
            elif self.name == "OKX" and "data" in data:
                d = data["data"][0]
                s, bp, bq, ap, aq = (
                    d["instId"],
                    d["bids"][0][0],
                    d["bids"][0][1],
                    d["asks"][0][0],
                    d["asks"][0][1],
                )
            elif self.name == "Bybit" and "data" in data:
                d = data["data"]
                s = d.get("s")
                if "b" in d and d["b"]:
                    bp, bq = d["b"][0][0], d["b"][0][1]
                if "a" in d and d["a"]:
                    ap, aq = d["a"][0][0], d["a"][0][1]
            elif self.name == "Gate" and "result" in data:
                d = data["result"]
                if isinstance(d, dict) and "s" in d:
                    s, bp, bq, ap, aq = d["s"], d["b"], d["B"], d["a"], d["A"]
            elif self.name == "Bitget" and "data" in data:
                d = data["data"][0]
                s, bp, bq, ap, aq = (
                    d["instId"],
                    d["bids"][0][0],
                    d["bids"][0][1],
                    d["asks"][0][0],
                    d["asks"][0][1],
                )
            elif self.name == "Bitmart" and "data" in data:
                items = (
                    data["data"] if isinstance(data["data"], list) else [data["data"]]
                )
                for i in items:
                    self._update_and_check(
                        i["symbol"], i["bid_px"], i["bid_sz"], i["ask_px"], i["ask_sz"]
                    )
                return

            if s:
                self._update_and_check(s, bp, bq, ap, aq)
        except:
            pass

    def _update_and_check(self, ex_s, bp, bq, ap, aq):
        norm = self.rev_map.get(ex_s)
        if norm and bp and ap:
            now = time.time()
            # 1. 更新价格池 (bp:买一价, bq:买一量, ap:卖一价, aq:卖一量)
            prices_pool[norm][self.name] = (
                float(bp),
                float(bq),
                float(ap),
                float(aq),
                now,
            )

            # 2. 事件驱动：收到消息立刻触发该币种的跨平台扫描
            self._scan_symbol_live(norm, now)

    def _scan_symbol_live(self, sym, now):
        ex_dict = prices_pool[sym]
        # 仅对比 3 秒内有更新的数据
        valid = {ex: data for ex, data in ex_dict.items() if now - data[4] < 3.0}
        if len(valid) < 2:
            return

        # 寻找最优买卖点
        # 目标：从卖价(Ask)最低的地方买入，到买价(Bid)最高的地方卖出
        best_buy_ex = min(valid.keys(), key=lambda x: valid[x][2])  # 谁卖得最便宜 (Ask)
        best_sell_ex = max(valid.keys(), key=lambda x: valid[x][0])  # 谁买得最贵 (Bid)

        if best_buy_ex == best_sell_ex:
            return

        # 核心数据提取
        buy_p, buy_q = valid[best_buy_ex][2], valid[best_buy_ex][3]  # 买入端价格与深度
        sell_p, sell_q = (
            valid[best_sell_ex][0],
            valid[best_sell_ex][1],
        )  # 卖出端价格与深度

        # --- 限制过滤条件 ---
        # 1. 深度限制：确保买卖两端的挂单额都足够大，防止小单虚假拉高利差
        if (buy_p * buy_q < MIN_DEPTH_USD) or (sell_p * sell_q < MIN_DEPTH_USD):
            return

        # 2. 净利润计算：(卖出价 * (1-费率)) / (买入价 * (1+费率)) - 1
        net_profit = (sell_p * (1 - EXCHANGE_FEE)) / (buy_p * (1 + EXCHANGE_FEE)) - 1

        # 3. 异常过滤：如果利润超过 MAX_ABNORMAL_DIFF (5%)，通常是停充提或交易所数据故障
        if net_profit > MAX_ABNORMAL_DIFF:
            return

        # 4. 最终展示逻辑
        if net_profit >= MIN_NET_PROFIT:
            arbitrage_opportunities.appendleft(
                {
                    "time": time.strftime("%H:%M:%S"),
                    "symbol": sym,
                    "net_profit": round(net_profit * 100, 3),  # 记录净利润
                    "buy_ex": best_buy_ex,
                    "buy_price": buy_p,
                    "buy_val": round(buy_p * buy_q, 1),
                    "sell_ex": best_sell_ex,
                    "sell_price": sell_p,
                    "sell_val": round(sell_p * sell_q, 1),
                }
            )


# ================= 5. 并行工作协程 =================
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
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        handler.parse(msg)  # 实时解析并触发比价
                        conn_tracker.update(
                            wid,
                            last_msg=time.time(),
                            latency=(time.perf_counter() - start_t) * 1000,
                        )
                    except asyncio.TimeoutError:
                        await ws.ping()

                    if time.time() - conn_tracker.conns[wid]["last_msg"] > 20.0:
                        raise ConnectionError("Connection Stale")
        except Exception:
            conn_tracker.update(
                wid, status="LOST", errors=conn_tracker.conns[wid]["errors"] + 1
            )
            await asyncio.sleep(8)
        finally:
            stats.active_conns = max(0, stats.active_conns - 1)


# ================= 6. Web 控制面板 =================
async def web_api_data(request):
    return web.json_response(
        {
            "tps": round(stats.msg_count / (time.time() - stats.start_time + 0.1), 1),
            "online_workers": stats.active_conns,
            "total_workers": len(conn_tracker.conns),
            "workers": [{**v, "wid": k} for k, v in conn_tracker.conns.items()],
            "opportunities": list(arbitrage_opportunities),
        }
    )


async def web_index(request):
    html = """
    <!DOCTYPE html><html lang="zh" data-bs-theme="dark"><head><meta charset="UTF-8">
    <title>高频套利监控面板</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background:#0a0a0a; color: #eee; font-family: 'Segoe UI', sans-serif; }
        .stat-card { background: #151515; border: 1px solid #333; border-radius: 12px; padding: 20px; margin-bottom: 20px; }
        .badge-online { background: #2ecc71; color: #fff; } 
        .badge-offline { background: #e74c3c; color: #fff; }
        .qty-text { font-size: 0.85em; color: #aaa; }
        .profit-high { color: #f1c40f; font-weight: bold; font-size: 1.1em; }
        /* 复制按钮样式 */
        .copy-symbol { 
            cursor: pointer; 
            transition: all 0.2s; 
            padding: 2px 6px;
            border-radius: 4px;
            background: rgba(255,255,255,0.05);
        }
        .copy-symbol:hover { background: rgba(255,255,255,0.2); color: #00d2ff; }
        .copy-symbol:active { transform: scale(0.9); }
        /* 提示框 */
        #copy-toast {
            position: fixed; top: 20px; right: 20px; z-index: 1000;
            padding: 10px 20px; background: #2ecc71; color: white;
            border-radius: 5px; display: none;
        }
    </style></head>
    <body class="p-4">
    <div id="copy-toast">已复制到剪贴板</div>
    <div class="container-fluid">
        <div class="row g-3 mb-4">
            <div class="col-md-4"><div class="stat-card"><h6>处理压力 (TPS)</h6><h2 id="tps" class="text-info">-</h2></div></div>
            <div class="col-md-4"><div class="stat-card"><h6>活动连接</h6><h2 class="text-success"><span id="on_cnt">-</span> / <span id="total_cnt">-</span></h2></div></div>
            <div class="col-md-4"><div class="stat-card"><h6>策略限制</h6><div style="font-size: 0.9em;">深度 > 100 USDT | 费率 0.1%</div></div></div>
        </div>
        <div class="row">
            <div class="col-md-4">
                <h5>📡 节点状态</h5>
                <div class="stat-card p-0"><table class="table table-dark mb-0">
                    <thead><tr><th>ID</th><th>交易所</th><th>状态</th><th>延迟</th></tr></thead>
                    <tbody id="worker-tbody"></tbody>
                </table></div>
            </div>
            <div class="col-md-8">
                <h5>⚡ 实时机会 (点击币种复制)</h5>
                <div class="stat-card p-0"><table class="table table-dark table-striped mb-0">
                    <thead><tr><th>时间</th><th>币种</th><th>净收益</th><th>买入(低)</th><th>卖出(高)</th></tr></thead>
                    <tbody id="opp-tbody"></tbody>
                </table></div>
            </div>
        </div>
    </div>
    <script>
        // 格式化名称：去掉 USDT 后缀
        function formatSymbol(s) {
            return s.replace('USDT', '').replace('-USDT', '').replace('_USDT', '');
        }

        // 复制函数
        function copyToClipboard(text) {
            const el = document.createElement('textarea');
            el.value = text;
            document.body.appendChild(el);
            el.select();
            document.execCommand('copy');
            document.body.removeChild(el);
            
            const toast = document.getElementById('copy-toast');
            toast.style.display = 'block';
            setTimeout(() => { toast.style.display = 'none'; }, 1500);
        }

        async function update() {
            try {
                const r = await fetch('/api/data'); const d = await r.json();
                document.getElementById('tps').innerText = d.tps;
                document.getElementById('on_cnt').innerText = d.online_workers;
                document.getElementById('total_cnt').innerText = d.total_workers;
                
                document.getElementById('worker-tbody').innerHTML = d.workers.map(w => `
                    <tr><td>${w.wid}</td><td>${w.ex}</td>
                    <td><span class="badge ${w.status==='ONLINE'?'badge-online':'badge-offline'}">${w.status}</span></td>
                    <td>${w.latency.toFixed(0)}ms</td></tr>`).join('');
                
                document.getElementById('opp-tbody').innerHTML = d.opportunities.map(o => {
                    const cleanSym = formatSymbol(o.symbol);
                    return `
                    <tr>
                        <td>${o.time}</td>
                        <td><strong class="copy-symbol" onclick="copyToClipboard('${cleanSym}')" title="点击复制">${cleanSym}</strong></td>
                        <td class="profit-high">+${o.net_profit}%</td>
                        <td><span class="badge bg-danger">${o.buy_ex}</span> ${o.buy_price}<br><span class="qty-text">$${o.buy_val}</span></td>
                        <td><span class="badge bg-success">${o.sell_ex}</span> ${o.sell_price}<br><span class="qty-text">$${o.sell_val}</span></td>
                    </tr>`;
                }).join('');
            } catch(e) {}
        }
        setInterval(update, 800);
    </script></body></html>
    """
    return web.Response(text=html, content_type="text/html")


# ================= 7. 初始化与启动 =================
async def fetch_symbols(client, name, url):
    try:
        resp = await client.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        pairs = {}
        if name == "Binance":
            for s in data["symbols"]:
                if s["status"] == "TRADING" and s["quoteAsset"] == "USDT":
                    pairs[s["symbol"]] = s["symbol"]
        elif name == "OKX":
            for s in data["data"]:
                if s["state"] == "live" and s["quoteCcy"] == "USDT":
                    pairs[s["instId"].replace("-", "")] = s["instId"]
        elif name == "Bybit":
            for s in data["result"]["list"]:
                if s["quoteCoin"] == "USDT":
                    pairs[s["symbol"]] = s["symbol"]
        elif name == "Gate":
            for s in data:
                if s.get("quote") == "USDT":
                    pairs[s["id"].replace("_", "")] = s["id"]
        elif name == "Bitget":
            for s in data["data"]:
                if s["quoteCoin"] == "USDT":
                    pairs[s["symbol"]] = s["symbol"]
        elif name == "Bitmart":
            syms = data.get("data", {}).get("symbols", [])
            for s in syms:
                if "_USDT" in s:
                    pairs[s.replace("_", "")] = s
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
        "Bitmart": "https://api-cloud.bitmart.com/spot/v1/symbols",
    }

    print("🚀 正在探测多交易所共有交易对...")
    async with httpx.AsyncClient(proxy=PROXIES[0], verify=False) as client:
        results = await asyncio.gather(
            *[fetch_symbols(client, n, u) for n, u in endpoints.items()]
        )
        ex_data = dict(zip(endpoints.keys(), results))

    all_symbols = set()
    for d in ex_data.values():
        all_symbols.update(d.keys())
    watchlist = [
        s for s in all_symbols if sum(1 for ex in ex_data if s in ex_data[ex]) >= 2
    ]
    print(f"✅ 探测到 {len(watchlist)} 个至少在两个交易所上市的交易对")

    app = web.Application()
    app.router.add_get("/", web_index)
    app.router.add_get("/api/data", web_api_data)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", WEB_PORT).start()

    tasks = []
    p_global_idx = 0
    for ex_name, limit in SUB_LIMITS.items():
        supported = {s: ex_data[ex_name][s] for s in watchlist if s in ex_data[ex_name]}
        items = list(supported.items())
        for i in range(0, len(items), limit):
            tasks.append(ws_worker(ex_name, dict(items[i : i + limit]), p_global_idx))
            p_global_idx += 1

    print(f"🔥 事件驱动引擎全速运转中. 请访问 http://127.0.0.1:{WEB_PORT} 查看面板")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping...")
