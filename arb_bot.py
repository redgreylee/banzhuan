import asyncio
import ccxt.pro as ccxtpro
import ccxt
import time
import logging
from collections import defaultdict, deque
from logging.handlers import TimedRotatingFileHandler

# Web面板依赖
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi import WebSocket, WebSocketDisconnect
import uvicorn
import threading
import json
from typing import List

# --- 1. 日志配置 ---
logger = logging.getLogger("ArbBot")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

file_handler = TimedRotatingFileHandler('arbitrage.log', when='midnight', interval=1, backupCount=7, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- 2. 核心配置区 ---
# WebSocket 连接管理器
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """广播消息到所有客户端"""
        for connection in self.active_connections[:]:  # 复制列表避免迭代时修改
            try:
                await connection.send_json(message)
            except Exception:
                # 移除无效连接
                self.active_connections.remove(connection)

# 全局 WebSocket 管理器
ws_manager = ConnectionManager()

# 已剔除：htx, mexc, kucoin
EXCHANGE_IDS = ['binance', 'okx', 'bybit', 'gate', 'bitget', 'bitmart']

PROXY_URL = 'http://127.0.0.1:7890' 

PROFIT_THRESHOLD = 0.8
MAX_TIME_DIFF_MS = 3000
REST_POLL_INTERVAL = 2.0

FEE_RATES = {
    'binance': 0.0010, 
    'okx': 0.0010, 
    'bybit': 0.0010,
    'gate': 0.0020, 
    'bitget': 0.0020, 
    'bitmart': 0.0025,
    'default': 0.0020
}

MIN_QUOTE_VOLUME = 50000
MIN_DEPTH_USDT = 100
ALERT_COOLDOWN = 60
MAX_PRICE_RATIO = 10.0

class GlobalArbitrageMonitor:
    def __init__(self):
        self.exchanges = {}
        self.price_data = defaultdict(dict)
        self.target_symbols = set()
        self.opp_count = 0
        self.last_alert_time = defaultdict(lambda: defaultdict(int))
        self.recent_opportunities = deque(maxlen=50)

    async def init_exchanges(self):
        base_config = {
            'enableRateLimit': True,
            'options': {'defaultType': 'spot', 'warnOnFetchOpenOrdersWithoutSymbol': False},
            'newUpdates': False,
            'timeout': 20000,
        }
        if PROXY_URL:
            base_config['aiohttp_proxy'] = PROXY_URL
            base_config['wsProxy'] = PROXY_URL
            base_config['proxies'] = {'http': PROXY_URL, 'https': PROXY_URL}

        logger.info(f"🔌 正在连接 {len(EXCHANGE_IDS)} 个交易所...")
        tasks = [self._load_exchange(eid, base_config) for eid in EXCHANGE_IDS]
        await asyncio.gather(*tasks)

        symbol_counter = defaultdict(int)
        valid_exchanges = 0
        for eid, ex in self.exchanges.items():
            if not ex.markets: continue
            valid_exchanges += 1
            for symbol in ex.markets:
                market = ex.markets[symbol]
                if market.get('spot') and symbol.endswith('/USDT') and market.get('active'):
                    symbol_counter[symbol] += 1

        self.target_symbols = {s for s, c in symbol_counter.items() if c >= 2}

        if not self.target_symbols:
            logger.error("❌ 未找到共同交易对，请检查网络或代理！")
            return False

        logger.info(f"✅ 初始化完成 | 有效交易所: {valid_exchanges} | 监控币种: {len(self.target_symbols)} 个")
        return True

    async def _load_exchange(self, eid, config):
        try:
            ex_class = getattr(ccxtpro, eid)
            exchange = ex_class(config)
            await exchange.load_markets()
            self.exchanges[eid] = exchange
            logger.info(f"🟢 {eid} 就绪")
        except Exception as e:
            logger.error(f"🔴 {eid} 初始化失败: {e}")

    async def watch_tickers_loop(self, eid):
        ex = self.exchanges[eid]
        relevant_symbols = {s for s in self.target_symbols if s in ex.markets}
        if not relevant_symbols: return

        # 默认使用WS，MEXC移除后此处通常为False
        use_rest_mode = False 
        
        while True:
            try:
                if use_rest_mode:
                    tickers = await ex.fetch_tickers()
                    await asyncio.sleep(REST_POLL_INTERVAL)
                else:
                    try:
                        tickers = await ex.watch_tickers()
                    except (ccxt.NotSupported, ccxt.ExchangeError, ccxt.BadRequest) as e:
                        if "support" in str(e).lower() or "method" in str(e).lower():
                            logger.warning(f"⚠️ [{eid}] WS不可用，降级为REST")
                            use_rest_mode = True
                            continue
                        else:
                            raise

                self.process_data(eid, tickers, relevant_symbols)

            except Exception as e:
                err_msg = str(e)
                if "support" in err_msg.lower() and not use_rest_mode:
                    use_rest_mode = True
                else:
                    logger.warning(f"⚠️ [{eid}] 连接抖动: {err_msg[:80]}... 5秒后重试")
                    await asyncio.sleep(5)

    def process_data(self, eid, tickers, relevant_symbols):
        current_ts = time.time() * 1000
        for symbol, ticker in tickers.items():
            if symbol not in relevant_symbols: continue

            bid = ticker.get('bid')
            ask = ticker.get('ask')
            bid_vol = ticker.get('bidVolume')
            ask_vol = ticker.get('askVolume')

            if not bid or not ask: continue

            self.price_data[symbol][eid] = {
                'bid': float(bid),
                'ask': float(ask),
                'bidVol': float(bid_vol) if bid_vol is not None else None,
                'askVol': float(ask_vol) if ask_vol is not None else None,
                'ts': ticker.get('timestamp') or current_ts,
                'quoteVolume': ticker.get('quoteVolume', 0)
            }
            self.calculate_arbitrage(symbol, trigger_ex=eid)

    def calculate_arbitrage(self, symbol, trigger_ex):
        platforms = self.price_data.get(symbol)
        if not platforms or len(platforms) < 2: return

        trigger_data = platforms[trigger_ex]
        for other_ex, other_data in platforms.items():
            if other_ex == trigger_ex: continue
            time_diff = abs(trigger_data['ts'] - other_data['ts'])
            if time_diff > MAX_TIME_DIFF_MS: continue

            self.check_profit(trigger_ex, other_ex, symbol, trigger_data['ask'], other_data['bid'])
            self.check_profit(other_ex, trigger_ex, symbol, other_data['ask'], trigger_data['bid'])

    def check_profit(self, buy_ex, sell_ex, symbol, buy_price, sell_price):
        if buy_price <= 0 or sell_price <= buy_price: return

        price_ratio = max(sell_price / buy_price, buy_price / sell_price)
        if price_ratio > MAX_PRICE_RATIO: return

        platforms = self.price_data.get(symbol, {})
        buy_data = platforms.get(buy_ex, {})
        sell_data = platforms.get(sell_ex, {})

        buy_vol_24h = buy_data.get('quoteVolume', 0)
        sell_vol_24h = sell_data.get('quoteVolume', 0)

        if buy_vol_24h < MIN_QUOTE_VOLUME or sell_vol_24h < MIN_QUOTE_VOLUME: return

        buy_vol = buy_data.get('askVol')
        sell_vol = sell_data.get('bidVol')
        buy_depth_usdt = buy_vol * buy_price if buy_vol is not None else 0
        sell_depth_usdt = sell_vol * sell_price if sell_vol is not None else 0

        if MIN_DEPTH_USDT > 0 and (buy_depth_usdt < MIN_DEPTH_USDT or sell_depth_usdt < MIN_DEPTH_USDT):
            return

        buy_fee = FEE_RATES.get(buy_ex, FEE_RATES['default'])
        sell_fee = FEE_RATES.get(sell_ex, FEE_RATES['default'])
        net_profit = (sell_price * (1 - sell_fee) / (buy_price * (1 + buy_fee)) - 1) * 100

        if net_profit > PROFIT_THRESHOLD:
            alert_key = (buy_ex, sell_ex)
            now = time.time()
            if now - self.last_alert_time[symbol][alert_key] < ALERT_COOLDOWN: return
            self.last_alert_time[symbol][alert_key] = now

            self.opp_count += 1
            gross = (sell_price - buy_price) / buy_price * 100
            
            buy_depth_str = f"{buy_depth_usdt:.1f} " if buy_vol is not None else "N/A"
            sell_depth_str = f"{sell_depth_usdt:.1f} " if sell_vol is not None else "N/A"

            logger.info(f"🔥 [机会 #{self.opp_count}] {symbol} | {buy_ex} → {sell_ex} | 净利: {net_profit:.2f}%")

            opportunity = {
                'time': time.strftime('%H:%M:%S'),
                'symbol': symbol,
                'buy_ex': buy_ex,
                'sell_ex': sell_ex,
                'net_profit': round(net_profit, 2),
                'gross': round(gross, 2),
                'buy_price': buy_price,
                'sell_price': sell_price,
                'buy_vol': buy_depth_str,
                'sell_vol': sell_depth_str
            }
            self.recent_opportunities.append(opportunity)
            
            # 通过 WebSocket 广播新机会
            asyncio.create_task(ws_manager.broadcast({
                'type': 'new_opportunity',
                'data': opportunity
            }))

    def start_web_panel(self):
        app = FastAPI(title="套利机器人 - Web面板")
        monitor = self  # 引用到闭包中

        # WebSocket 端点
        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await ws_manager.connect(websocket)
            try:
                # 发送当前状态
                await websocket.send_json({
                    'type': 'init',
                    'exchanges': len(monitor.exchanges),
                    'symbols': len(monitor.target_symbols),
                    'opp_count': monitor.opp_count
                })
                while True:
                    # 保持连接，等待心跳或断开
                    try:
                        await asyncio.wait_for(websocket.receive_text(), timeout=30)
                    except asyncio.TimeoutError:
                        # 发送心跳
                        await websocket.send_json({'type': 'ping'})
            except WebSocketDisconnect:
                ws_manager.disconnect(websocket)

        # 获取最新机会的 REST API
        @app.get("/api/opportunities")
        async def get_opportunities():
            return list(monitor.recent_opportunities)[-30:]

        # 获取统计信息
        @app.get("/api/stats")
        async def get_stats():
            return {
                'exchanges': len(monitor.exchanges),
                'symbols': len(monitor.target_symbols),
                'opp_count': monitor.opp_count
            }

        @app.get("/", response_class=HTMLResponse)
        async def dashboard():
            # WebSocket 实时推送版本
            html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>套利机器人 - 实时监控面板</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .highlight { animation: flash 1s ease-out; }
        @keyframes flash {
            0% { background-color: rgba(52, 211, 153, 0.3); }
            100% { background-color: transparent; }
        }
    </style>
</head>
<body class="bg-zinc-950 text-zinc-200">
    <div class="max-w-7xl mx-auto p-8">
        <div class="flex justify-between items-center mb-8">
            <h1 class="text-4xl font-bold text-emerald-400">🔥 跨所现货套利实时监控</h1>
            <div class="flex items-center gap-4">
                <div class="flex items-center gap-2">
                    <div id="ws-status" class="w-2 h-2 rounded-full bg-yellow-400"></div>
                    <span id="ws-text" class="text-sm text-zinc-500">连接中...</span>
                </div>
                <div class="text-sm text-zinc-500" id="update-time"></div>
            </div>
        </div>

        <div class="grid grid-cols-4 gap-6 mb-10">
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">已连接交易所</div>
                <div class="text-5xl font-bold text-white mt-2" id="exchanges-count">0</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">监控交易对</div>
                <div class="text-5xl font-bold text-white mt-2" id="symbols-count">0</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">累计发现机会</div>
                <div class="text-5xl font-bold text-emerald-400 mt-2" id="opp-count">0</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">面板端口</div>
                <div class="text-5xl font-bold text-amber-400 mt-2">8000</div>
            </div>
        </div>

        <div class="bg-zinc-900 rounded-3xl overflow-hidden border border-zinc-800">
            <div class="bg-zinc-800 px-8 py-5 flex justify-between items-center">
                <h2 class="text-2xl font-semibold">💰 最新套利机会</h2>
                <div class="flex items-center gap-3">
                    <span class="text-xs bg-zinc-700 px-4 py-1.5 rounded-full">实时推送</span>
                    <span class="text-xs bg-emerald-900 text-emerald-300 px-3 py-1 rounded-full" id="new-badge" style="display:none">新!</span>
                </div>
            </div>
            <table class="w-full text-sm">
                <thead>
                    <tr class="bg-zinc-950 border-b border-zinc-800 text-zinc-400 text-xs">
                        <th class="px-6 py-5 text-left font-normal">时间</th>
                        <th class="px-6 py-5 text-left font-normal">交易对</th>
                        <th class="px-6 py-5 text-left font-normal">买卖方向</th>
                        <th class="px-6 py-5 text-right font-normal">净利</th>
                        <th class="px-6 py-5 text-right font-normal">买价</th>
                        <th class="px-6 py-5 text-left font-normal">可买入 (USDT)</th>
                        <th class="px-6 py-5 text-right font-normal">卖价</th>
                        <th class="px-6 py-5 text-left font-normal">可卖出 (USDT)</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-zinc-800" id="opportunities-tbody">
                </tbody>
            </table>
            <div id="empty-msg" class="text-center py-20 text-zinc-500">暂无机会，耐心等待...</div>
        </div>
    
        <div class="text-center text-xs text-zinc-600 mt-8">
            跨所套利机器人 • 高雄本地运行 • 按 Ctrl+C 停止
        </div>
    </div>

    <script>
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(wsProtocol + '//' + window.location.host + '/ws');
        let opportunities = [];
        
        // 格式化利润颜色
        function getProfitColor(profit) {
            if (profit >= 2) return 'text-green-400';
            if (profit >= 1) return 'text-yellow-400';
            return 'text-orange-400';
        }
        
        // 渲染表格行
        function renderRow(opp, isNew = false) {
            const profitColor = getProfitColor(opp.net_profit);
            const highlight = isNew ? 'highlight' : '';
            return `<tr class="border-b border-gray-700 hover:bg-gray-750 ${highlight}">
                <td class="px-6 py-4 text-sm text-gray-400">${opp.time}</td>
                <td class="px-6 py-4 font-medium text-white">${opp.symbol}</td>
                <td class="px-6 py-4">
                    <span class="px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-900 text-green-300">${opp.buy_ex}</span>
                    <span class="mx-2 text-gray-500">→</span>
                    <span class="px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-900 text-red-300">${opp.sell_ex}</span>
                </td>
                <td class="px-6 py-4 text-right font-mono ${profitColor} font-bold">${opp.net_profit}%</td>
                <td class="px-6 py-4 text-right text-white">${opp.buy_price}</td>
                <td class="px-6 py-4 text-sm text-gray-300">${opp.buy_vol}</td>
                <td class="px-6 py-4 text-right text-white">${opp.sell_price}</td>
                <td class="px-6 py-4 text-sm text-gray-300">${opp.sell_vol}</td>
            </tr>`;
        }
        
        // 渲染表格
        function renderTable() {
            const tbody = document.getElementById('opportunities-tbody');
            const emptyMsg = document.getElementById('empty-msg');
            
            if (opportunities.length === 0) {
                tbody.innerHTML = '';
                emptyMsg.style.display = 'block';
                return;
            }
            
            emptyMsg.style.display = 'none';
            tbody.innerHTML = opportunities.map(opp => renderRow(opp)).join('');
        }
        
        // 更新统计
        function updateStats(data) {
            document.getElementById('exchanges-count').textContent = data.exchanges || 0;
            document.getElementById('symbols-count').textContent = data.symbols || 0;
            document.getElementById('opp-count').textContent = data.opp_count || 0;
        }
        
        // 更新连接状态
        function setConnected(connected) {
            const status = document.getElementById('ws-status');
            const text = document.getElementById('ws-text');
            if (connected) {
                status.className = 'w-2 h-2 rounded-full bg-green-400';
                text.textContent = '已连接';
            } else {
                status.className = 'w-2 h-2 rounded-full bg-red-400';
                text.textContent = '断开连接';
            }
        }
        
        // 显示新机会提醒
        function showNewBadge() {
            const badge = document.getElementById('new-badge');
            badge.style.display = 'inline';
            setTimeout(() => { badge.style.display = 'none'; }, 3000);
        }
        
        // 更新最后时间
        function updateTime() {
            const now = new Date();
            document.getElementById('update-time').textContent = now.toLocaleString('zh-CN');
        }
        
        // WebSocket 消息处理
        ws.onopen = () => {
            setConnected(true);
            // 获取初始数据
            fetch('/api/opportunities').then(r => r.json()).then(data => {
                opportunities = data;
                renderTable();
                updateTime();
            });
            fetch('/api/stats').then(r => r.json()).then(updateStats);
        };
        
        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            
            if (msg.type === 'init') {
                updateStats(msg);
            } else if (msg.type === 'new_opportunity') {
                // 添加新机会到列表
                opportunities.unshift(msg.data);
                if (opportunities.length > 30) opportunities.pop();
                renderTable();
                showNewBadge();
                updateTime();
                // 更新计数器
                const countEl = document.getElementById('opp-count');
                countEl.textContent = parseInt(countEl.textContent) + 1;
            } else if (msg.type === 'ping') {
                ws.send('pong');
            }
        };
        
        ws.onclose = () => {
            setConnected(false);
            // 尝试重连
            setTimeout(() => {
                window.location.reload();
            }, 3000);
        };
        
        ws.onerror = () => {
            setConnected(false);
        };
        
        // 每30秒刷新一次统计数据（备用）
        setInterval(() => {
            fetch('/api/stats').then(r => r.json()).then(updateStats);
        }, 30000);
    </script>
</body>
</html>
            """
            return html


        def run_server():
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        logger.info("🌐 Web面板已启动 → http://127.0.0.1:8000")

    async def run(self):
        if not await self.init_exchanges(): return
        self.start_web_panel()
        tasks = [self.watch_tickers_loop(eid) for eid in self.exchanges.keys()]
        logger.info("🚀 监控引擎启动...")
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"主程序错误: {e}")
        finally:
            await self.shutdown()

    async def shutdown(self):
        for ex in self.exchanges.values():
            await ex.close()

if __name__ == "__main__":
    bot = GlobalArbitrageMonitor()
    try:
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass