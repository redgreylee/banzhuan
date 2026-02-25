import asyncio
import ccxt.pro as ccxtpro
import ccxt
import time
import logging
from collections import defaultdict, deque  # 引入 deque
from logging.handlers import TimedRotatingFileHandler

# Web面板依赖
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
import threading

# --- 1. 日志配置（控制台 + 每日轮转文件）---
logger = logging.getLogger("ArbBot")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

file_handler = TimedRotatingFileHandler(
    'arbitrage.log',
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- 2. 核心配置区 ---
EXCHANGE_IDS = ['binance', 'okx', 'bybit', 'gate', 'kucoin', 'mexc', 'bitget', 'bitmart', 'htx']

PROXY_URL = 'http://127.0.0.1:7890'   # 台湾环境若无需代理请改成 ''

PROFIT_THRESHOLD = 0.8
MAX_TIME_DIFF_MS = 3000
REST_POLL_INTERVAL = 2.0

FEE_RATES = {
    'binance': 0.0010, 'okx': 0.0010, 'bybit': 0.0010,
    'gate': 0.0020, 'kucoin': 0.0020, 'mexc': 0.0020,
    'bitget': 0.0020, 
    'htx': 0.0020,      # 新增：火币手续费 0.2%
    'bitmart': 0.0025,  # 新增：Bitmart手续费 0.25%
    'default': 0.0020
}

MIN_QUOTE_VOLUME = 50000
MIN_DEPTH_USDT = 100
ALERT_COOLDOWN = 60

# 防ticker冲突（同一个符号不同币种）
MAX_PRICE_RATIO = 10.0

class GlobalArbitrageMonitor:
    def __init__(self):
        self.exchanges = {}
        self.price_data = defaultdict(dict)
        self.target_symbols = set()
        self.opp_count = 0
        self.last_alert_time = defaultdict(lambda: defaultdict(int))
        
        # 【优化】使用双端队列管理机会列表，设定最大长度 50，具备优秀的线程安全性及 O(1) 性能
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

        use_rest_mode = (eid == 'mexc')
        if use_rest_mode:
            logger.info(f"ℹ️  [{eid}] 使用 REST 轮询模式")
        else:
            logger.info(f"📡 [{eid}] 启动 WebSocket 全量监控")

        while True:
            try:
                if use_rest_mode:
                    tickers = await ex.fetch_tickers()
                    await asyncio.sleep(REST_POLL_INTERVAL)
                else:
                    try:
                        # 对于支持全量WS的交易所，无参调用即可；若遇到订阅失败，也可尝试改为 ex.watch_tickers(list(relevant_symbols))
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
        if buy_price <= 0 or sell_price <= buy_price:
            return

        price_ratio = max(sell_price / buy_price, buy_price / sell_price)
        if price_ratio > MAX_PRICE_RATIO:
            return

        platforms = self.price_data.get(symbol, {})
        buy_data = platforms.get(buy_ex, {})
        sell_data = platforms.get(sell_ex, {})

        # 新增：过滤 24h 交易量为 0 或低于阈值的情况
        buy_vol_24h = buy_data.get('quoteVolume', 0)
        sell_vol_24h = sell_data.get('quoteVolume', 0)

        if buy_vol_24h == 0 or sell_vol_24h == 0 or \
           buy_vol_24h < MIN_QUOTE_VOLUME or sell_vol_24h < MIN_QUOTE_VOLUME:
            return

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
            if now - self.last_alert_time[symbol][alert_key] < ALERT_COOLDOWN:
                return
            self.last_alert_time[symbol][alert_key] = now

            self.opp_count += 1
            gross = (sell_price - buy_price) / buy_price * 100

            buy_depth_str = f"{buy_depth_usdt:.1f} " if buy_vol is not None else "N/A"
            sell_depth_str = f"{sell_depth_usdt:.1f} " if sell_vol is not None else "N/A"

            logger.info(
                f"🔥 [机会 #{self.opp_count}] {symbol} | "
                f"{buy_ex} → {sell_ex} | 净利: {net_profit:.2f}% (毛利 {gross:.2f}%)\n"
                f"   🟢 买价: {buy_price:<12} | 可买入: {buy_depth_str}\n"
                f"   🔴 卖价: {sell_price:<12} | 可卖出: {sell_depth_str}\n"
                f"   📊 24h量: {buy_data.get('quoteVolume',0):.0f} / {sell_data.get('quoteVolume',0):.0f} USDT"
            )

            self.recent_opportunities.append({
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
            })

    def start_web_panel(self):
        app = FastAPI(title="套利机器人 - Web面板")

        @app.get("/", response_class=HTMLResponse)
        async def dashboard():
            rows = ""
            # 【优化】将 deque 转换为 list 后进行切片，避免 TypeError
            recent_list = list(self.recent_opportunities)[-30:]
            for opp in reversed(recent_list):
                profit_color = "text-green-400" if opp['net_profit'] >= 2 else "text-yellow-400"
                rows += f"""
                <tr class="border-b border-gray-700 hover:bg-gray-750">
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-400">{opp['time']}</td>
                    <td class="px-6 py-4 font-medium text-white">{opp['symbol']}</td>
                    <td class="px-6 py-4">
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-900 text-green-300">
                            {opp['buy_ex']}
                        </span>
                        <span class="mx-2 text-gray-500">→</span>
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-900 text-red-300">
                            {opp['sell_ex']}
                        </span>
                    </td>
                    <td class="px-6 py-4 text-right font-mono {profit_color} font-bold">{opp['net_profit']}%</td>
                    <td class="px-6 py-4 text-right text-white">{opp['buy_price']}</td>
                    <td class="px-6 py-4 text-sm text-gray-300">{opp['buy_vol']}</td>
                    <td class="px-6 py-4 text-right text-white">{opp['sell_price']}</td>
                    <td class="px-6 py-4 text-sm text-gray-300">{opp['sell_vol']}</td>
                </tr>
                """

            html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>套利机器人 - 实时监控面板</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>function refresh() {{ location.reload(); }} setInterval(refresh, 5000);</script>
</head>
<body class="bg-zinc-950 text-zinc-200">
    <div class="max-w-7xl mx-auto p-8">
        <div class="flex justify-between items-center mb-8">
            <h1 class="text-4xl font-bold text-emerald-400">🔥 跨所现货套利实时监控</h1>
            <div class="text-sm text-zinc-500">每5秒刷新 • {time.strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>

        <div class="grid grid-cols-4 gap-6 mb-10">
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">已连接交易所</div>
                <div class="text-5xl font-bold text-white mt-2">{len(self.exchanges)}</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">监控交易对</div>
                <div class="text-5xl font-bold text-white mt-2">{len(self.target_symbols)}</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">累计发现机会</div>
                <div class="text-5xl font-bold text-emerald-400 mt-2">{self.opp_count}</div>
            </div>
            <div class="bg-zinc-900 p-6 rounded-2xl border border-zinc-800">
                <div class="text-xs text-zinc-500">面板端口</div>
                <div class="text-5xl font-bold text-amber-400 mt-2">8000</div>
            </div>
        </div>

        <div class="bg-zinc-900 rounded-3xl overflow-hidden border border-zinc-800">
            <div class="bg-zinc-800 px-8 py-5 flex justify-between items-center">
                <h2 class="text-2xl font-semibold">💰 最新套利机会</h2>
                <div class="text-xs bg-zinc-700 px-4 py-1.5 rounded-full">最新30条</div>
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
                <tbody class="divide-y divide-zinc-800">
                    {rows}
                </tbody>
            </table>
            {'' if self.recent_opportunities else '<div class="text-center py-20 text-zinc-500">暂无机会，耐心等待...</div>'}
        </div>

        <div class="text-center text-xs text-zinc-600 mt-8">
            跨所套利机器人 • 高雄本地运行 • 按 Ctrl+C 停止
        </div>
    </div>
</body>
</html>
            """
            return html

        def run_server():
            # 【优化】将 log_level 调整为 warning，防止页面定时刷新的 200 OK 刷屏
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        logger.info("🌐 Web面板已启动 → http://127.0.0.1:8000")

    async def run(self):
        if not await self.init_exchanges():
            return
        self.start_web_panel()
        tasks = [self.watch_tickers_loop(eid) for eid in self.exchanges.keys()]
        logger.info("🚀 监控引擎启动... (Ctrl+C 停止)")
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"主程序错误: {e}")
        finally:
            await self.shutdown()

    async def shutdown(self):
        logger.info("正在关闭连接...")
        for ex in self.exchanges.values():
            await ex.close()
        logger.info("👋 再见")

if __name__ == "__main__":
    bot = GlobalArbitrageMonitor()
    try:
        # 兼容 Windows ProactorEventLoop 在 CCXT Pro 下的报错问题
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass