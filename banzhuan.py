import asyncio
import ccxt.pro as ccxtpro
import ccxt
import time
import logging
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler

# --- 1. 日志配置（控制台 + 每日轮转文件，保留7天）---
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
EXCHANGE_IDS = ['binance', 'okx']
##EXCHANGE_IDS = ['binance', 'okx', 'bybit', 'gate', 'kucoin', 'mexc', 'bitget']


PROXY_URL = 'http://127.0.0.1:7890'   # 台湾环境若无需代理请改成 '' 

PROFIT_THRESHOLD = 0.8          # 净利阈值（%），建议 0.5~1.0
MAX_TIME_DIFF_MS = 3000         # 价格有效时间窗口（毫秒）
REST_POLL_INTERVAL = 2.0        # REST轮询间隔（秒）

# 个性化手续费（Taker）
FEE_RATES = {
    'binance': 0.0010,
    'okx': 0.0010,
    'bybit': 0.0010,
    'gate': 0.0020,
    'kucoin': 0.0020,
    'mexc': 0.0020,
    'bitget': 0.0020,
    'default': 0.0020
}

MIN_QUOTE_VOLUME = 50000        # 24h USDT成交量阈值（过滤小流动性）
MIN_DEPTH_USDT = 100            # 顶层深度过滤：买/卖双方至少100 USDT可吃（设0关闭）
ALERT_COOLDOWN = 60             # 同一买卖路径冷却秒数

class GlobalArbitrageMonitor:
    def __init__(self):
        self.exchanges = {}
        self.price_data = defaultdict(dict)          # {symbol: {ex: {bid, ask, bidVol, askVol, ts, quoteVolume}}}
        self.target_symbols = set()
        self.opp_count = 0
        self.last_alert_time = defaultdict(lambda: defaultdict(int))  # symbol -> (buy_ex, sell_ex) -> timestamp

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
        if not relevant_symbols:
            return

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
            if symbol not in relevant_symbols:
                continue

            bid = ticker.get('bid')
            ask = ticker.get('ask')
            bid_vol = ticker.get('bidVolume')
            ask_vol = ticker.get('askVolume')

            if not bid or not ask:
                continue

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
        if not platforms or len(platforms) < 2:
            return

        trigger_data = platforms[trigger_ex]
        for other_ex, other_data in platforms.items():
            if other_ex == trigger_ex:
                continue

            time_diff = abs(trigger_data['ts'] - other_data['ts'])
            if time_diff > MAX_TIME_DIFF_MS:
                continue

            self.check_profit(trigger_ex, other_ex, symbol, trigger_data['ask'], other_data['bid'])
            self.check_profit(other_ex, trigger_ex, symbol, other_data['ask'], trigger_data['bid'])

    def check_profit(self, buy_ex, sell_ex, symbol, buy_price, sell_price):
        if buy_price <= 0 or sell_price <= buy_price:
            return

        platforms = self.price_data.get(symbol, {})
        buy_data = platforms.get(buy_ex, {})
        sell_data = platforms.get(sell_ex, {})

        # 深度读取
        buy_vol = buy_data.get('askVol')
        sell_vol = sell_data.get('bidVol')
        buy_depth_usdt = buy_vol * buy_price if buy_vol is not None else 0
        sell_depth_usdt = sell_vol * sell_price if sell_vol is not None else 0

        # 深度过滤
        if MIN_DEPTH_USDT > 0 and (buy_depth_usdt < MIN_DEPTH_USDT or sell_depth_usdt < MIN_DEPTH_USDT):
            return

        # 手续费 & 净利
        buy_fee = FEE_RATES.get(buy_ex, FEE_RATES['default'])
        sell_fee = FEE_RATES.get(sell_ex, FEE_RATES['default'])
        net_profit = (sell_price * (1 - sell_fee) / (buy_price * (1 + buy_fee)) - 1) * 100

        if net_profit > PROFIT_THRESHOLD:
            # 冷却防刷屏
            alert_key = (buy_ex, sell_ex)
            now = time.time()
            if now - self.last_alert_time[symbol][alert_key] < ALERT_COOLDOWN:
                return
            self.last_alert_time[symbol][alert_key] = now

            self.opp_count += 1
            gross = (sell_price - buy_price) / buy_price * 100

            base = symbol.split('/')[0]
            buy_vol_str = f"{buy_vol:.6f} {base} ({buy_depth_usdt:.1f} USDT)" if buy_vol is not None else "N/A"
            sell_vol_str = f"{sell_vol:.6f} {base} ({sell_depth_usdt:.1f} USDT)" if sell_vol is not None else "N/A"

            logger.info(
                f"🔥 [机会 #{self.opp_count}] {symbol} | "
                f"{buy_ex} → {sell_ex} | 净利: {net_profit:.2f}% (毛利 {gross:.2f}%)\n"
                f"   🟢 买价: {buy_price:<12} | 可买入: {buy_vol_str}\n"
                f"   🔴 卖价: {sell_price:<12} | 可卖出: {sell_vol_str}\n"
                f"   📊 24h量: {buy_data.get('quoteVolume',0):.0f} / {sell_data.get('quoteVolume',0):.0f} USDT"
            )

    async def run(self):
        if not await self.init_exchanges():
            return
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
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass