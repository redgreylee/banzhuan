import asyncio
import ccxt.pro as ccxtpro
import time
import logging

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("GlobalArbitrage")

# --- 配置区 ---
PROXY_URL = 'http://127.0.0.1:7890' 
PROFIT_THRESHOLD = 0.2        # 全量监控时建议略微调高阈值，过滤杂讯 (%)
TAKER_FEE_RATE = 0.001        # 0.1% 手续费
MAX_TIME_DIFF = 2000          # 允许的时间差 (ms)
MIN_24H_VOLUME = 1000000      # 过滤掉 24 小时成交额小于 100w USDT 的垃圾币

class GlobalArbitrageMonitor:
    def __init__(self):
        self.exchange_config = {
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'} # 确保是现货
        }
        if PROXY_URL:
            self.exchange_config['aiohttp_proxy'] = PROXY_URL
            self.exchange_config['wsProxy'] = PROXY_URL

        self.binance = ccxtpro.binance(self.exchange_config)
        self.okx = ccxtpro.okx(self.exchange_config)
        
        # 存储格式: { symbol: { 'binance': ticker_obj, 'okx': ticker_obj } }
        self.price_data = {}

    async def watch_exchange_tickers(self, exchange, exchange_name):
        """
        全量监听交易所的所有 Ticker
        """
        logger.info(f"📡 启动 {exchange_name} 全量 Ticker 监听...")
        while True:
            try:
                # CCXT Pro 的 watch_tickers 不传参数通常会获取所有活跃交易对
                tickers = await exchange.watch_tickers()
                
                for symbol, ticker in tickers.items():
                    # 1. 基础过滤：仅限 USDT 现货交易对
                    if not symbol.endswith('/USDT') or ':' in symbol:
                        continue
                    
                    # 2. 活跃度过滤：成交量太小的币价差往往是“幻觉”
                    if ticker.get('quoteVolume') and ticker['quoteVolume'] < MIN_24H_VOLUME:
                        continue

                    if symbol not in self.price_data:
                        self.price_data[symbol] = {}

                    # 更新行情
                    self.price_data[symbol][exchange_name] = {
                        'bid': ticker['bid'],
                        'ask': ticker['ask'],
                        'bid_vol': ticker.get('bidVolume', 0),
                        'ask_vol': ticker.get('askVolume', 0),
                        'ts': ticker['timestamp'] or int(time.time() * 1000)
                    }
                    
                    # 每次更新都尝试触发计算（仅针对当前更新的币种）
                    self.check_profit(symbol)

            except Exception as e:
                logger.error(f"❌ {exchange_name} 连接异常: {e}")
                await asyncio.sleep(5)

    def check_profit(self, symbol):
        """计算逻辑"""
        data = self.price_data.get(symbol)
        if not data or 'binance' not in data or 'okx' not in data:
            return

        bn = data['binance']
        ok = data['okx']

        # 校验数据有效性
        if not (bn['bid'] and bn['ask'] and ok['bid'] and ok['ask']):
            return
            
        # 校验时间同步
        if abs(bn['ts'] - ok['ts']) > MAX_TIME_DIFF:
            return

        # 计算路径 A: Binance 买 -> OKX 卖
        # $Profit = \frac{SellPrice \times (1 - Fee)}{BuyPrice \times (1 + Fee)} - 1$
        profit_a = (ok['bid'] * (1 - TAKER_FEE_RATE) / (bn['ask'] * (1 + TAKER_FEE_RATE)) - 1) * 100
        if profit_a > PROFIT_THRESHOLD:
            self.log_opportunity("Binance -> OKX", symbol, profit_a, bn['ask'], ok['bid'])

        # 计算路径 B: OKX 买 -> Binance 卖
        profit_b = (bn['bid'] * (1 - TAKER_FEE_RATE) / (ok['ask'] * (1 + TAKER_FEE_RATE)) - 1) * 100
        if profit_b > PROFIT_THRESHOLD:
            self.log_opportunity("OKX -> Binance", symbol, profit_b, ok['ask'], bn['bid'])

    def log_opportunity(self, direction, symbol, profit, buy_p, sell_p):
        logger.info(f"💰 [机会] {symbol} | {direction} | 收益: {profit:.3f}% | 买: {buy_p} / 卖: {sell_p}")

    async def run(self):
        # 初始化市场信息
        await asyncio.gather(self.binance.load_markets(), self.okx.load_markets())
        
        # 并发监听两家交易所的全量行情
        await asyncio.gather(
            self.watch_exchange_tickers(self.binance, 'binance'),
            self.watch_exchange_tickers(self.okx, 'okx')
        )

if __name__ == "__main__":
    bot = GlobalArbitrageMonitor()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass