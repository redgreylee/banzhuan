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
PROXY_URL = 'http://127.0.0.1:7890' # 若在海外服务器请设为 None
PROFIT_THRESHOLD = 0.2              # 目标净利润阈值 (%)
TAKER_FEE_RATE = 0.001              # 默认 Taker 手续费 0.1%
MAX_TIME_DIFF = 2000                # 最大允许两端数据时间差 (ms)
MIN_24H_VOLUME = 500000             # 过滤 24h 成交额低于 50w USDT 的币种

class GlobalArbitrageMonitor:
    def __init__(self):
        # 交易所基础配置
        self.exchange_config = {
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'} 
        }
        if PROXY_URL:
            self.exchange_config['aiohttp_proxy'] = PROXY_URL
            self.exchange_config['wsProxy'] = PROXY_URL

        self.binance = ccxtpro.binance(self.exchange_config)
        self.okx = ccxtpro.okx(self.exchange_config)
        
        # 数据结构: { symbol: { 'binance': {...}, 'okx': {...} } }
        self.price_data = {}
        self.common_symbols = []

    async def init_markets(self):
        """初始化并获取共同的 USDT 现货交易对"""
        try:
            logger.info("🔍 正在拉取交易所市场数据...")
            await asyncio.gather(self.binance.load_markets(), self.okx.load_markets())
            
            # 获取交集：两边都有的 USDT 现货对
            bn_syms = {s for s in self.binance.symbols if s.endswith('/USDT') and ':' not in s}
            ok_syms = {s for s in self.okx.symbols if s.endswith('/USDT') and ':' not in s}
            
            self.common_symbols = list(bn_syms.intersection(ok_syms))
            logger.info(f"✅ 初始化完成。共同 USDT 现货交易对数量: {len(self.common_symbols)}")
            return True
        except Exception as e:
            logger.error(f"❌ 市场初始化失败: {e}")
            return False

    async def watch_exchange_tickers(self, exchange, exchange_name):
        """
        全量监听 Ticker 更新
        """
        if not self.common_symbols:
            return

        logger.info(f"📡 启动 {exchange_name} WebSocket 监听...")
        
        while True:
            try:
                # 传入 common_symbols 列表，解决 OKX 不支持空列表订阅的问题
                # CCXT Pro 会自动处理分片订阅
                tickers = await exchange.watch_tickers(self.common_symbols)
                
                for symbol, ticker in tickers.items():
                    # 只处理我们关心的共同币种
                    if symbol not in self.common_symbols:
                        continue

                    # 24小时成交额过滤（低流动性币种的价差通常无法成交）
                    if ticker.get('quoteVolume') and ticker['quoteVolume'] < MIN_24H_VOLUME:
                        continue

                    if symbol not in self.price_data:
                        self.price_data[symbol] = {}

                    # 填充数据
                    self.price_data[symbol][exchange_name] = {
                        'bid': ticker['bid'],
                        'ask': ticker['ask'],
                        'ts': ticker['timestamp'] or int(time.time() * 1000)
                    }
                    
                    # 尝试计算收益
                    self.check_profit(symbol)

            except Exception as e:
                logger.warning(f"⚠️ {exchange_name} WS 异常: {e}，5秒后重连...")
                await asyncio.sleep(5)

    def check_profit(self, symbol):
        """
        核心套利计算逻辑
        收益率公式:
        $$Profit = \frac{Price_{sell} \times (1 - Fee)}{Price_{buy} \times (1 + Fee)} - 1$$
        """
        data = self.price_data.get(symbol)
        if not data or 'binance' not in data or 'okx' not in data:
            return

        bn = data['binance']
        ok = data['okx']

        # 确保价格数据完整
        if not (bn['bid'] and bn['ask'] and ok['bid'] and ok['ask']):
            return
            
        # 校验两端数据的延迟差，防止因为行情卡顿导致的假价差
        time_diff = abs(bn['ts'] - ok['ts'])
        if time_diff > MAX_TIME_DIFF:
            return

        # ---------------------------------------------------------
        # 路径 A: Binance 买 (吃其 Ask), OKX 卖 (砸其 Bid)
        # ---------------------------------------------------------
        profit_a = (ok['bid'] * (1 - TAKER_FEE_RATE) / (bn['ask'] * (1 + TAKER_FEE_RATE)) - 1) * 100
        if profit_a > PROFIT_THRESHOLD:
            self.log_opportunity("Binance -> OKX", symbol, profit_a, bn['ask'], ok['bid'], time_diff)

        # ---------------------------------------------------------
        # 路径 B: OKX 买 (吃其 Ask), Binance 卖 (砸其 Bid)
        # ---------------------------------------------------------
        profit_b = (bn['bid'] * (1 - TAKER_FEE_RATE) / (ok['ask'] * (1 + TAKER_FEE_RATE)) - 1) * 100
        if profit_b > PROFIT_THRESHOLD:
            self.log_opportunity("OKX -> Binance", symbol, profit_b, ok['ask'], bn['bid'], time_diff)

    def log_opportunity(self, direction, symbol, profit, buy_p, sell_p, diff):
        logger.info(
            f"💰 [套利机会] {symbol} | {direction}\n"
            f"   净利润: {profit:.3f}% | 买入: {buy_p} | 卖出: {sell_p} | 延迟: {diff}ms"
        )

    async def run(self):
        # 1. 第一步：初始化市场并对齐币种
        success = await self.init_markets()
        if not success:
            return

        logger.info(f"🚀 策略正式启动 | 阈值: {PROFIT_THRESHOLD}% | 过滤成交额: {MIN_24H_VOLUME} USDT")
        
        # 2. 第二步：并发执行双端监听
        try:
            await asyncio.gather(
                self.watch_exchange_tickers(self.binance, 'binance'),
                self.watch_exchange_tickers(self.okx, 'okx')
            )
        except Exception as e:
            logger.error(f"🔥 系统崩溃: {e}")
        finally:
            await self.close()

    async def close(self):
        logger.info("🔌 正在关闭连接...")
        await self.binance.close()
        await self.okx.close()

if __name__ == "__main__":
    bot = GlobalArbitrageMonitor()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("\n👋 用户手动停止。")