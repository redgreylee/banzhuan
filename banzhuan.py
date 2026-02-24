import asyncio
import ccxt.pro as ccxtpro
import ccxt
import time
import logging
from collections import defaultdict

# --- 1. 日志配置 ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("ArbBot")

# --- 2. 核心配置区 ---
# 交易所列表
EXCHANGE_IDS = ['binance', 'okx', 'bybit', 'gate', 'kucoin', 'mexc', 'bitget']

# 代理设置 (国内环境必须配置，否则无法连接)
# 如果使用 V2RayN/Clash，通常是 127.0.0.1:7890 或 10809
PROXY_URL = 'http://127.0.0.1:7890' 

# 阈值设置
PROFIT_THRESHOLD = 0.6     # 净利润阈值 (%)，达到此值才打印日志
TAKER_FEE_RATE = 0.002     # 预估单边手续费 (0.2%)
MAX_TIME_DIFF_MS = 3000    # 价格有效时间窗口 (3秒)，超过此时间差不比价
REST_POLL_INTERVAL = 2.0   # 轮询模式下的休眠间隔 (秒)

class GlobalArbitrageMonitor:
    def __init__(self):
        self.exchanges = {}
        # 数据结构: { symbol: { exchange_id: {bid, ask, ts} } }
        self.price_data = defaultdict(dict)
        self.target_symbols = set() # 使用 Set 提高查找速度
        self.opp_count = 0

    async def init_exchanges(self):
        """初始化连接并计算共同交易对"""
        base_config = {
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot', 
                'warnOnFetchOpenOrdersWithoutSymbol': False
            },
            # 某些交易所全量订阅需要此配置以保持稳定性
            'newUpdates': False, 
            'timeout': 20000,
        }
        
        # 配置代理
        if PROXY_URL:
            base_config['aiohttp_proxy'] = PROXY_URL
            base_config['wsProxy'] = PROXY_URL
            base_config['proxies'] = {'http': PROXY_URL, 'https': PROXY_URL}

        logger.info(f"🔌 正在连接 {len(EXCHANGE_IDS)} 个交易所...")
        
        # 并行加载市场
        tasks = [self._load_exchange(eid, base_config) for eid in EXCHANGE_IDS]
        await asyncio.gather(*tasks)

        # --- 核心逻辑：筛选共同币种 ---
        # 统计每个币种在多少个交易所上线
        symbol_counter = defaultdict(int)
        valid_exchanges = 0
        
        for eid, ex in self.exchanges.items():
            if not ex.markets: continue
            valid_exchanges += 1
            for symbol in ex.markets:
                market = ex.markets[symbol]
                # 只监控现货且活跃的 USDT 交易对
                if market.get('spot') and symbol.endswith('/USDT') and market.get('active'):
                    symbol_counter[symbol] += 1
        
        # 只要在 >= 2 个交易所存在，就纳入监控
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
        """
        核心循环：使用 'Watch All' 模式避免参数过长报错
        """
        ex = self.exchanges[eid]
        
        # 计算该交易所实际上线了我们目标清单里的哪些币
        # 用于后续过滤数据
        relevant_symbols = {s for s in self.target_symbols if s in ex.markets}
        if not relevant_symbols:
            return

        # 默认为 False，尝试使用 WebSocket
        use_rest_mode = False
        
        # MEXC 已知不支持 Spot WebSocket 批量，直接切 REST
        if eid == 'mexc':
            use_rest_mode = True
            logger.info(f"ℹ️  [{eid}] 已知不支持 WS 批量，默认使用 REST 轮询")
        else:
            logger.info(f"📡 [{eid}] 启动全量监控 (Watch All) ...")

        while True:
            try:
                tickers = {}
                
                # --- 分支 A: REST 轮询模式 ---
                if use_rest_mode:
                    # 获取所有 tickers，避免 URL 过长
                    tickers = await ex.fetch_tickers() 
                    await asyncio.sleep(REST_POLL_INTERVAL)

                # --- 分支 B: WebSocket 实时模式 ---
                else:
                    try:
                        # 关键修改：不传参数！订阅全市场！
                        # 解决 Bybit args > 10 和 KuCoin URL invalid 问题
                        tickers = await ex.watch_tickers()
                        
                    except (ccxt.NotSupported, ccxt.ExchangeError, ccxt.BadRequest) as e:
                        # 如果全量订阅报错，降级到 REST
                        err_str = str(e).lower()
                        if "support" in err_str or "method" in err_str:
                            logger.warning(f"⚠️ [{eid}] WS 全量订阅不可用，降级为 REST: {e}")
                            use_rest_mode = True
                            continue
                        else:
                            raise e

                # --- 数据处理 ---
                self.process_data(eid, tickers, relevant_symbols)

            except Exception as e:
                err_msg = str(e)
                if "support" in err_msg.lower() and not use_rest_mode:
                     use_rest_mode = True
                else:
                    # 避免日志刷屏，只记录简短错误
                    logger.warning(f"⚠️ [{eid}] 连接抖动: {err_msg[:50]}... 5秒后重连")
                    await asyncio.sleep(5)

    def process_data(self, eid, tickers, relevant_symbols):
        """处理并过滤行情数据"""
        current_ts = time.time() * 1000
        
        for symbol, ticker in tickers.items():
            # ⚡ 极速过滤：只处理我们在乎的币种
            if symbol not in relevant_symbols: 
                continue 
            
            bid = ticker.get('bid')
            ask = ticker.get('ask')
            
            if not bid or not ask: continue

            # 更新内存数据
            self.price_data[symbol][eid] = {
                'bid': float(bid),
                'ask': float(ask),
                'ts': ticker.get('timestamp') or current_ts
            }
            
            # 触发比价逻辑
            self.calculate_arbitrage(symbol, trigger_ex=eid)

    def calculate_arbitrage(self, symbol, trigger_ex):
        """O(N) 复杂度的比价逻辑"""
        platforms = self.price_data.get(symbol)
        if not platforms or len(platforms) < 2: return

        trigger_data = platforms[trigger_ex]
        
        # 只对比 trigger_ex 和其他交易所
        for other_ex, other_data in platforms.items():
            if other_ex == trigger_ex: continue

            # 1. 时间窗口检查 (剔除陈旧数据)
            time_diff = abs(trigger_data['ts'] - other_data['ts'])
            if time_diff > MAX_TIME_DIFF_MS:
                continue

            # 2. 路径 A: Trigger 买 -> Other 卖
            self.check_profit(trigger_ex, other_ex, symbol, trigger_data['ask'], other_data['bid'])
            
            # 3. 路径 B: Other 买 -> Trigger 卖
            self.check_profit(other_ex, trigger_ex, symbol, other_data['ask'], trigger_data['bid'])

    def check_profit(self, buy_ex, sell_ex, symbol, buy_price, sell_price):
        if buy_price <= 0: return

        # 净利计算: (卖出所得 - 买入成本) / 买入成本
        # 卖出所得 = sell_price * (1 - fee)
        # 买入成本 = buy_price * (1 + fee)
        
        net_profit = (sell_price * (1 - TAKER_FEE_RATE) / (buy_price * (1 + TAKER_FEE_RATE)) - 1) * 100

        if net_profit > PROFIT_THRESHOLD:
            self.opp_count += 1
            # 简单计算一个毛利给日志看
            gross = (sell_price - buy_price) / buy_price * 100
            
            logger.info(
                f"🔥 [机会 #{self.opp_count}] {symbol} | "
                f"{buy_ex} -> {sell_ex}\n"
                f"   💰 净利: {net_profit:.2f}% (毛利 {gross:.2f}%)\n"
                f"   🟢 买: {buy_price:<10} 🔴 卖: {sell_price:<10} (差价: {(sell_price-buy_price):.4f})"
            )

    async def run(self):
        if not await self.init_exchanges(): return

        tasks = [self.watch_tickers_loop(eid) for eid in self.exchanges.keys()]
        
        logger.info("🚀 监控引擎启动... (按 Ctrl+C 停止)")
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"主程序错误: {e}")
        finally:
            await self.shutdown()

    async def shutdown(self):
        logger.info("正在关闭连接...")
        for name, ex in self.exchanges.items():
            await ex.close()
        logger.info("👋 再见")

if __name__ == "__main__":
    bot = GlobalArbitrageMonitor()
    try:
        # Windows Python 3.8+ 兼容性设置
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass