import asyncio
import ccxt.pro as ccxtpro
import ccxt
import time
import logging
import threading
from collections import defaultdict, deque
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

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

# --- 2. 核心配置区 (保持原逻辑) ---
EXCHANGE_IDS = ['binance', 'okx', 'bybit', 'gate', 'kucoin', 'mexc', 'bitget', 'bitmart', 'htx']
PROXY_URL = 'http://127.0.0.1:7890'  # 根据实际调整
PROFIT_THRESHOLD = 0.8
MAX_TIME_DIFF_MS = 3000
REST_POLL_INTERVAL = 2.0
FEE_RATES = {
    'binance': 0.0010, 'okx': 0.0010, 'bybit': 0.0010,
    'gate': 0.0020, 'kucoin': 0.0020, 'mexc': 0.0020,
    'bitget': 0.0020, 'htx': 0.0020, 'bitmart': 0.0025,
    'default': 0.0020
}
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
        
        logger.info(f"🔌 正在初始化 {len(EXCHANGE_IDS)} 个交易所...")
        tasks = [self._load_exchange(eid, base_config) for eid in EXCHANGE_IDS]
        await asyncio.gather(*tasks)

        symbol_counter = defaultdict(int)
        for eid, ex in self.exchanges.items():
            if not ex.markets: continue
            for symbol in ex.markets:
                market = ex.markets[symbol]
                if market.get('spot') and symbol.endswith('/USDT') and market.get('active'):
                    symbol_counter[symbol] += 1
        self.target_symbols = {s for s, c in symbol_counter.items() if c >= 2}
        logger.info(f"✅ 初始化完成 | 监控币种: {len(self.target_symbols)}")
        return True if self.target_symbols else False

    async def _load_exchange(self, eid, config):
        try:
            ex_class = getattr(ccxtpro, eid)
            exchange = ex_class(config)
            await exchange.load_markets()
            self.exchanges[eid] = exchange
            logger.info(f"🟢 {eid} 已就绪")
        except Exception as e:
            logger.error(f"🔴 {eid} 失败: {e}")

    async def watch_tickers_loop(self, eid):
        ex = self.exchanges[eid]
        relevant_symbols = {s for s in self.target_symbols if s in ex.markets}
        if not relevant_symbols: return
        use_rest = (eid == 'mexc')
        while True:
            try:
                tickers = await ex.fetch_tickers() if use_rest else await ex.watch_tickers()
                if use_rest: await asyncio.sleep(REST_POLL_INTERVAL)
                self.process_data(eid, tickers, relevant_symbols)
            except Exception:
                await asyncio.sleep(5)

    def process_data(self, eid, tickers, relevant_symbols):
        now_ts = time.time() * 1000
        for symbol, t in tickers.items():
            if symbol not in relevant_symbols or not t.get('bid') or not t.get('ask'): continue
            bid = float(t['bid'])
            ask = float(t['ask'])
            if bid <= 0 or ask <= 0 or bid > ask:  # 新增：bid/ask异常检查
                logger.warning(f"⚠️ 异常价格: {symbol}@{eid} | bid {bid}, ask {ask}")
                continue
            self.price_data[symbol][eid] = {
                'bid': bid, 'ask': ask,
                'bidVol': float(t.get('bidVolume', 0) or 0),
                'askVol': float(t.get('askVolume', 0) or 0),
                'ts': t.get('timestamp') or now_ts,
            }
            self.calculate_arbitrage(symbol, eid)

    def calculate_arbitrage(self, symbol, trigger_ex):
        data = self.price_data[symbol]
        t_data = data[trigger_ex]
        for o_ex, o_data in data.items():
            if o_ex == trigger_ex or abs(t_data['ts'] - o_data['ts']) > MAX_TIME_DIFF_MS: continue
            self.check_profit(trigger_ex, o_ex, symbol, t_data['ask'], o_data['bid'])
            self.check_profit(o_ex, trigger_ex, symbol, o_data['ask'], t_data['bid'])

    def check_profit(self, buy_ex, sell_ex, symbol, b_price, s_price):
        if b_price <= 0 or s_price <= b_price: return
        
        # 新增：过滤异常价格比率
        price_ratio = s_price / b_price
        if price_ratio > MAX_PRICE_RATIO or price_ratio < 1 / MAX_PRICE_RATIO:
            ##logger.warning(f"⚠️ 异常价格比率: {symbol} | {buy_ex}买价 {b_price}, {sell_ex}卖价 {s_price} | 比率 {price_ratio:.2f}")
            return

        # 计算深度
        b_depth = self.price_data[symbol][buy_ex].get('askVol', 0) * b_price
        s_depth = self.price_data[symbol][sell_ex].get('bidVol', 0) * s_price
        if b_depth < MIN_DEPTH_USDT or s_depth < MIN_DEPTH_USDT: return

        # 净利计算
        b_fee, s_fee = FEE_RATES.get(buy_ex, 0.002), FEE_RATES.get(sell_ex, 0.002)
        net_profit = (s_price * (1 - s_fee) / (b_price * (1 + b_fee)) - 1) * 100

        # 新增：过滤异常高利润（上限50%）
        if net_profit > 50.0:
            ##logger.warning(f"⚠️ 异常高利润: {symbol} | 利润 {net_profit:.2f}% | 可能数据错误")
            return

        if net_profit > PROFIT_THRESHOLD:
            key = (buy_ex, sell_ex)
            if time.time() - self.last_alert_time[symbol][key] < ALERT_COOLDOWN: return
            self.last_alert_time[symbol][key] = time.time()
            self.opp_count += 1
            self.recent_opportunities.append({
                'id': self.opp_count, 'time': time.strftime('%H:%M:%S'), 'symbol': symbol,
                'buy_ex': buy_ex, 'sell_ex': sell_ex, 'profit': round(net_profit, 2),
                'b_price': b_price, 's_price': s_price, 'b_vol': round(b_depth, 1), 's_vol': round(s_depth, 1)
            })

# --- FastAPI 接口 ---
bot = GlobalArbitrageMonitor()
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/data")
async def get_data():
    return {
        "status": {"total": bot.opp_count, "ex": len(bot.exchanges), "sym": len(bot.target_symbols)},
        "list": list(bot.recent_opportunities)[::-1]
    }

async def main():
    if await bot.init_exchanges():
        threading.Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error"), daemon=True).start()
        await asyncio.gather(*[bot.watch_tickers_loop(eid) for eid in bot.exchanges])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass