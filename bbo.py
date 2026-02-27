import asyncio
import json
import time
import ssl
import certifi
import websockets
from dataclasses import dataclass

@dataclass
class BBO:
    exchange: str
    symbol: str
    bid_p: float
    bid_sz: float
    ask_p: float
    ask_sz: float

# 全局内存缓存
orderbooks = {}

# --- 配置代理与 SSL ---
PROXY_URL = "http://127.0.0.1:7890" 
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE 

# --- 各交易所解析逻辑 ---

def parse_binance(data):
    if 'b' in data:
        orderbooks["Binance"] = BBO("Binance", "BTCUSDT", float(data['b']), float(data['B']), float(data['a']), float(data['A']))

def parse_okx(data):
    if "data" in data:
        d = data['data'][0]
        orderbooks["OKX"] = BBO("OKX", "BTC-USDT", float(d['bids'][0][0]), float(d['bids'][0][1]), float(d['asks'][0][0]), float(d['asks'][0][1]))

def parse_bybit(data):
    if "data" in data:
        d = data['data']
        orderbooks["Bybit"] = BBO("Bybit", "BTCUSDT", float(d['b'][0][0]), float(d['b'][0][1]), float(d['a'][0][0]), float(d['a'][0][1]))

def parse_bitget(data):
    if "data" in data:
        d = data['data'][0]
        orderbooks["Bitget"] = BBO("Bitget", "BTCUSDT", float(d['bids'][0][0]), float(d['bids'][0][1]), float(d['asks'][0][0]), float(d['asks'][0][1]))

def parse_gate(data):
    if data.get("event") == "update":
        d = data['result']
        orderbooks["Gate.io"] = BBO("Gate.io", "BTC_USDT", float(d['b']), float(d['B']), float(d['a']), float(d['A']))

def parse_bitmart(data):
    # 根据你提供的原始消息实时修正：bid_px, bid_sz, ask_px, ask_sz
    if "data" in data and isinstance(data['data'], list):
        for item in data['data']:
            if 'bid_px' in item:
                orderbooks["BitMart"] = BBO(
                    "BitMart", 
                    item.get("symbol", "BTC_USDT"), 
                    float(item['bid_px']), 
                    float(item['bid_sz']),
                    float(item['ask_px']), 
                    float(item['ask_sz'])
                )

# --- 通用订阅逻辑 ---

async def safe_subscribe(exchange, url, sub_msg, parser_func):
    while True:
        try:
            async with websockets.connect(
                url, ssl=ssl_context, proxy=PROXY_URL,
                ping_interval=20, ping_timeout=10
            ) as ws:
                if sub_msg:
                    await ws.send(json.dumps(sub_msg))
                async for msg in ws:
                    data = json.loads(msg)
                    parser_func(data)
        except Exception:
            await asyncio.sleep(5)

# --- 监控打印逻辑 ---

async def display_monitor():
    print("正在聚合各交易所数据...")
    while True:
        current_books = list(orderbooks.values())
        if not current_books:
            await asyncio.sleep(1)
            continue
            
        print(f"\n[{time.strftime('%H:%M:%S')}] 现货 BBO 汇总:")
        print(f"{'交易所':<10} | {'买一价':<10} | {'买一量':<10} | {'卖一价':<10} | {'卖一量':<10}")
        print("-" * 75)
        
        for b in sorted(current_books, key=lambda x: x.exchange):
            print(f"{b.exchange:<10} | {b.bid_p:<10.2f} | {b.bid_sz:<10.4f} | {b.ask_p:<10.2f} | {b.ask_sz:<10.4f}")
        
        await asyncio.sleep(1)

# --- 主入口 ---

async def main():
    tasks = [
        safe_subscribe("Binance", "wss://stream.binance.com:9443/ws/btcusdt@bookTicker", None, parse_binance),
        safe_subscribe("OKX", "wss://ws.okx.com:8443/ws/v5/public", {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT"}]}, parse_okx),
        safe_subscribe("Bybit", "wss://stream.bybit.com/v5/public/spot", {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}, parse_bybit),
        safe_subscribe("Bitget", "wss://ws.bitget.com/v2/ws/public", {"op": "subscribe", "args": [{"instType": "SPOT", "channel": "books1", "instId": "BTCUSDT"}]}, parse_bitget),
        safe_subscribe("Gate", "wss://api.gateio.ws/ws/v4/", {"channel": "spot.book_ticker", "event": "subscribe", "payload": ["BTC_USDT"]}, parse_gate),
        safe_subscribe("BitMart", "wss://ws-manager-compress.bitmart.com/api?protocol=1.1", {"op": "subscribe", "args": ["spot/ticker:BTC_USDT"]}, parse_bitmart),
        display_monitor()
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已退出")