import asyncio
import ccxt.pro as ccxtpro

async def test_proxy():
    # 填入你本机梯子的真实端口！Clash默认7890，v2rayN默认10809
    PROXY_URL = 'http://127.0.0.1:7890' 
    
    # 针对 Python 异步库的最强代理配置写法
    exchange_config = {
        'timeout': 15000,
        'proxies': {'http': PROXY_URL, 'https': PROXY_URL},
        'aiohttp_proxy': PROXY_URL, 
    }
    
    print(f"🚀 正在使用代理 {PROXY_URL} 测试连接 OKX...")
    okx = ccxtpro.okx(exchange_config)
    
    try:
        markets = await okx.load_markets()
        print(f"✅ 代理配置完全正确！成功连接 OKX，获取到 {len(markets)} 个交易对。")
    except Exception as e:
        print(f"❌ 连接彻底失败，报错信息: {e}")
        print("\n👇 请严格检查以下3点：")
        print("1. 你的代理软件到底是不是 7890 端口？")
        print("2. 你的代理软件是不是开着【全局模式/Global】或【TUN虚拟网卡】？（国内连OKX必须走全局或手动配规则）")
        print("3. 把 PROXY_URL 换成 'http://127.0.0.1:你的真实端口' 再试。")
    finally:
        await okx.close()
        await asyncio.sleep(0.5) # 彻底解决 Unclosed client session 警告

if __name__ == "__main__":
    asyncio.run(test_proxy())