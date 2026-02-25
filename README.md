

# 🚀 banzhuan: 跨交易所现货套利实时监控系统

**banzhuan** 是一款基于 Python 开发的高性能、多交易所现货套利监控工具。它能够实时捕捉 9+ 主流交易所（Binance, OKX, Bybit 等）的盘口价格波动，计算扣除手续费后的**真实净利润**，并提供直观的 Web 可视化面板。

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg)
![CCXT](https://img.shields.io/badge/library-CCXT%20Pro-orange.svg)

---

## ✨ 核心特性

-   **⚡ 实时引擎**：利用 `CCXT Pro` 的 WebSocket 技术，毫秒级同步各交易所 L1 盘口数据（Bid/Ask）。
-   **📊 真实收益计算**：
    -   自动扣除买入/卖出双边交易手续费。
    -   支持自定义各交易所的 VIP 手续费率。
-   **🛡️ 智能过滤机制**：
    -   **流动性过滤**：自动忽略 24h 成交量过低（僵尸币）或深度不足的项目。
    -   **时间同步**：严格对比不同交易所的时间戳，确保不因延迟产生“虚假价差”。
    -   **黑名单**：自动屏蔽杠杆代币（UP/DOWN/BEAR/BULL）等高风险资产。
-   **🌐 Web 监控面板**：内置基于 FastAPI 和 Tailwind CSS 的响应式仪表盘，支持手机/PC 实时查看最新机会。
-   **📝 自动化日志**：本地保存详细的套利日志，支持按天轮转，方便复盘分析。

---

## 🛠️ 技术栈

-   **核心框架**: [CCXT Pro](https://github.com/ccxt/ccxt) (异步多交易所交易库)
-   **并发模型**: Python `asyncio`
-   **Web 服务**: FastAPI + Uvicorn
-   **前端 UI**: Tailwind CSS (CDN 加载)
-   **日志管理**: Python `logging` + `TimedRotatingFileHandler`

---

## 📦 安装与启动

### 1. 克隆仓库
```bash
git clone https://github.com/redgreylee/banzhuan
cd banzhuan
```

### 2. 环境配置
建议使用 Python 3.9+ 环境：
```bash
pip install ccxt fastapi uvicorn aiohttp
```
*注：由于使用了 WebSocket 全量数据，建议在海外 VPS（如东京、新加坡、香港）运行以获取最佳延迟。*

### 3. 运行程序
```bash
python main.py
```

### 4. 访问面板
打开浏览器访问：`http://localhost:8000`

---

## ⚙️ 关键配置说明

你可以在 `main.py` 的 `核心配置区` 修改以下参数以适应你的交易策略：

| 配置项 | 说明 | 推荐值 |
| :--- | :--- | :--- |
| `PROFIT_THRESHOLD` | 触发报警的净利润阈值（百分比） | `1.2` (1.2%) |
| `MIN_QUOTE_VOLUME` | 24h 成交量阈值（低于此值不监控） | `100000` USDT |
| `MIN_DEPTH_USDT` | 盘口最小可成交金额（防止深度不足） | `200` USDT |
| `MAX_TIME_DIFF_MS` | 允许的两所价格时间差（毫秒） | `2000` |
| `FEE_RATES` | 各交易所手续费率（根据你的等级调整） | `0.001` (0.1%) |

---

## 🖥️ 界面预览

-   **状态栏**：实时显示各交易所连接状态及心跳延迟。
-   **机会列表**：详细展示 `交易对 | 路径 | 净利润 | 价格明细 | 容纳深度`。
-   **实时刷新**：面板每 5 秒自动更新，捕捉最新瞬时机会。

---

## ⚠️ 风险提示与声明

1.  **滑点风险**：实际执行套利时，盘口深度可能瞬间发生变化。
2.  **提币成本**：本系统主要监控现货价格差，未将链上提币手续费（Withdrawal Fee）实时计入，实际操作前需手动核算。
3.  **网络延迟**：虽然使用了 WebSocket，但各交易所到服务器的物理距离会导致数据延迟。
4.  **免责声明**：本工具仅供学习和市场研究使用，不构成任何投资建议。因使用本工具导致的任何资金损失，开发者概不负责。

---

## 🤝 贡献与反馈

欢迎提交 Issue 或 Pull Request 来改进算法。如果你觉得这个项目对你有帮助，请给一个 ⭐️ **Star**！

---

## 📜 许可证

[MIT License](LICENSE)