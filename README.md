# FundingCarry — Public Portfolio Edition

> 跨交易所资金费率研究与对冲交易系统：从历史资金费率比较、机会筛选，到双腿交易预检、持仓核对和风险监控。

## 项目简介

FundingCarry 是一个围绕永续合约资金费率差构建的个人项目。系统采集多个交易所的历史资金费率，按实际结算记录比较不同周期的累计费率，并辅助分析跨所多空对冲机会；V4 使用 Gate CrossEx 接口，展示双腿交易预检、持仓监控和资金费收支分析等流程。

项目作者已经在真实市场运行这一策略，并经历了熊市和震荡市场的检验；**牛市环境下的表现仍有待检验**。这是作者的实盘运行经历，不代表经过独立审计的业绩，也不保证未来收益。资金费率会变化，交易手续费、滑点、价差和保证金风险都可能影响实际结果。

本仓库是经脱敏整理的作品集版本：它展示数据处理与交易系统的主要设计，但账户写入和实盘下单已禁用，演示参数也不代表作者当前的实盘配置。

## Overview

FundingCarry is a personal research and trading-systems project for exploring
funding-rate differences across perpetual-futures venues. It combines public
historical data collection and comparison with a Gate CrossEx cockpit that
demonstrates two-leg preflight checks, position monitoring and funding-ledger
analysis.

The project author has operated this strategy in live markets through bearish
and range-bound conditions. Its behavior in a bull market remains untested.
This is a statement of the author's experience, not independently audited
performance or a guarantee of future returns.

This repository is a sanitized, non-production portfolio edition of two
independent components: a public-market funding-rate collector/comparator and
a Gate CrossEx two-leg execution cockpit demo.

## Components

### `funding_scanner/`

- Collects public perpetual-market catalogs, funding history, and liquidity
  metadata from Aster, Binance, Bybit, Deribit, Gate, Hyperliquid, Kraken,
  Lighter, and OKX.
- Compares cumulative 7-, 14-, and 30-day funding rates across venues.
- Keeps exchange-specific pagination and rate-unit handling in each adapter.
- Marks incomplete histories as unavailable instead of treating missing data
  as a zero rate.
- Uses a small generic sample universe by default. Set
  `FUNDING_SCANNER_ASSETS` to change it.

Run the local read-only scanner page and API (the service refreshes in the
background every 30 minutes by default):

```bash
FUNDING_SCANNER_ASSETS=BTC,ETH,SOL python3 funding_scanner/service.py
```

Open `http://127.0.0.1:8765/`. The collector makes public market-data requests
only; it does not require exchange API credentials or place orders. For a
single JSON snapshot, run `python3 funding_scanner/collector.py` directly.

### `v4_demo/`

- Demonstrates a Gate CrossEx-backed two-leg cockpit, preflight validation,
  account/position normalization, execution reconciliation, and paired-guard
  state handling.
- Binds to `127.0.0.1` by default.
- Uses generic demonstration risk values rather than personal production
  thresholds or leverage preferences.
- Searches only explicitly supplied process credentials or the local `.env`;
  it does not search the production server's credential paths.
- Is permanently read-only: order submission, leverage changes, and guard
  mutations are hard-disabled in this portfolio copy.

**This public copy is for code review and simulation, not live trading.** It has
not received an independent security or exchange-behavior audit. Do not add
production credentials or expose the service to the public internet.
Cross-exchange execution is non-atomic and can leave residual exposure.

After starting the scanner service, open a second terminal and run the server
from `v4_demo/`:

```bash
cd v4_demo
python3 fundingcarryv4_server.py
```

Then open `http://127.0.0.1:8794/`. Without credentials, private account
widgets remain unavailable; the opportunity scan reads the public local
collector at `127.0.0.1:8765`.

## Tests

All tests use synthetic fixtures and do not call exchanges:

```bash
python3 -m unittest discover -s tests -v
```

## Before publishing

- Review the complete staged diff and the repository history for secrets.
- Keep `.env`, runtime state, logs, keys, account data, and deployment files out
  of Git.
- Do not include personal watchlists, real positions, IP addresses, private
  domains, server names, or account-specific risk parameters.
- Select a license only after reviewing third-party code and dependencies.

This portfolio copy intentionally contains no production deployment unit,
server address, API key, position snapshot, or strategy-account whitelist.
