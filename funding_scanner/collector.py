
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from itertools import combinations
import os
from urllib.parse import urlencode

from market_data_http import async_request_json

BINANCE_FAPI = "https://fapi.binance.com"
ASTER_FAPI = "https://fapi.asterdex.com"
LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"
GATE_BASE = "https://api.gateio.ws"
BYBIT_BASE = "https://api.bybit.com"
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"
KRAKEN_FUTURES_BASE = "https://futures.kraken.com/derivatives/api/v3"
OKX_BASE = "https://www.okx.com"
PERIODS = (7, 14, 30)
PLATFORMS = ("Aster", "Binance", "Bybit", "Deribit", "Gate", "Hyperliquid", "Kraken", "Lighter", "OKX")
HISTORY_SEMAPHORE = asyncio.Semaphore(12)
HYPERLIQUID_HISTORY_SEMAPHORE = asyncio.Semaphore(1)
HYPERLIQUID_ORDER_BOOK_SEMAPHORE = asyncio.Semaphore(1)
LIGHTER_HISTORY_SEMAPHORE = asyncio.Semaphore(1)
LIGHTER_HISTORY_RETRIES = 4
LIGHTER_HISTORY_REQUEST_INTERVAL_SECONDS = 0.8

ASSET_UNIVERSE = [item.strip().upper() for item in os.getenv("FUNDING_SCANNER_ASSETS", "BTC,ETH,SOL,ADA").split(",") if item.strip()]
MARKETS = [{"symbol": symbol} for symbol in ASSET_UNIVERSE]


def millis(dt):
    return int(dt.timestamp() * 1000)


def candidates(item, venue):
    return item.get(venue, [item["symbol"]])


def sum_period(entries, cutoff_ms):
    return sum(rate for ts, rate in entries if ts >= cutoff_ms)


async def load_binance_symbols():
    exchange_info, ticker_rows = await asyncio.gather(
        async_request_json(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=20),
        async_request_json(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", timeout=20),
    )
    symbols = {row["symbol"] for row in exchange_info.get("symbols", []) if row.get("status") == "TRADING"}
    ticker_map = {row.get("symbol"): row for row in ticker_rows if row.get("symbol")}
    result = {}
    for item in MARKETS:
        for base in candidates(item, "binance"):
            if base.endswith("USDT") or base.endswith("USDC"):
                if base in symbols:
                    ticker = ticker_map.get(base, {})
                    result[item["symbol"]] = {
                        "symbol": base,
                        "day_volume": float(ticker.get("quoteVolume", 0) or 0),
                    }
                    break
                continue
            for quote in ("USDC", "USDT"):
                symbol = f"{base}{quote}"
                if symbol in symbols:
                    ticker = ticker_map.get(symbol, {})
                    result[item["symbol"]] = {
                        "symbol": symbol,
                        "day_volume": float(ticker.get("quoteVolume", 0) or 0),
                    }
                    break
            if item["symbol"] in result:
                break
    return result


async def load_aster_symbols():
    exchange_info, ticker_rows = await asyncio.gather(
        async_request_json(f"{ASTER_FAPI}/fapi/v1/exchangeInfo", timeout=20),
        async_request_json(f"{ASTER_FAPI}/fapi/v1/ticker/24hr", timeout=20),
    )
    symbols = {row["symbol"] for row in exchange_info.get("symbols", []) if row.get("status") == "TRADING"}
    ticker_map = {row.get("symbol"): row for row in ticker_rows if row.get("symbol")}
    result = {}
    for item in MARKETS:
        for base in candidates(item, "aster"):
            symbol = base if base.endswith(("USDT", "USDC")) else f"{base}USDT"
            if symbol in symbols:
                result[item["symbol"]] = {
                    "symbol": symbol,
                    "day_volume": float(ticker_map.get(symbol, {}).get("quoteVolume", 0) or 0),
                }
                break
    return result


async def load_lighter_symbols():
    details = await async_request_json(f"{LIGHTER_BASE}/api/v1/orderBookDetails", timeout=20)
    mapping = {
        item.get("symbol"): item
        for item in details.get("order_book_details", [])
        if item.get("symbol")
    }
    result = {}
    for item in MARKETS:
        for symbol in candidates(item, "lighter"):
            if symbol in mapping:
                detail = mapping[symbol]
                result[item["symbol"]] = {
                    "symbol": symbol,
                    "market_id": int(detail["market_id"]),
                    "day_volume": float(detail.get("daily_quote_token_volume", 0) or 0),
                }
                break
    return result


async def load_hyperliquid_symbols():
    primary_payload, dex_rows = await asyncio.gather(
        async_request_json(
            HYPERLIQUID_INFO,
            method="POST",
            body={"type": "metaAndAssetCtxs"},
            timeout=20,
        ),
        async_request_json(
            HYPERLIQUID_INFO,
            method="POST",
            body={"type": "perpDexs"},
            timeout=20,
        ),
    )
    dex_names = [row.get("name") for row in dex_rows if isinstance(row, dict) and row.get("name")]
    dex_payloads = await asyncio.gather(
        *[
            async_request_json(
                HYPERLIQUID_INFO,
                method="POST",
                body={"type": "metaAndAssetCtxs", "dex": dex_name},
                timeout=20,
            )
            for dex_name in dex_names
        ],
        return_exceptions=True,
    )
    payloads = [(None, primary_payload)] + [
        (dex_name, payload)
        for dex_name, payload in zip(dex_names, dex_payloads)
        if not isinstance(payload, Exception)
    ]
    universe = {}
    for dex_name, payload in payloads:
        if not isinstance(payload, list) or len(payload) < 2:
            continue
        for meta, ctx in zip(payload[0].get("universe", []), payload[1]):
            name = meta.get("name")
            if not name:
                continue
            # HIP-3 uses a dex-prefixed coin name for funding history and books.
            coin = name if ":" in name else (f"{dex_name}:{name}" if dex_name else name)
            universe.setdefault(name, {
                "symbol": coin,
                "day_volume": float(ctx.get("dayNtlVlm", 0) or 0),
            })
    result = {}
    for item in MARKETS:
        for symbol in candidates(item, "hyperliquid"):
            if symbol in universe:
                result[item["symbol"]] = universe[symbol]
                break
    return result


async def load_gate_symbols():
    """Discover active Gate USDT perpetuals and their 24h quote volume."""
    contracts_payload, tickers_payload = await asyncio.gather(
        async_request_json(
            f"{GATE_BASE}/api/v4/futures/usdt/contracts",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
        async_request_json(
            f"{GATE_BASE}/api/v4/futures/usdt/tickers",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
    )
    ticker_map = {
        str(row.get("contract") or "").upper(): row
        for row in (tickers_payload if isinstance(tickers_payload, list) else [])
        if row.get("contract")
    }
    mapping = {}
    for row in contracts_payload if isinstance(contracts_payload, list) else []:
        contract = str(row.get("name") or "").upper()
        if not contract.endswith("_USDT") or str(row.get("status") or "trading").lower() != "trading":
            continue
        base = contract.removesuffix("_USDT")
        ticker = ticker_map.get(contract, {})
        volume = ticker.get("volume_24h_quote") or ticker.get("volume_24h_settle") or 0
        mapping[base] = {
            "symbol": contract,
            "day_volume": float(volume or 0),
        }
    result = {}
    for item in MARKETS:
        for base in candidates(item, "gate"):
            name = str(base).upper().removesuffix("USDT").removesuffix("USDC")
            if name in mapping:
                result[item["symbol"]] = mapping[name]
                break
    return result


async def load_bybit_symbols():
    """Discover active linear USDT/USDC perpetuals and 24h turnover."""
    instruments_payload, tickers_payload = await asyncio.gather(
        async_request_json(
            f"{BYBIT_BASE}/v5/market/instruments-info?category=linear&limit=1000",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
        async_request_json(
            f"{BYBIT_BASE}/v5/market/tickers?category=linear",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
    )
    if instruments_payload.get("retCode") != 0 or tickers_payload.get("retCode") != 0:
        raise RuntimeError("Bybit returned an unsuccessful public market response")
    ticker_map = {
        row.get("symbol"): row
        for row in (tickers_payload.get("result") or {}).get("list", [])
        if row.get("symbol")
    }
    mapping = {}
    for row in (instruments_payload.get("result") or {}).get("list", []):
        symbol = str(row.get("symbol") or "")
        if row.get("status") != "Trading":
            continue
        quote = str(row.get("quoteCoin") or "").upper()
        if quote not in {"USDT", "USDC"} or not symbol.endswith(quote):
            continue
        base = symbol.removesuffix(quote).upper()
        quote_rank = 0 if quote == "USDC" else 1
        existing = mapping.get(base)
        if existing and existing["quote_rank"] <= quote_rank:
            continue
        mapping[base] = {
            "symbol": symbol,
            "quote_rank": quote_rank,
            "day_volume": float(ticker_map.get(symbol, {}).get("turnover24h", 0) or 0),
        }
    result = {}
    for item in MARKETS:
        for base in candidates(item, "bybit"):
            name = str(base).upper().removesuffix("USDT").removesuffix("USDC")
            if name in mapping:
                result[item["symbol"]] = mapping[name]
                break
    return result


async def load_okx_symbols():
    """Discover active OKX linear USDT/USDC perpetual swaps."""
    instruments_payload, tickers_payload = await asyncio.gather(
        async_request_json(
            f"{OKX_BASE}/api/v5/public/instruments?instType=SWAP",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
        async_request_json(
            f"{OKX_BASE}/api/v5/market/tickers?instType=SWAP",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
    )
    if instruments_payload.get("code") != "0" or tickers_payload.get("code") != "0":
        raise RuntimeError("OKX returned an unsuccessful public market response")
    ticker_map = {
        row.get("instId"): row
        for row in tickers_payload.get("data", [])
        if row.get("instId")
    }
    mapping = {}
    for row in instruments_payload.get("data", []):
        inst_id = str(row.get("instId") or "")
        if row.get("state") != "live":
            continue
        if inst_id.endswith("-USDC-SWAP"):
            base, quote_rank = inst_id.removesuffix("-USDC-SWAP"), 0
        elif inst_id.endswith("-USDT-SWAP"):
            base, quote_rank = inst_id.removesuffix("-USDT-SWAP"), 1
        else:
            continue
        existing = mapping.get(base.upper())
        if existing and existing["quote_rank"] <= quote_rank:
            continue
        ticker = ticker_map.get(inst_id, {})
        mapping[base.upper()] = {
            "symbol": inst_id,
            "quote_rank": quote_rank,
            "day_volume": float(ticker.get("volCcy24h", 0) or 0),
        }
    result = {}
    for item in MARKETS:
        for base in candidates(item, "okx"):
            name = str(base).upper().removesuffix("USDT").removesuffix("USDC")
            if name in mapping:
                result[item["symbol"]] = mapping[name]
                break
    return result


async def load_deribit_symbols():
    """Discover open Deribit perpetuals and their USD 24h volume."""
    payload = await async_request_json(
        f"{DERIBIT_BASE}/get_instruments?{urlencode({'currency': 'any', 'kind': 'future'})}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=30,
    )
    instruments = payload.get("result", []) if isinstance(payload, dict) else []
    wanted_bases = {
        str(base).upper().removesuffix("USDT").removesuffix("USDC")
        for item in MARKETS
        for base in candidates(item, "deribit")
    }
    selected = {}
    for row in instruments:
        base = str(row.get("base_currency") or "").upper()
        instrument = row.get("instrument_name")
        if (
            not instrument
            or base not in wanted_bases
            or row.get("kind") != "future"
            or row.get("settlement_period") != "perpetual"
            or row.get("is_active") is not True
            or row.get("state") != "open"
        ):
            continue
        settlement = str(row.get("settlement_currency") or "").upper()
        selected[base] = {"symbol": instrument, "settlement_currency": settlement}
    currencies = sorted({row["settlement_currency"] for row in selected.values() if row["settlement_currency"]})

    async def summaries(currency):
        query = urlencode({"currency": currency, "kind": "future"})
        result = await async_request_json(
            f"{DERIBIT_BASE}/get_book_summary_by_currency?{query}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        )
        if isinstance(result, dict) and result.get("error"):
            raise RuntimeError(f"Deribit summary error for {currency}")
        return result.get("result", []) if isinstance(result, dict) else []

    summary_rows = await asyncio.gather(*(summaries(currency) for currency in currencies)) if currencies else []
    volume_by_instrument = {
        row.get("instrument_name"): float(row.get("volume_usd") or 0)
        for rows in summary_rows
        for row in rows
        if row.get("instrument_name")
    }
    result = {}
    for item in MARKETS:
        for base in candidates(item, "deribit"):
            name = str(base).upper().removesuffix("USDT").removesuffix("USDC")
            row = selected.get(name)
            if row:
                result[item["symbol"]] = {
                    "symbol": row["symbol"],
                    "day_volume": volume_by_instrument.get(row["symbol"], 0),
                }
                break
    return result


async def load_kraken_symbols():
    """Discover tradeable Kraken linear perpetuals and USD 24h turnover."""
    instruments_payload, tickers_payload = await asyncio.gather(
        async_request_json(
            f"{KRAKEN_FUTURES_BASE}/instruments",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
        async_request_json(
            f"{KRAKEN_FUTURES_BASE}/tickers",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        ),
    )
    if instruments_payload.get("result") != "success" or tickers_payload.get("result") != "success":
        raise RuntimeError("Kraken Futures returned an unsuccessful public market response")
    ticker_map = {
        str(row.get("symbol") or "").upper(): row
        for row in tickers_payload.get("tickers", [])
        if row.get("symbol")
    }
    mapping = {}
    for row in instruments_payload.get("instruments", []):
        symbol = str(row.get("symbol") or "").upper()
        if row.get("tradeable") is not True or row.get("type") != "flexible_futures" or not symbol.startswith("PF_"):
            continue
        base = str(row.get("base") or "").upper()
        base = "BTC" if base == "XBT" else base
        ticker = ticker_map.get(symbol, {})
        if ticker.get("tag") != "perpetual" or ticker.get("suspended") is True:
            continue
        volume = float(ticker.get("volumeQuote", 0) or 0)
        existing = mapping.get(base)
        if existing is None or volume > existing["day_volume"]:
            mapping[base] = {"symbol": symbol, "day_volume": volume}
    result = {}
    for item in MARKETS:
        for base in candidates(item, "kraken"):
            name = str(base).upper().removesuffix("USDT").removesuffix("USDC")
            name = "BTC" if name == "XBT" else name
            if name in mapping:
                result[item["symbol"]] = mapping[name]
                break
    return result


async def binance_history(symbol, start_ms, end_ms):
    query = urlencode(
        {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        }
    )
    rows = await async_request_json(f"{BINANCE_FAPI}/fapi/v1/fundingRate?{query}", timeout=20)
    return [
        (int(row.get("fundingTime", 0) or 0), float(row.get("fundingRate", 0) or 0))
        for row in rows
    ]


async def aster_history(symbol, start_ms, end_ms):
    query = urlencode({"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000})
    rows = await async_request_json(f"{ASTER_FAPI}/fapi/v1/fundingRate?{query}", timeout=20)
    return [
        (int(row.get("fundingTime", 0) or 0), float(row.get("fundingRate", 0) or 0))
        for row in rows
    ]


async def lighter_history(market_id, start_ms, end_ms):
    async def fetch_payload(query):
        last_error = None
        for attempt in range(LIGHTER_HISTORY_RETRIES):
            try:
                async with LIGHTER_HISTORY_SEMAPHORE:
                    await asyncio.sleep(LIGHTER_HISTORY_REQUEST_INTERVAL_SECONDS)
                    payload = await async_request_json(
                        f"{LIGHTER_BASE}/api/v1/fundings?{query}",
                        headers={"Origin": "https://app.lighter.xyz", "Referer": "https://app.lighter.xyz/"},
                        timeout=20,
                    )
                if not isinstance(payload.get("fundings"), list) or not payload["fundings"]:
                    raise RuntimeError("Lighter returned an empty funding history")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt + 1 < LIGHTER_HISTORY_RETRIES:
                    await asyncio.sleep(min(30.0, 2.0 ** (attempt + 1)))
        raise last_error

    query = urlencode(
        {
            "market_id": market_id,
            "resolution": "1h",
            "start_timestamp": start_ms,
            "end_timestamp": end_ms,
            "count_back": 1000,
        }
    )
    payload = await fetch_payload(query)
    entries = []
    for item in payload.get("fundings", []):
        timestamp = int(item.get("timestamp", 0) or 0)
        timestamp_ms = timestamp * 1000 if timestamp < 10_000_000_000 else timestamp
        sign = -1 if item.get("direction") == "short" else 1
        entries.append((timestamp_ms, sign * float(item.get("rate", 0) or 0) / 100))
    return entries


async def hyperliquid_history(symbol, start_ms, end_ms):
    # Hyperliquid's public history endpoint rejects a burst of simultaneous
    # requests and returns at most 500 records per page.  A single 30-day
    # request can therefore omit the most recent week for hourly markets.
    # Walk the cursor until the requested end time so a missing page is never
    # misrepresented as a zero funding rate.
    entries = []
    cursor = start_ms
    while cursor < end_ms:
        for attempt in range(3):
            try:
                async with HYPERLIQUID_HISTORY_SEMAPHORE:
                    rows = await async_request_json(
                        HYPERLIQUID_INFO,
                        method="POST",
                        body={"type": "fundingHistory", "coin": symbol, "startTime": cursor, "endTime": end_ms},
                        timeout=20,
                    )
                    await asyncio.sleep(1.1)
                break
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))
        if not rows:
            break
        entries.extend(
            (int(row.get("time", 0) or 0), float(row.get("fundingRate", 0) or 0))
            for row in rows
        )
        last_ts = int(rows[-1].get("time", 0) or 0)
        if len(rows) < 500 or last_ts <= cursor:
            break
        cursor = last_ts + 1
    return entries


async def gate_history(contract, start_ms, end_ms):
    """Read Gate's public USDT perpetual funding history."""
    query = urlencode({
        "contract": contract,
        "from": int(start_ms / 1000),
        "to": int(end_ms / 1000),
        "limit": 1000,
    })
    rows = await async_request_json(
        f"{GATE_BASE}/api/v4/futures/usdt/funding_rate?{query}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=20,
    )
    entries = []
    for row in rows if isinstance(rows, list) else []:
        timestamp = int(row.get("t", 0) or 0) * 1000
        if start_ms <= timestamp <= end_ms:
            entries.append((timestamp, float(row.get("r", 0) or 0)))
    return entries


async def bybit_history(symbol, start_ms, end_ms):
    """Read Bybit linear perpetual funding history, paging backward."""
    entries = {}
    cursor_end = end_ms
    while cursor_end > start_ms:
        query = urlencode({
            "category": "linear",
            "symbol": symbol,
            "limit": 200,
            "startTime": start_ms,
            "endTime": cursor_end,
        })
        payload = await async_request_json(
            f"{BYBIT_BASE}/v5/market/funding/history?{query}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        )
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit funding history error: {payload.get('retMsg') or payload.get('retCode')}")
        rows = (payload.get("result") or {}).get("list", [])
        if not rows:
            break
        timestamps = []
        for row in rows:
            timestamp = int(row.get("fundingRateTimestamp", 0) or 0)
            if start_ms <= timestamp <= end_ms:
                entries[timestamp] = float(row.get("fundingRate", 0) or 0)
                timestamps.append(timestamp)
        oldest = min(timestamps or [0])
        if len(rows) < 200 or oldest <= start_ms or not oldest:
            break
        cursor_end = oldest - 1
    return sorted(entries.items())


async def okx_history(inst_id, start_ms, end_ms):
    """Read OKX perpetual funding history using its backward cursor."""
    entries = {}
    after = None
    while True:
        query = {"instId": inst_id, "limit": 400}
        if after is not None:
            query["after"] = after
        payload = await async_request_json(
            f"{OKX_BASE}/api/v5/public/funding-rate-history?{urlencode(query)}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        )
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX funding history error: {payload.get('msg') or payload.get('code')}")
        rows = payload.get("data", [])
        if not rows:
            break
        stop = False
        for row in rows:
            timestamp = int(row.get("fundingTime", 0) or 0)
            if timestamp < start_ms:
                stop = True
                continue
            if timestamp <= end_ms:
                entries[timestamp] = float(row.get("fundingRate", 0) or 0)
        oldest = int(rows[-1].get("fundingTime", 0) or 0)
        if stop or oldest <= start_ms or len(rows) < 400:
            break
        after = oldest
    return sorted(entries.items())


async def deribit_history(instrument, start_ms, end_ms):
    """Read Deribit hourly perpetual funding (interest_1h, decimal rate)."""
    query = urlencode({
        "instrument_name": instrument,
        "start_timestamp": start_ms,
        "end_timestamp": end_ms,
    })
    payload = await async_request_json(
        f"{DERIBIT_BASE}/get_funding_rate_history?{query}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=20,
    )
    if not isinstance(payload, dict) or payload.get("error"):
        raise RuntimeError(f"Deribit funding history error for {instrument}")
    return [
        (int(row.get("timestamp", 0) or 0), float(row.get("interest_1h", 0) or 0))
        for row in payload.get("result", [])
        if start_ms <= int(row.get("timestamp", 0) or 0) <= end_ms
    ]


async def kraken_history(symbol, start_ms, end_ms):
    """Read Kraken Futures relative funding rates (not absolute contract cash)."""
    query = urlencode({"symbol": symbol})
    payload = await async_request_json(
        f"{KRAKEN_FUTURES_BASE}/historical-funding-rates?{query}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=20,
    )
    if not isinstance(payload, dict) or payload.get("result") != "success":
        raise RuntimeError(f"Kraken Futures funding history error for {symbol}")
    entries = []
    for row in payload.get("rates", []):
        timestamp = row.get("timestamp")
        if not timestamp:
            continue
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        timestamp_ms = int(parsed.timestamp() * 1000)
        if start_ms <= timestamp_ms <= end_ms:
            entries.append((timestamp_ms, float(row.get("relativeFundingRate", 0) or 0)))
    return entries


def summarize(entries, now):
    result = {}
    for days in PERIODS:
        cutoff = millis(now - timedelta(days=days))
        window_entries = [(ts, rate) for ts, rate in entries if ts >= cutoff]
        coverage_days = (
            (max(ts for ts, _ in window_entries) - min(ts for ts, _ in window_entries)) / 86_400_000
            if len(window_entries) >= 2 else 0
        )
        result[str(days)] = {
            "rate": sum(rate for _, rate in window_entries),
            "count": len(window_entries),
            "coverage_days": coverage_days,
            # A partial history must never be interpreted as a zero funding
            # rate. Eighty percent leaves room for the different funding
            # cadences while still rejecting a recently missing venue.
            "is_complete": coverage_days >= days * 0.8,
        }
    return result


def best_spread(venues, period):
    available = [
        {"venue": name, "rate": data["periods"][period]["rate"]}
        for name, data in venues.items()
        if data is not None and data["periods"][period].get("is_complete")
    ]
    if len(available) < 2:
        return None
    low = min(available, key=lambda item: item["rate"])
    high = max(available, key=lambda item: item["rate"])
    spread = high["rate"] - low["rate"]
    return {
        "spread": spread,
        "long": low["venue"],
        "short": high["venue"],
        "direction": f"{low['venue']} 多 / {high['venue']} 空",
    }


async def safe_history(loader):
    try:
        async with HISTORY_SEMAPHORE:
            return await loader()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


async def safe_market_map(loader):
    """Keep one public venue outage from blocking the other scan venues."""
    try:
        return await loader()
    except Exception:
        # The affected venue remains absent/unknown in each row; never turn a
        # failed market catalogue request into a zero funding rate.
        return {}


async def main():
    now = datetime.now(timezone.utc)
    start_ms = millis(now - timedelta(days=30, hours=2))
    end_ms = millis(now)
    symbol_maps = dict(zip(
        PLATFORMS,
        await asyncio.gather(
            safe_market_map(load_aster_symbols),
            safe_market_map(load_binance_symbols),
            safe_market_map(load_bybit_symbols),
            safe_market_map(load_deribit_symbols),
            safe_market_map(load_gate_symbols),
            safe_market_map(load_hyperliquid_symbols),
            safe_market_map(load_kraken_symbols),
            safe_market_map(load_lighter_symbols),
            safe_market_map(load_okx_symbols),
        ),
    ))
    history_loaders = {
        "Aster": lambda row, start, end: aster_history(row["symbol"], start, end),
        "Binance": lambda row, start, end: binance_history(row["symbol"], start, end),
        "Bybit": lambda row, start, end: bybit_history(row["symbol"], start, end),
        "Deribit": lambda row, start, end: deribit_history(row["symbol"], start, end),
        "Gate": lambda row, start, end: gate_history(row["symbol"], start, end),
        "Hyperliquid": lambda row, start, end: hyperliquid_history(row["symbol"], start, end),
        "Kraken": lambda row, start, end: kraken_history(row["symbol"], start, end),
        "Lighter": lambda row, start, end: lighter_history(row["market_id"], start, end),
        "OKX": lambda row, start, end: okx_history(row["symbol"], start, end),
    }

    history_tasks = {}
    for item in MARKETS:
        symbol = item["symbol"]
        for venue in PLATFORMS:
            venue_row = symbol_maps[venue].get(symbol)
            if venue_row:
                history_tasks[(symbol, venue)] = safe_history(
                    lambda row=venue_row, loader=history_loaders[venue]: loader(row, start_ms, end_ms)
                )

    history_results = await asyncio.gather(*history_tasks.values()) if history_tasks else []
    loaded_all = dict(zip(history_tasks.keys(), history_results))

    rows = []
    for item in MARKETS:
        symbol = item["symbol"]
        venues = {}
        for venue in PLATFORMS:
            venue_row = symbol_maps[venue].get(symbol)
            entries = loaded_all.get((symbol, venue))
            if venue_row is None or entries is None:
                venues[venue] = None
            elif isinstance(entries, dict) and "error" in entries:
                venues[venue] = {"error": entries["error"]}
            else:
                venues[venue] = {
                    "symbol": venue_row["symbol"],
                    "liquidity": {"day_volume": float(venue_row.get("day_volume", 0) or 0)},
                    "periods": summarize(entries, now),
                }

        spreads = {
            str(days): best_spread(
                {name: data for name, data in venues.items() if data and "periods" in data},
                str(days),
            )
            for days in PERIODS
        }
        rows.append(
            {
                "symbol": symbol,
                "venues": venues,
                "available_count": sum(1 for item in venues.values() if item and "periods" in item),
                "spreads": spreads,
            }
        )

    rows.sort(key=lambda row: row["spreads"]["30"]["spread"] if row["spreads"]["30"] else -1, reverse=True)
    print(
        json.dumps(
            {
                "generated_at": now.isoformat(),
                "periods": list(PERIODS),
                "platforms": list(PLATFORMS),
                "rows": rows,
                "note": "表内为九家平台历史累计资金费率；建议方向为低费率平台做多、高费率平台做空。各平台仅纳入公开市场目录中可交易的永续合约。",
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
