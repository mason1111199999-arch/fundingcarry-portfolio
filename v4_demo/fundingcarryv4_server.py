#!/usr/bin/env python3
"""FundingCarry v4 Gate CrossEx read-only portfolio demo.

The browser never receives Gate credentials.  One Gate CrossEx API credential
is used for all private account, position, order and ledger reads.  The
FundingCarry scanner remains a public strategy-data source and is proxied by
this process so the page has one same-origin backend.

Account write endpoints are permanently disabled in this portfolio copy. The
browser never receives credentials and no submit request is retried.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
HTML_PATH = ROOT / "fundingcarryv4.html"
GATE_BASE = "https://api.gateio.ws/api/v4"
SCANNER_URL = os.getenv(
    "FUNDINGCARRY_SCANNER_URL",
    "http://127.0.0.1:8765/api/funding-history",
).strip()
SCANNER_URLS = (SCANNER_URL,) if SCANNER_URL else ()
SCANNER_CACHE_PATH = ROOT / "state" / "opportunities_cache.json"
EXECUTION_JOURNAL_PATH = ROOT / "state" / "fundingcarryv4_executions.json"
GUARD_STATE_PATH = ROOT / "state" / "fundingcarryv4_guards.json"
_scanner_cache_lock = threading.Lock()
_trade_asset_lock = threading.Lock()
_execution_lock = threading.Lock()
_guard_lock = threading.Lock()
# Coordinate two-leg submission and guard snapshots within this process.
# The portfolio edition remains read-only; this documents production design.
_trade_guard_coordination_lock = threading.RLock()
_ticker_lock = threading.Lock()
_rule_lock = threading.Lock()
_ticker_cache: tuple[float, dict[str, Decimal]] = (0.0, {})
_rule_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])
_scoped_rule_cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]]]] = {}
_trade_asset_cache: tuple[float, dict[str, str]] = (0.0, {})
_scoped_ticker_cache: tuple[float, dict[str, Decimal]] = (0.0, {})
_ticker_last_request = 0.0
TRADE_ASSET_CACHE_SECONDS = 300.0
CROSSEX_VENUES = {
    "binance",
    "okx",
    "gate",
    "bybit",
    "kraken",
    "hyperliquid",
    "deribit",
    "lighter",
}
EXCLUDED_OPPORTUNITY_ASSETS: set[str] = set()
PREFLIGHT_TTL_SECONDS = 60
DEFAULT_POSITION_LEVERAGE = Decimal("3")
BTC_POSITION_LEVERAGE = DEFAULT_POSITION_LEVERAGE
NEW_ORDER_POSITION_LEVERAGE = DEFAULT_POSITION_LEVERAGE
# Gate CrossEx returns these rates as ratios (e.g. 2.0 means 200%). The
# dashboard and preflight contract use percentage points, so normalize at the
# API boundary and keep the safety threshold in the same unit.
GATE_MARGIN_RATE_TO_PERCENT = Decimal("100")
MIN_INITIAL_MARGIN_RATE_PERCENT = Decimal("150")
MAX_EXPOSURE_MISMATCH_RATIO = Decimal("0.01")
MAX_NOTIONAL_MISMATCH = Decimal("1")
PREFLIGHT_GATE_TIMEOUT_SECONDS = 8
FUNDING_WINDOW_MS = 24 * 60 * 60 * 1000
FUNDING_PAGE_LIMIT = 1000
FUNDING_MAX_PAGES = 5
DAILY_LEDGER_PAGE_LIMIT = 1000
DAILY_LEDGER_MAX_PAGES = 20
DAILY_LEDGER_DETAIL_LIMIT = 500
DAILY_LEDGER_TYPES = ("TRADING_FEE", "FUNDING_FEE")
DAILY_LEDGER_TIMEZONE = "Asia/Shanghai"
TICKER_MIN_INTERVAL_SECONDS = 1.05
GUARD_BELOW_MINIMUM_REASON = "guard difference below executable minimum"
_preflight_lock = threading.Lock()
_preflight_tokens: dict[str, dict[str, Any]] = {}
_leverage_preflight_lock = threading.Lock()
_leverage_preflight_tokens: dict[str, dict[str, Any]] = {}


def _atomic_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _json_store_read(path: Path, default: Any) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default
    return value


def _journal_upsert(task_id: str, patch: dict[str, Any]) -> None:
    with _execution_lock:
        data = _json_store_read(EXECUTION_JOURNAL_PATH, {})
        if not isinstance(data, dict):
            data = {}
        row = data.get(task_id)
        if not isinstance(row, dict):
            row = {"task_id": task_id}
        row.update(patch)
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        data[task_id] = row
        _atomic_json_write(EXECUTION_JOURNAL_PATH, data)


def _guard_upsert(guard_id: str, patch: dict[str, Any]) -> None:
    with _guard_lock:
        data = _json_store_read(GUARD_STATE_PATH, {})
        if not isinstance(data, dict):
            data = {}
        row = data.get(guard_id)
        if not isinstance(row, dict):
            row = {"guard_id": guard_id}
        row.update(patch)
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        data[guard_id] = row
        _atomic_json_write(GUARD_STATE_PATH, data)


def _register_guard(legs: list[dict[str, str]], positions: dict[str, Decimal]) -> str:
    asset = legs[0]["canonical_asset"]
    guard_id = "guard-" + hashlib.sha256(
        "|".join(sorted(leg["symbol"] for leg in legs)).encode("utf-8")
    ).hexdigest()[:20]
    _guard_upsert(
        guard_id,
        {
            "state": "active",
            "asset": asset,
            "legs": [
                {
                    "symbol": leg["symbol"],
                    "venue": leg["venue"],
                    "canonical_asset": leg["canonical_asset"],
                    "multiplier": format(
                        _asset_multiplier(_symbol_parts(leg["symbol"])[1]), "f"
                    ),
                }
                for leg in legs
            ],
            "baseline": {
                leg["symbol"]: format(positions.get(leg["symbol"], Decimal("0")), "f")
                for leg in legs
            },
            "observation": None,
            "observation_count": 0,
        },
    )
    return guard_id


def _symbol_parts(symbol: str) -> tuple[str, str, str]:
    parts = str(symbol).strip().upper().split("_")
    if len(parts) < 4 or parts[1] != "FUTURE":
        raise ValueError(f"无法解析永续合约 {symbol}")
    return parts[0], "_".join(parts[2:-1]), parts[-1]


def _asset_multiplier(base: str) -> Decimal:
    match = re.match(r"^(1000000|100000|10000|1000)(.+)$", base)
    return Decimal(match.group(1)) if match else Decimal("1")


def _canonical_contract_asset(symbol: str) -> str:
    _, base, _ = _symbol_parts(symbol)
    return re.sub(r"^(1000000|100000|10000|1000)", "", base) or base


def _signed_position_map(payload: Any) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for row in _rows(payload):
        symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default="")).upper()
        if not symbol:
            continue
        raw = _first(
            row, "position_qty", "size", "quantity", "position_size", "position_amt", default="0"
        )
        try:
            quantity = Decimal(str(raw))
        except InvalidOperation as exc:
            raise RuntimeError(f"Gate position quantity invalid for {symbol}") from exc
        side = str(_first(row, "position_side", "side", "direction", default="")).upper()
        if side == "SHORT" and quantity > 0:
            quantity = -quantity
        elif side == "LONG" and quantity < 0:
            quantity = -quantity
        result[symbol] = quantity
    return result


def _position_value_map(payload: Any) -> dict[str, Decimal]:
    """Read Gate's authoritative per-position notional values for leverage checks."""
    values: dict[str, Decimal] = {}
    quantities = _signed_position_map(payload)
    for row in _rows(payload):
        symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default="")).upper()
        if not symbol or quantities.get(symbol, Decimal("0")) == 0:
            continue
        raw = _first(row, "position_value", "notional", default=None)
        try:
            value = Decimal(str(raw))
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(
                f"Gate position value unavailable for open position {symbol}; refusing leverage estimate"
            ) from exc
        if not value.is_finite() or value <= 0:
            raise RuntimeError(
                f"Gate position value invalid for open position {symbol}; refusing leverage estimate"
            )
        values[symbol] = abs(value)
    return values


def _ticker_map(payload: Any) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for row in _rows(payload):
        symbol = str(row.get("symbol", "")).upper()
        value = _first(row, "mark_price", "last_price", "index_price", default=None)
        if not symbol or value in (None, ""):
            continue
        try:
            price = Decimal(str(value))
        except InvalidOperation:
            continue
        if price > 0:
            result[symbol] = price
    return result


def _load_tickers(
    client: "GateCrossExClient",
    max_age: float = 0.8,
    timeout: float = 12,
    symbols: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict[str, Decimal]:
    global _ticker_cache, _scoped_ticker_cache, _ticker_last_request
    requested = sorted({str(symbol).strip().upper() for symbol in symbols or () if str(symbol).strip()})
    with _ticker_lock:
        now = time.monotonic()
        if requested:
            if _ticker_cache[1] and now - _ticker_cache[0] <= max_age and set(requested) <= _ticker_cache[1].keys():
                return {symbol: _ticker_cache[1][symbol] for symbol in requested}
            if _scoped_ticker_cache[1] and now - _scoped_ticker_cache[0] <= max_age and set(requested) <= _scoped_ticker_cache[1].keys():
                return {symbol: _scoped_ticker_cache[1][symbol] for symbol in requested}
        elif _ticker_cache[1] and now - _ticker_cache[0] <= max_age:
            return dict(_ticker_cache[1])

        wait = TICKER_MIN_INTERVAL_SECONDS - (now - _ticker_last_request)
        if _ticker_last_request and wait > 0:
            time.sleep(wait)
        _ticker_last_request = time.monotonic()
        params = {"symbols": ",".join(requested)} if requested else None
        value = _ticker_map(
            client.get("/crossex/market/tickers", params=params, timeout=timeout)
        )
        if not value:
            raise RuntimeError("Gate CrossEx ticker snapshot is empty")
        if requested:
            missing = [symbol for symbol in requested if symbol not in value]
            if missing:
                raise RuntimeError(
                    "Gate CrossEx ticker snapshot omitted requested symbols: " + ", ".join(missing)
                )
            _scoped_ticker_cache = (time.monotonic(), value)
            return {symbol: value[symbol] for symbol in requested}
        _ticker_cache = (time.monotonic(), value)
        return dict(value)


def _load_rule_rows(
    client: "GateCrossExClient",
    max_age: float = 30.0,
    timeout: float = 12,
    symbols: list[str] | tuple[str, ...] | set[str] | None = None,
) -> list[dict[str, Any]]:
    global _rule_cache
    requested = tuple(sorted({str(symbol).strip().upper() for symbol in symbols or () if str(symbol).strip()}))
    now = time.monotonic()
    with _rule_lock:
        if requested:
            full_by_symbol = {str(row.get("symbol", "")).upper(): row for row in _rule_cache[1]}
            if _rule_cache[1] and now - _rule_cache[0] <= max_age and all(symbol in full_by_symbol for symbol in requested):
                return [dict(full_by_symbol[symbol]) for symbol in requested]
            scoped = _scoped_rule_cache.get(requested)
            if scoped and now - scoped[0] <= max_age:
                return [dict(row) for row in scoped[1]]
        elif _rule_cache[1] and now - _rule_cache[0] <= max_age:
            return [dict(row) for row in _rule_cache[1]]

    # Do not hold the shared cache lock during network I/O. The symbol menu
    # loads the full catalog, while preflight/submit/guards need only a few
    # contracts; a slow catalog response must never queue these safety reads.
    params = {"symbols": ",".join(requested)} if requested else None
    rows = _rows(client.get("/crossex/rule/symbols", params=params, timeout=timeout))
    if not rows:
        raise RuntimeError("Gate CrossEx rule snapshot is empty")
    if requested:
        returned = {str(row.get("symbol", "")).upper() for row in rows}
        missing = [symbol for symbol in requested if symbol not in returned]
        if missing:
            raise RuntimeError(
                "Gate CrossEx rule snapshot omitted requested symbols: " + ", ".join(missing)
            )
        with _rule_lock:
            _scoped_rule_cache[requested] = (time.monotonic(), [dict(row) for row in rows])
        return [dict(row) for row in rows]
    with _rule_lock:
        _rule_cache = (time.monotonic(), [dict(row) for row in rows])
    return [dict(row) for row in rows]


def _venue_supported(value: Any) -> bool:
    name = str(value or "").strip().lower()
    aliases = {"gate.io": "gate", "gateio": "gate"}
    return aliases.get(name, name) in CROSSEX_VENUES


def filter_crossex_opportunities(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove every opportunity whose selected two-leg route is not CrossEx-supported."""
    result = dict(payload)
    source_key = "rows" if isinstance(payload.get("rows"), list) else "opportunities"
    source = payload.get(source_key)
    if not isinstance(source, list):
        result[source_key] = []
        result["crossex_filter"] = {"kept": 0, "discarded": 0}
        return result
    kept: list[Any] = []
    for row in source:
        if not isinstance(row, dict):
            continue
        asset = str(row.get("symbol") or row.get("asset") or "").strip().upper()
        if asset in EXCLUDED_OPPORTUNITY_ASSETS:
            continue
        venues = row.get("venues")
        clean_venues = (
            {name: value for name, value in venues.items() if _venue_supported(name)}
            if isinstance(venues, dict)
            else {}
        )
        complete: dict[str, dict[str, Any]] = {}
        for name, venue_row in clean_venues.items():
            periods = venue_row.get("periods", {}) if isinstance(venue_row, dict) else {}
            if not isinstance(periods, dict):
                continue
            if all(
                _number(periods.get(days, {}).get("rate")) is not None
                and periods.get(days, {}).get("is_complete") is True
                for days in ("7", "14", "30")
            ):
                complete[name] = venue_row
        if len(complete) < 2:
            continue
        ranked = sorted(
            complete,
            key=lambda name: _number(complete[name]["periods"]["30"]["rate"], 0) or 0,
        )
        long_venue, short_venue = ranked[0], ranked[-1]
        if long_venue == short_venue:
            continue
        clean = dict(row)
        clean["venues"] = complete
        clean_spreads: dict[str, Any] = {}
        long_periods = complete[long_venue]["periods"]
        short_periods = complete[short_venue]["periods"]
        for days in ("7", "14", "30"):
            long_rate = _number(long_periods[days].get("rate"), 0) or 0
            short_rate = _number(short_periods[days].get("rate"), 0) or 0
            clean_spreads[days] = {
                "spread": short_rate - long_rate,
                "long": long_venue,
                "short": short_venue,
                "direction": f"{long_venue} 多 / {short_venue} 空",
            }
        clean["spreads"] = clean_spreads
        clean["available_count"] = len(complete)
        kept.append(clean)
    result[source_key] = kept
    result["crossex_filter"] = {"kept": len(kept), "discarded": len(source) - len(kept)}
    result["excluded_assets"] = sorted(EXCLUDED_OPPORTUNITY_ASSETS)
    result["supported_venues"] = sorted(CROSSEX_VENUES)
    result["source_total_rows"] = result.get("total_rows", len(source))
    result["total_rows"] = len(kept)
    result["note"] = "每个币仅保留 Gate CrossEx 支持平台，并在这些平台内重新选择30日最低费率做多、最高费率做空，再用同一双腿计算7/14/30日差。"
    return result


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first(row: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return default


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class Credentials:
    key: str
    secret: str
    source: str


def load_credentials() -> Credentials | None:
    direct_key = os.getenv("GATE_API_KEY", "").strip()
    direct_secret = os.getenv("GATE_API_SECRET", "").strip()
    if direct_key and direct_secret:
        return Credentials(direct_key, direct_secret, "process environment")

    configured = os.getenv("FUNDINGCARRY_V4_GATE_ENV_FILE", "").strip()
    if configured:
        # An explicitly configured file is an isolation boundary. Do not fall
        # back to credentials belonging to another service/account if it is
        # absent or incomplete.
        candidates = [Path(configured)]
    else:
        # The public portfolio build never searches production or shared
        # credential locations; credentials must be explicitly local.
        candidates = [ROOT / ".env"]
    for path in candidates:
        if path is None or not path.is_file():
            continue
        values = _read_env(path)
        key = values.get("GATE_API_KEY", "").strip()
        secret = values.get("GATE_API_SECRET", "").strip()
        if key and secret:
            return Credentials(key, secret, str(path))
    return None


class GateCrossExClient:
    def __init__(self, credentials: Credentials):
        self.credentials = credentials

    @staticmethod
    def signed_headers(
        key: str,
        secret: str,
        method: str,
        endpoint: str,
        query: str,
        body: str,
        timestamp: str,
    ) -> dict[str, str]:
        body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
        canonical = f"{method.upper()}\n/api/v4{endpoint}\n{query}\n{body_hash}\n{timestamp}"
        signature = hmac.new(
            secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha512
        ).hexdigest()
        return {
            "KEY": key,
            "Timestamp": timestamp,
            "SIGN": signature,
            "Accept": "application/json",
            "User-Agent": "fundingcarry-v4-local/0.1",
        }

    def request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        timeout: float = 12,
    ) -> Any:
        params = params or {}
        query_items = sorted(
            (str(key), str(value))
            for key, value in params.items()
            if value not in (None, "")
        )
        # Gate's APIv4 signature canonicalizes the query string without URL
        # encoding. Keep the submitted parameter order identical, preserve the
        # comma in CrossEx's comma-separated symbols list, and encode reserved
        # characters in the actual URL independently.
        signature_query = "&".join(f"{key}={value}" for key, value in query_items)
        request_query = "&".join(
            f"{quote(key, safe='')}={quote(value, safe=',')}"
            for key, value in query_items
        )
        body_text = "" if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        timestamp = str(time.time())
        headers = self.signed_headers(
            self.credentials.key,
            self.credentials.secret,
            method,
            endpoint,
            signature_query,
            body_text,
            timestamp,
        )
        if body is not None:
            headers["Content-Type"] = "application/json"
        url = GATE_BASE + endpoint + (f"?{request_query}" if request_query else "")
        request = Request(
            url,
            headers=headers,
            method=method.upper(),
            data=body_text.encode("utf-8") if body is not None else None,
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:240]
            raise RuntimeError(f"Gate {endpoint} HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Gate {endpoint} unavailable: {type(exc).__name__}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Gate {endpoint} returned invalid JSON") from exc

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        timeout: float = 12,
    ) -> Any:
        return self.request("GET", endpoint, params=params, timeout=timeout)

    def post(self, endpoint: str, body: dict[str, Any]) -> Any:
        return self.request("POST", endpoint, body=body)

    def delete(self, endpoint: str) -> Any:
        return self.request("DELETE", endpoint)


def live_trading_enabled() -> bool:
    # This portfolio build is permanently read-only. Keep execution
    # reconciliation code inspectable, but never enable account writes here.
    return False


def _decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} 必须是数字") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{field} 必须大于 0")
    return number


def validate_pair_request(
    payload: dict[str, Any],
    rules: dict[str, dict[str, Any]],
    account: dict[str, Any],
    prices: dict[str, Decimal] | None = None,
) -> list[dict[str, str]]:
    if str(account.get("account_mode", "")).upper() != "CROSS_EXCHANGE":
        raise ValueError("Gate 账户不是 CROSS_EXCHANGE 模式")
    if str(account.get("position_mode", "")).upper() != "SINGLE":
        raise ValueError("当前只支持 Gate SINGLE 单向持仓模式")
    legs = payload.get("legs")
    if not isinstance(legs, list) or len(legs) != 2:
        raise ValueError("必须提供且只能提供两条腿")
    normalized: list[dict[str, str]] = []
    for index, leg in enumerate(legs, start=1):
        if not isinstance(leg, dict):
            raise ValueError(f"第 {index} 腿格式错误")
        symbol = str(leg.get("symbol", "")).strip().upper()
        side = str(leg.get("side", "")).strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"第 {index} 腿方向必须是 BUY 或 SELL")
        rule = rules.get(symbol)
        if rule is None:
            raise ValueError(f"Gate CrossEx 不存在交易对 {symbol}")
        if str(rule.get("business_type", "")).upper() != "FUTURE":
            raise ValueError(f"{symbol} 不是永续合约")
        if str(rule.get("state", "")).lower() != "live":
            raise ValueError(f"{symbol} 当前不可交易")
        if str(rule.get("support_cross", "")).lower() != "true":
            raise ValueError(f"{symbol} 不支持 CrossEx 共用保证金")
        quantity = _decimal(leg.get("qty"), f"第 {index} 腿数量")
        minimum = _decimal(rule.get("min_size", "0.000000000000000001"), "min_size")
        api_maximum: Decimal | None = None
        try:
            api_maximum = _decimal(rule.get("max_market_size"), "max_market_size")
        except ValueError:
            # Some Gate CrossEx rule snapshots contain null/empty/non-numeric
            # max_market_size values. Do not silently invent an exchange limit;
            # require an explicit user-entered cap for this leg instead.
            api_maximum = None
        manual_maximum: Decimal | None = None
        manual_raw = leg.get("manual_max_market_size")
        if manual_raw not in (None, ""):
            manual_maximum = _decimal(
                manual_raw, f"第 {index} 腿手动最大市价数量"
            )
        if api_maximum is None and manual_maximum is None:
            raise ValueError(
                f"{symbol} 交易所返回的 max_market_size 无效；"
                f"请在第 {index} 腿输入正数最大市价数量"
            )
        if api_maximum is None:
            maximum = manual_maximum
            maximum_source = "manual_fallback"
        elif manual_maximum is None:
            maximum = api_maximum
            maximum_source = "exchange"
        else:
            maximum = min(api_maximum, manual_maximum)
            maximum_source = "exchange_and_manual_min"
        step = _decimal(rule.get("lot_size", "0.000000000000000001"), "lot_size")
        if quantity < minimum or quantity > maximum:
            raise ValueError(f"{symbol} 数量不在 {minimum} 至 {maximum} 范围内")
        if quantity % step != 0:
            raise ValueError(f"{symbol} 数量必须是步长 {step} 的整数倍")
        if prices is None or symbol not in prices:
            raise ValueError(f"{symbol} 标记价不可读，禁止提交")
        price = prices[symbol]
        notional = quantity * price
        minimum_notional = Decimal(str(rule.get("min_notional", "0") or "0"))
        if notional < minimum_notional:
            raise ValueError(f"{symbol} 名义价值 {notional} 低于最小值 {minimum_notional}")
        venue, base, _ = _symbol_parts(symbol)
        multiplier = _asset_multiplier(base)
        normalized.append(
            {
                "symbol": symbol,
                "venue": venue,
                "canonical_asset": _canonical_contract_asset(symbol),
                "side": side,
                "qty": format(quantity, "f"),
                "manual_max_market_size": (
                    format(manual_maximum, "f") if manual_maximum is not None else ""
                ),
                "max_market_size": format(maximum, "f"),
                "max_market_size_source": maximum_source,
                "base_exposure": format(quantity * multiplier, "f"),
                "mark_price": format(price, "f"),
                "notional": format(notional, "f"),
                "type": "MARKET",
                "reduce_only": "true" if bool(payload.get("reduce_only")) else "false",
            }
        )
    if normalized[0]["symbol"] == normalized[1]["symbol"]:
        raise ValueError("两条腿不能使用同一个交易对")
    if normalized[0]["side"] == normalized[1]["side"]:
        raise ValueError("两条腿方向必须相反")
    if normalized[0]["canonical_asset"] != normalized[1]["canonical_asset"]:
        raise ValueError("两条腿必须属于同一个基础币种")
    if normalized[0]["venue"] == normalized[1]["venue"]:
        raise ValueError("两条腿必须来自不同交易所")
    exposure_one = Decimal(normalized[0]["base_exposure"])
    exposure_two = Decimal(normalized[1]["base_exposure"])
    mismatch_ratio = abs(exposure_one - exposure_two) / max(exposure_one, exposure_two)
    if mismatch_ratio > MAX_EXPOSURE_MISMATCH_RATIO:
        raise ValueError("两腿基础币敞口偏差超过 0.5%")
    notional_one = Decimal(normalized[0]["notional"])
    notional_two = Decimal(normalized[1]["notional"])
    if abs(notional_one - notional_two) > MAX_NOTIONAL_MISMATCH:
        raise ValueError("两腿名义价值绝对偏差超过 0.5 USDT")
    initial_rate = _decimal(account.get("initial_margin_rate"), "initial_margin_rate")
    if initial_rate < MIN_INITIAL_MARGIN_RATE_PERCENT:
        raise ValueError(
            f"统一账户初始保证金率低于 {MIN_INITIAL_MARGIN_RATE_PERCENT}%"
        )
    # Keep requiring a valid positive equity snapshot, but do not impose a
    # local gross-notional/effective-leverage ceiling. Gate remains the
    # authoritative margin and liquidation-risk engine.
    _decimal(account.get("margin_balance"), "margin_balance")
    return normalized


def _gross_notional(
    positions: dict[str, Decimal],
    prices: dict[str, Decimal],
    position_values: dict[str, Decimal] | None = None,
) -> Decimal:
    if position_values is not None:
        missing = [
            symbol for symbol, quantity in positions.items()
            if quantity != 0 and symbol not in position_values
        ]
        if missing:
            raise RuntimeError(
                "Gate omitted authoritative notional for open positions: " + ", ".join(missing)
            )
        return sum(position_values.values(), Decimal("0"))
    return sum(
        (abs(quantity) * prices.get(symbol, Decimal("0")) for symbol, quantity in positions.items()),
        Decimal("0"),
    )


def _validate_reduce_only_positions(
    legs: list[dict[str, str]], positions: dict[str, Decimal]
) -> None:
    for leg in legs:
        current = positions.get(leg["symbol"], Decimal("0"))
        quantity = Decimal(leg["qty"])
        if current == 0:
            raise ValueError(f"{leg['symbol']} 当前没有可减仓仓位")
        if leg["side"] == "SELL" and current <= 0:
            raise ValueError(f"{leg['symbol']} SELL reduce-only 不能减少当前空仓")
        if leg["side"] == "BUY" and current >= 0:
            raise ValueError(f"{leg['symbol']} BUY reduce-only 不能减少当前多仓")
        if quantity > abs(current):
            raise ValueError(f"{leg['symbol']} 减仓数量超过当前持仓")


def _risk_limit_tiers_by_symbol(payload: Any) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in _rows(payload):
        symbol = str(row.get("symbol", "")).strip().upper()
        tiers = row.get("tiers")
        if symbol and isinstance(tiers, list):
            result[symbol] = [tier for tier in tiers if isinstance(tier, dict)]
    return result


def _maximum_leverage_for_notional(
    symbol: str,
    notional: Decimal,
    risk_limits: dict[str, list[dict[str, Any]]],
) -> Decimal:
    tiers = risk_limits.get(symbol.upper(), [])
    matching: list[Decimal] = []
    for tier in tiers:
        try:
            minimum = Decimal(str(tier.get("min_risk_limit_value", "0") or "0"))
            maximum = Decimal(str(tier.get("max_risk_limit_value")))
            leverage_max = Decimal(str(tier.get("leverage_max")))
        except (InvalidOperation, ValueError):
            continue
        if not all(value.is_finite() for value in (minimum, maximum, leverage_max)):
            continue
        if minimum <= notional <= maximum and leverage_max > 0:
            matching.append(leverage_max)
    if not matching:
        raise RuntimeError(
            f"{symbol} 的 Gate 风险限额没有覆盖预计名义价值 {notional}；拒绝猜测杠杆上限"
        )
    # At tier boundaries more than one tier can match; use the stricter cap.
    return min(matching)


def _target_existing_position_leverage(symbol: str, venue_max: Decimal) -> Decimal:
    """Keep the prior explicit policy for user-confirmed bulk updates to open positions."""
    requested = (
        BTC_POSITION_LEVERAGE
        if _canonical_contract_asset(symbol) == "BTC"
        else DEFAULT_POSITION_LEVERAGE
    )
    return min(requested, venue_max)


def _target_new_order_leverage(venue_max: Decimal) -> Decimal:
    """Use 10x for every new opening, capped by Gate's authoritative risk tier."""
    return min(NEW_ORDER_POSITION_LEVERAGE, venue_max)


def _attach_order_leverage_policy(
    legs: list[dict[str, str]],
    risk_limits: dict[str, list[dict[str, Any]]],
    position_values: dict[str, Decimal],
    *,
    reduce_only: bool,
) -> None:
    if reduce_only:
        return
    for leg in legs:
        symbol = leg["symbol"]
        projected_position_value = position_values.get(symbol, Decimal("0")) + Decimal(
            leg["notional"]
        )
        venue_max = _maximum_leverage_for_notional(
            symbol, projected_position_value, risk_limits
        )
        target = _target_new_order_leverage(venue_max)
        if target <= 0:
            raise RuntimeError(f"{symbol} 的可用杠杆上限无效；禁止提交")
        leg["target_leverage"] = format(target, "f")
        leg["max_leverage"] = format(venue_max, "f")


def _open_futures_position_rows(payload: Any) -> list[dict[str, Any]]:
    quantities = _signed_position_map(payload)
    rows: dict[str, dict[str, Any]] = {}
    for row in _rows(payload):
        symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default="")).upper()
        if not symbol or quantities.get(symbol, Decimal("0")) == 0:
            continue
        _symbol_parts(symbol)  # Fail closed if an unexpected non-futures position appears.
        if symbol in rows:
            raise RuntimeError(f"Gate 返回重复的活动持仓 {symbol}；拒绝批量修改杠杆")
        rows[symbol] = {**row, "symbol": symbol, "signed_qty": quantities[symbol]}
    return [rows[symbol] for symbol in sorted(rows)]


def _position_maximum_leverage(
    row: dict[str, Any],
    risk_limits: dict[str, list[dict[str, Any]]],
) -> Decimal:
    raw = _first(row, "max_leverage", default=None)
    if raw not in (None, ""):
        try:
            maximum = Decimal(str(raw))
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(f"{row['symbol']} 的 max_leverage 无效") from exc
        if maximum.is_finite() and maximum > 0:
            return maximum
        raise RuntimeError(f"{row['symbol']} 的 max_leverage 无效")
    notional_raw = _first(row, "position_value", "notional", default=None)
    try:
        notional = Decimal(str(notional_raw))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"{row['symbol']} 缺少有效持仓名义价值，无法核实杠杆上限") from exc
    if notional <= 0 or not notional.is_finite():
        raise RuntimeError(f"{row['symbol']} 持仓名义价值无效，无法核实杠杆上限")
    return _maximum_leverage_for_notional(row["symbol"], notional, risk_limits)


def _leverage_policy_snapshot(client: "GateCrossExClient") -> list[dict[str, str]]:
    positions_payload = client.get("/crossex/positions", timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS)
    position_rows = _open_futures_position_rows(positions_payload)
    if not position_rows:
        raise RuntimeError("Gate 当前没有可设置杠杆的永续持仓")
    missing_caps = [row for row in position_rows if _first(row, "max_leverage", default=None) in (None, "")]
    risk_limits: dict[str, list[dict[str, Any]]] = {}
    if missing_caps:
        symbols = [row["symbol"] for row in missing_caps]
        raw_limits = client.get(
            "/crossex/rule/risk_limits",
            params={"symbols": ",".join(symbols)},
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
        )
        risk_limits = _risk_limit_tiers_by_symbol(raw_limits)
    snapshot: list[dict[str, str]] = []
    for row in position_rows:
        maximum = _position_maximum_leverage(row, risk_limits)
        target = _target_existing_position_leverage(row["symbol"], maximum)
        current_raw = _first(row, "leverage", default="")
        current = ""
        if current_raw not in (None, ""):
            try:
                current = format(Decimal(str(current_raw)), "f")
            except (InvalidOperation, ValueError):
                current = "unknown"
        snapshot.append(
            {
                "symbol": row["symbol"],
                "position_side": str(_first(row, "position_side", default="")),
                "position_qty": format(abs(Decimal(str(row["signed_qty"]))), "f"),
                "current_leverage": current,
                "max_leverage": format(maximum, "f"),
                "target_leverage": format(target, "f"),
            }
        )
    return snapshot


def _set_and_verify_leverages(
    client: "GateCrossExClient", items: list[dict[str, str]]
) -> dict[str, str]:
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(8, len(items) or 1)) as pool:
        futures = {
            pool.submit(
                client.post,
                "/crossex/positions/leverage",
                {"symbol": item["symbol"], "leverage": item["target_leverage"]},
            ): item
            for item in items
        }
        for future in as_completed(futures):
            item = futures[future]
            try:
                response = future.result()
                if isinstance(response, dict) and str(response.get("symbol", "")).upper() not in ("", item["symbol"]):
                    errors.append(f"{item['symbol']}: Gate 返回了不匹配的合约")
            except Exception as exc:
                errors.append(f"{item['symbol']}: {type(exc).__name__}")
    symbols = [item["symbol"] for item in items]
    try:
        raw_settings = client.get(
            "/crossex/positions/leverage",
            params={"symbols": ",".join(symbols)},
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        raise RuntimeError(
            "下单前杠杆设置请求已发出，但 Gate 回读失败；不得提交开仓单，"
            "请刷新账户杠杆核实后再操作。"
        ) from exc
    if isinstance(raw_settings, dict) and isinstance(raw_settings.get("data"), dict):
        raw_settings = raw_settings["data"]
    if not isinstance(raw_settings, dict):
        raise RuntimeError("Gate 杠杆设置回读格式无效；不得提交开仓单")
    actual: dict[str, str] = {}
    for item in items:
        raw_value = raw_settings.get(item["symbol"])
        try:
            value = Decimal(str(raw_value))
        except (InvalidOperation, ValueError):
            value = Decimal("-1")
        actual[item["symbol"]] = format(value, "f") if value > 0 else "unknown"
        if value != Decimal(item["target_leverage"]):
            errors.append(
                f"{item['symbol']}: 目标 {item['target_leverage']}×，回读 {actual[item['symbol']]}×"
            )
    if errors:
        raise RuntimeError(
            "下单前杠杆设置未全部确认，系统没有提交任何开仓订单。"
            + "；".join(errors)
            + "。部分合约杠杆配置可能已改变，请先刷新核对。"
        )
    return actual


def create_leverage_preflight() -> dict[str, Any]:
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate CrossEx API 未配置")
    client = GateCrossExClient(credentials)
    items = _leverage_policy_snapshot(client)
    token = uuid.uuid4().hex
    code = f"{secrets.randbelow(10_000):04d}"
    with _leverage_preflight_lock:
        _leverage_preflight_tokens[token] = {
            "items": items,
            "confirmation_code": code,
            "expires_at": time.time() + PREFLIGHT_TTL_SECONDS,
        }
    return {
        "preflight_token": token,
        "confirmation_code": code,
        "expires_in_seconds": PREFLIGHT_TTL_SECONDS,
        "items": items,
    }


def submit_leverage_preflight(payload: dict[str, Any]) -> dict[str, Any]:
    if not live_trading_enabled():
        raise PermissionError("本机公开作品集副本为只读模式：账户写操作已永久禁用")
    token = str(payload.get("preflight_token", ""))
    with _leverage_preflight_lock:
        plan = _leverage_preflight_tokens.pop(token, None)
    if plan is None:
        raise ValueError("杠杆预检令牌不存在或已经使用")
    if time.time() > float(plan["expires_at"]):
        raise ValueError("杠杆预检已过期，请重新检查")
    if not hmac.compare_digest(
        str(payload.get("confirmation", "")), str(plan["confirmation_code"])
    ):
        raise ValueError("4 位数字确认码不匹配；请重新检查杠杆")
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate CrossEx API 未配置")
    client = GateCrossExClient(credentials)
    fresh_items = _leverage_policy_snapshot(client)
    planned = {item["symbol"]: item for item in plan["items"]}
    fresh = {item["symbol"]: item for item in fresh_items}
    if set(planned) != set(fresh):
        raise ValueError("预检后活动持仓列表已变化；未修改杠杆，请重新检查")
    for symbol, item in planned.items():
        current = fresh[symbol]
        if (
            current["position_side"] != item["position_side"]
            or current["position_qty"] != item["position_qty"]
            or current["max_leverage"] != item["max_leverage"]
            or current["target_leverage"] != item["target_leverage"]
        ):
            raise ValueError(f"{symbol} 预检后仓位或上限发生变化；未修改杠杆")
    actual = _set_and_verify_leverages(client, fresh_items)
    return {
        "state": "verified",
        "items": [
            {**item, "verified_leverage": actual[item["symbol"]]}
            for item in fresh_items
        ],
        "message": "演示版持仓杠杆检查完成；账户写操作在公开副本中永久禁用。",
    }


def _order_request(leg: dict[str, str]) -> dict[str, str]:
    return {
        key: leg[key]
        for key in ("text", "symbol", "side", "type", "qty", "reduce_only")
        if key in leg
    }


def _reconcile_order(
    client: GateCrossExClient,
    leg: dict[str, str],
    response: Any = None,
    submit_error: str = "",
) -> dict[str, Any]:
    order_id = _first(response, "id", "order_id") if isinstance(response, dict) else None
    lookup = str(order_id or leg["text"])
    last_order: Any = None
    last_error = submit_error
    for attempt in range(20):
        if attempt:
            time.sleep(0.5)
        try:
            last_order = client.get(f"/crossex/orders/{quote(lookup, safe='')}")
            last_error = ""
        except Exception as exc:
            last_error = str(exc)
            continue
        state = str(_first(last_order, "state", "status", default="UNKNOWN")).upper()
        if state in {"FILLED", "FAIL", "REJECT", "REJECTED", "CANCELLED", "CANCELED"}:
            break
    state = (
        str(_first(last_order, "state", "status", default="UNKNOWN")).upper()
        if isinstance(last_order, dict)
        else "UNKNOWN"
    )
    return {
        "leg": leg,
        "accepted": not bool(submit_error),
        "response": response,
        "order_id": str(order_id or _first(last_order, "order_id", "id", default="") or ""),
        "order": last_order,
        "reconcile_error": last_error,
        "reconcile_state": state,
    }


def _position_deltas_match(
    legs: list[dict[str, str]],
    before: dict[str, Decimal],
    after: dict[str, Decimal],
) -> bool:
    for leg in legs:
        expected = Decimal(leg["qty"]) * (Decimal("1") if leg["side"] == "BUY" else Decimal("-1"))
        actual = after.get(leg["symbol"], Decimal("0")) - before.get(leg["symbol"], Decimal("0"))
        tolerance = max(Decimal("0.00000001"), abs(expected) * Decimal("0.0001"))
        if abs(actual - expected) > tolerance:
            return False
    return True


def _guard_action_delta_matches(
    action: dict[str, Any],
    before: dict[str, Decimal],
    after: dict[str, Decimal],
) -> bool:
    symbol = str(action.get("symbol", ""))
    side = str(action.get("side", "")).upper()
    try:
        quantity = Decimal(str(action.get("qty", "0")))
    except InvalidOperation:
        return False
    if not symbol or side not in {"BUY", "SELL"} or quantity <= 0:
        return False
    expected = quantity if side == "BUY" else -quantity
    actual = after.get(symbol, Decimal("0")) - before.get(symbol, Decimal("0"))
    tolerance = max(Decimal("0.00000001"), quantity * Decimal("0.0001"))
    return abs(actual - expected) <= tolerance


def guard_decision(
    guard: dict[str, Any],
    current: dict[str, Decimal],
    rules: dict[str, dict[str, Any]],
    prices: dict[str, Decimal],
) -> dict[str, Any]:
    legs = guard.get("legs")
    baseline_raw = guard.get("baseline")
    if not isinstance(legs, list) or len(legs) != 2 or not isinstance(baseline_raw, dict):
        return {"state": "manual_reconcile_required", "reason": "guard baseline invalid"}
    baseline = {key: Decimal(str(value)) for key, value in baseline_raw.items()}
    reductions: list[Decimal] = []
    for leg in legs:
        symbol = str(leg["symbol"])
        before = baseline.get(symbol, Decimal("0"))
        now = current.get(symbol, Decimal("0"))
        if before == 0:
            return {"state": "manual_reconcile_required", "reason": f"{symbol} baseline is zero"}
        if now != 0 and (now > 0) != (before > 0):
            return {"state": "manual_reconcile_required", "reason": f"{symbol} direction flipped"}
        if abs(now) > abs(before):
            return {"state": "manual_reconcile_required", "reason": f"{symbol} position increased"}
        multiplier = Decimal(str(leg["multiplier"]))
        reductions.append((abs(before) - abs(now)) * multiplier)
    difference = reductions[0] - reductions[1]
    tolerance = max(Decimal("0.00000001"), max(reductions) * MAX_EXPOSURE_MISMATCH_RATIO)
    if abs(difference) <= tolerance:
        if max(reductions) > 0:
            return {
                "state": "baseline_update",
                "baseline": {key: format(value, "f") for key, value in current.items()},
            }
        return {"state": "healthy"}
    target_index = 1 if difference > 0 else 0
    target = legs[target_index]
    symbol = str(target["symbol"])
    current_quantity = current.get(symbol, Decimal("0"))
    if current_quantity == 0:
        return {"state": "manual_reconcile_required", "reason": f"{symbol} already zero"}
    rule = rules.get(symbol)
    price = prices.get(symbol)
    if rule is None or price is None:
        return {"state": "remote_unknown", "reason": f"{symbol} rule or price unavailable"}
    multiplier = Decimal(str(target["multiplier"]))
    raw_quantity = abs(difference) / multiplier
    step = Decimal(str(rule.get("lot_size", "0")))
    if step <= 0:
        return {"state": "manual_reconcile_required", "reason": f"{symbol} lot size invalid"}
    quantity = (raw_quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    minimum = Decimal(str(rule.get("min_size", "0")))
    minimum_notional = Decimal(str(rule.get("min_notional", "0") or "0"))
    if quantity <= 0 or quantity < minimum or quantity * price < minimum_notional:
        return {"state": "below_minimum_waiting", "reason": GUARD_BELOW_MINIMUM_REASON}
    if quantity > abs(current_quantity):
        return {"state": "manual_reconcile_required", "reason": "guard reduction exceeds position"}
    return {
        "state": "reduce_other_leg",
        "symbol": symbol,
        "side": "SELL" if current_quantity > 0 else "BUY",
        "qty": format(quantity, "f"),
    }


def create_preflight(payload: dict[str, Any]) -> dict[str, Any]:
    submitted_legs = payload.get("legs")
    if not isinstance(submitted_legs, list) or len(submitted_legs) != 2:
        raise ValueError("必须提供且只能提供两条腿")
    requested_symbols = [
        str(leg.get("symbol", "")).strip().upper()
        for leg in submitted_legs
        if isinstance(leg, dict) and str(leg.get("symbol", "")).strip()
    ]
    if len(requested_symbols) != 2:
        raise ValueError("两条腿都必须先选择有效合约")
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate CrossEx API 未配置")
    client = GateCrossExClient(credentials)
    read_started = time.monotonic()

    def timed_read(name: str, read: Any) -> tuple[Any, int, Exception | None]:
        started = time.monotonic()
        print(f"[preflight-read] {name} started", flush=True)
        try:
            value = read()
            elapsed_ms = round((time.monotonic() - started) * 1000)
            print(f"[preflight-read] {name} completed in {elapsed_ms} ms", flush=True)
            return value, elapsed_ms, None
        except Exception as exc:  # retain a safe, user-visible source failure
            elapsed_ms = round((time.monotonic() - started) * 1000)
            print(
                f"[preflight-read] {name} failed after {elapsed_ms} ms ({type(exc).__name__})",
                flush=True,
            )
            return None, elapsed_ms, exc

    # These five reads are independent. Fetch them concurrently so a slow Gate
    # endpoint or scanner fallback does not serialize the whole preflight.
    reads = {
        "account": lambda: client.get(
            "/crossex/accounts", timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS
        ),
        "positions": lambda: client.get(
            "/crossex/positions", timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS
        ),
        "rules": lambda: _load_rule_rows(
            client, timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS, symbols=requested_symbols
        ),
        "mark_prices": lambda: _load_tickers(
            client,
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
            symbols=requested_symbols,
        ),
        "fundingcarry_assets": fundingcarry_trade_assets,
    }
    is_reduce_only = bool(payload.get("reduce_only"))
    if not is_reduce_only:
        reads["risk_limits"] = lambda: client.get(
            "/crossex/rule/risk_limits",
            params={"symbols": ",".join(requested_symbols)},
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
        )
    results: dict[str, Any] = {}
    timings: dict[str, int] = {}
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=len(reads), thread_name_prefix="fc4-preflight") as pool:
        futures = {
            pool.submit(timed_read, name, read): name
            for name, read in reads.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            value, elapsed_ms, error = future.result()
            timings[name] = elapsed_ms
            if error is not None:
                failures.append(f"{name} ({elapsed_ms} ms): {type(error).__name__}: {error}")
            else:
                results[name] = value
    if failures:
        detail = "; ".join(failures)
        raise RuntimeError(f"Gate 预检读取失败：{detail}")
    gate_read_ms = round((time.monotonic() - read_started) * 1000)

    account = normalize_account(results["account"])
    rule_rows = results["rules"]
    prices = results["mark_prices"]
    positions_before = _signed_position_map(results["positions"])
    position_values = _position_value_map(results["positions"])
    allowed_assets = results["fundingcarry_assets"]
    asset_read_ms = timings["fundingcarry_assets"]
    allowed_symbols = {
        item["symbol"] for item in filter_trade_symbol_rows(rule_rows, allowed_assets)
    }
    rules = {
        str(row.get("symbol", "")).upper(): row
        for row in rule_rows
        if str(row.get("symbol", "")).upper() in allowed_symbols
    }
    try:
        legs = validate_pair_request(
            payload,
            rules,
            account,
            prices,
        )
    except ValueError as exc:
        raise ValueError(
            f"{exc}（Gate 并发读取 {gate_read_ms} ms；"
            f"主网币种 {asset_read_ms} ms）"
        ) from exc
    if bool(payload.get("reduce_only")):
        _validate_reduce_only_positions(legs, positions_before)
    else:
        _attach_order_leverage_policy(
            legs,
            _risk_limit_tiers_by_symbol(results["risk_limits"]),
            position_values,
            reduce_only=False,
        )
    token = uuid.uuid4().hex
    confirmation_code = f"{secrets.randbelow(10_000):04d}"
    plan_id = uuid.uuid4().hex[:16]
    for index, leg in enumerate(legs, start=1):
        leg["text"] = f"fc4-{plan_id}-{index}"
    plan = {
        "token": token,
        "confirmation_code": confirmation_code,
        "expires_at": time.time() + PREFLIGHT_TTL_SECONDS,
        "legs": legs,
        "positions_before": {key: format(value, "f") for key, value in positions_before.items()},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with _preflight_lock:
        _preflight_tokens[token] = plan
    return {
        "preflight_token": token,
        "expires_in_seconds": PREFLIGHT_TTL_SECONDS,
        "legs": legs,
        "live_trading_enabled": live_trading_enabled(),
        "confirmation_code": confirmation_code,
        "read_timings_ms": {
            **timings,
            "parallel_total": gate_read_ms,
            "fundingcarry_assets": asset_read_ms,
            "request_total": round((time.monotonic() - read_started) * 1000),
        },
    }


def submit_preflight(payload: dict[str, Any]) -> dict[str, Any]:
    with _trade_guard_coordination_lock:
        return _submit_preflight_coordinated(payload)


def _submit_preflight_coordinated(payload: dict[str, Any]) -> dict[str, Any]:
    if not live_trading_enabled():
        raise PermissionError("公开作品集副本为只读模式：实盘下单及账户写操作已永久禁用")
    token = str(payload.get("preflight_token", ""))
    with _preflight_lock:
        plan = _preflight_tokens.pop(token, None)
    if plan is None:
        raise ValueError("预检令牌不存在或已经使用")
    if time.time() > float(plan["expires_at"]):
        raise ValueError("预检令牌已过期，请重新预检")
    confirmation = str(payload.get("confirmation", ""))
    if not hmac.compare_digest(confirmation, str(plan["confirmation_code"])):
        raise ValueError("四位数字确认码不匹配；该预检已作废，请重新预检")
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate CrossEx API 未配置")
    client = GateCrossExClient(credentials)
    symbols = [leg["symbol"] for leg in plan["legs"]]
    reads = {
        "account": lambda: client.get(
            "/crossex/accounts", timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS
        ),
        "positions": lambda: client.get(
            "/crossex/positions", timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS
        ),
        "rules": lambda: _load_rule_rows(
            client, timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS, symbols=symbols
        ),
        "mark_prices": lambda: _load_tickers(
            client,
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
            max_age=0,
            symbols=symbols,
        ),
    }
    is_reduce_only = plan["legs"][0]["reduce_only"] == "true"
    if not is_reduce_only:
        reads["risk_limits"] = lambda: client.get(
            "/crossex/rule/risk_limits",
            params={"symbols": ",".join(symbols)},
            timeout=PREFLIGHT_GATE_TIMEOUT_SECONDS,
        )
    snapshot: dict[str, Any] = {}
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="fc4-submit-check") as pool:
        futures = {pool.submit(read): name for name, read in reads.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                snapshot[name] = future.result()
            except Exception as exc:
                failures.append(f"{name}: {type(exc).__name__}: {exc}")
    if failures:
        raise RuntimeError("提交前 Gate 实时复核失败：" + "; ".join(failures))

    account = normalize_account(snapshot["account"])
    rule_rows = snapshot["rules"]
    rules = {str(row.get("symbol", "")).upper(): row for row in rule_rows}
    prices = snapshot["mark_prices"]
    positions_payload = snapshot["positions"]
    positions_now = _signed_position_map(positions_payload)
    position_values = _position_value_map(positions_payload)
    fresh_legs = validate_pair_request(
        {"legs": plan["legs"], "reduce_only": plan["legs"][0]["reduce_only"] == "true"},
        rules,
        account,
        prices,
    )
    if not is_reduce_only:
        _attach_order_leverage_policy(
            fresh_legs,
            _risk_limit_tiers_by_symbol(snapshot["risk_limits"]),
            position_values,
            reduce_only=False,
        )
    if [(_order_request(item)["symbol"], item["side"], item["qty"]) for item in fresh_legs] != [
        (_order_request(item)["symbol"], item["side"], item["qty"]) for item in plan["legs"]
    ]:
        raise ValueError("提交前交易计划发生变化，请重新预检")
    if not is_reduce_only and [item["target_leverage"] for item in fresh_legs] != [
        item["target_leverage"] for item in plan["legs"]
    ]:
        raise ValueError("提交前合约杠杆上限发生变化，请重新预检")
    if plan["legs"][0]["reduce_only"] == "true":
        _validate_reduce_only_positions(plan["legs"], positions_now)
    before = {key: Decimal(str(value)) for key, value in plan["positions_before"].items()}
    if positions_now != before:
        raise ValueError("预检后真实仓位已变化，请重新预检")
    if not is_reduce_only:
        leverage_items = [
            {
                "symbol": leg["symbol"],
                "target_leverage": leg["target_leverage"],
            }
            for leg in plan["legs"]
        ]
        _set_and_verify_leverages(client, leverage_items)
    task_id = "fc4-" + uuid.uuid4().hex[:20]
    _journal_upsert(
        task_id,
        {
            "state": "submitting",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "legs": plan["legs"],
            "positions_before": plan["positions_before"],
        },
    )
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(client.post, "/crossex/orders", _order_request(leg)): leg
            for leg in plan["legs"]
        }
        for future in as_completed(futures):
            leg = futures[future]
            try:
                response = future.result()
                results.append(_reconcile_order(client, leg, response=response))
            except Exception as exc:  # submission result may be unknown; never retry here
                results.append(_reconcile_order(client, leg, submit_error=str(exc)))
    all_accepted = all(item["accepted"] for item in results)
    states = [str(item.get("reconcile_state", "UNKNOWN")).upper() for item in results]
    positions_after = _signed_position_map(client.get("/crossex/positions"))
    deltas_match = _position_deltas_match(plan["legs"], before, positions_after)
    both_filled = len(states) == 2 and all(state == "FILLED" for state in states) and deltas_match
    any_terminal_failure = any(
        state in {"FAIL", "REJECT", "REJECTED", "CANCELLED", "CANCELED"}
        for state in states
    )
    final_state = "filled" if both_filled else (
        "manual_reconcile_required"
        if not both_filled
        else "submitted_pending_reconcile"
    )
    guard_id = _register_guard(plan["legs"], positions_after) if both_filled else ""
    _journal_upsert(
        task_id,
        {
            "state": final_state,
            "results": results,
            "positions_after": {key: format(value, "f") for key, value in positions_after.items()},
            "position_deltas_match": deltas_match,
            "guard_id": guard_id,
        },
    )
    return {
        "task_id": task_id,
        "guard_id": guard_id,
        "submitted": all_accepted,
        "state": final_state,
        "results": results,
        "message": (
            "两腿均已由 Gate 确认为 FILLED。"
            if both_filled
            else "未确认两腿均已成交；禁止自动重发或回滚，请继续核对订单和真实仓位。"
        ),
    }


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "items", "list", "positions", "accounts", "orders"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return [payload]
    return []


def _canonical_asset(symbol: str) -> str:
    text = symbol.upper().replace("-", "_").replace("/", "_")
    parts = [part for part in text.split("_") if part]
    structural = {"PERP", "FUTURE", "SPOT", "MARGIN", "OPTION", "USDT", "USDC", "USD"}
    structural.update(venue.upper() for venue in CROSSEX_VENUES)
    base = next((p for p in parts if p not in structural), "UNKNOWN")
    return re.sub(r"^(1000000|100000|10000|1000)", "", base) or base


def _asset_variants(asset: Any) -> set[str]:
    """Return FundingCarry/Gate aliases for a canonical asset.

    Gate may expose a multiplier contract as ``1000TOKEN`` while the
    FundingCarry scanner uses ``TOKEN`` (or the reverse).  The alias set lets
    us match those two representations without broadening the universe to
    every Gate instrument.
    """
    text = str(asset or "").strip().upper().replace("-", "_").replace("/", "_")
    if not text:
        return set()
    variants = {text}
    stripped = re.sub(r"^(1000000|100000|10000|1000)", "", text)
    if stripped:
        variants.add(stripped)
    return variants


def fundingcarry_trade_assets() -> dict[str, str]:
    """Load the first-version FundingCarry scanner universe.

    The trade menu must follow the main FundingCarry page, rather than the
    much larger Gate CrossEx rule catalogue.  The scanner response is already
    filtered to the supported two-leg strategy universe and has a persisted
    stale-cache fallback for temporary source outages.
    """
    global _trade_asset_cache
    with _trade_asset_lock:
        now = time.monotonic()
        if _trade_asset_cache[1] and now - _trade_asset_cache[0] <= TRADE_ASSET_CACHE_SECONDS:
            return dict(_trade_asset_cache[1])
        payload = scanner_payload()
        source_key = "rows" if isinstance(payload.get("rows"), list) else "opportunities"
        rows = payload.get(source_key)
        if not isinstance(rows, list):
            raise RuntimeError("FundingCarry 主网币种列表不可用")
        allowed: dict[str, str] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("symbol") or row.get("asset") or "").strip().upper()
            if not asset or asset in EXCLUDED_OPPORTUNITY_ASSETS:
                continue
            for variant in _asset_variants(asset):
                allowed[variant] = asset
        if not allowed:
            raise RuntimeError("FundingCarry 主网币种列表为空")
        _trade_asset_cache = (time.monotonic(), allowed)
        return dict(allowed)


def filter_trade_symbol_rows(rows: list[dict[str, Any]], allowed_assets: dict[str, str]) -> list[dict[str, Any]]:
    """Keep only live FUTURE rules whose asset is in FundingCarry's universe."""
    symbols: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol in seen:
            continue
        if str(row.get("business_type", "")).upper() != "FUTURE":
            continue
        if str(row.get("state", "")).lower() != "live":
            continue
        if str(row.get("support_cross", "")).lower() != "true":
            continue
        parts = symbol.split("_")
        exchange = str(row.get("exchange", "") or (parts[0] if parts else "")).upper()
        base_part = parts[2] if len(parts) >= 4 and parts[1] == "FUTURE" else symbol
        funding_asset = next(
            (allowed_assets[variant] for variant in _asset_variants(base_part) if variant in allowed_assets),
            None,
        )
        if funding_asset is None:
            continue
        symbols.append(
            {
                "symbol": symbol,
                "asset": funding_asset,
                "gate_asset": re.sub(r"^(1000000|100000|10000|1000)", "", base_part) or base_part,
                "exchange": exchange,
                "settle": str(row.get("settle", "") or (parts[-1] if parts else "")).upper(),
            }
        )
        seen.add(symbol)
    symbols.sort(key=lambda item: (item["asset"], item["exchange"], item["symbol"]))
    return symbols


def trade_symbols_payload() -> dict[str, Any]:
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate CrossEx API 未配置")
    client = GateCrossExClient(credentials)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="fc4-symbols") as pool:
        assets_future = pool.submit(fundingcarry_trade_assets)
        rules_future = pool.submit(_load_rule_rows, client)
        allowed_assets = assets_future.result()
        rule_rows = rules_future.result()
    symbols = filter_trade_symbol_rows(rule_rows, allowed_assets)
    return {
        "symbols": symbols,
        "count": len(symbols),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fundingcarry_asset_count": len({item["asset"] for item in symbols}),
        "source": "FundingCarry 主网机会扫描资产列表",
        "note": "仅返回 FundingCarry 主网资产与 Gate CrossEx 规则的交集，且必须同时满足 business_type=FUTURE、state=live。",
    }


def normalize_account(payload: Any) -> dict[str, Any]:
    rows = _rows(payload)
    if not rows:
        raise RuntimeError("Gate CrossEx accounts response has no account row")
    preferred = next(
        (
            row
            for row in rows
            if str(_first(row, "currency", "settle", "coin", default="")).upper()
            in {"USDT", "USDC", "USD"}
        ),
        rows[0],
    )
    assets = preferred.get("assets")
    if not isinstance(assets, list):
        assets = []
        for row in rows:
            nested = row.get("assets")
            if isinstance(nested, list):
                assets.extend(nested)
    venue_assets = []
    for item in assets:
        if not isinstance(item, dict):
            continue
        venue_assets.append(
            {
                "venue": str(_first(item, "exchange", "venue", "platform", default="Gate")),
                "coin": str(_first(item, "coin", "currency", "asset", default="")),
                "balance": _number(_first(item, "balance", "wallet_balance", default=0), 0),
                "equity": _number(_first(item, "equity", "margin_balance", default=0), 0),
                "available": _number(_first(item, "available", "available_margin", default=0), 0),
                "unrealized_pnl": _number(
                    _first(item, "unrealized_pnl", "unrealised_pnl", "unrealised_profit", default=0), 0
                ),
            }
        )
    initial_margin_rate_raw = _number(
        _first(preferred, "initial_margin_rate", default=None)
    )
    maintenance_margin_rate_raw = _number(
        _first(preferred, "maintenance_margin_rate", default=None)
    )
    initial_margin_rate_percent = (
        _number(
            Decimal(str(_first(preferred, "initial_margin_rate")))
            * GATE_MARGIN_RATE_TO_PERCENT
        )
        if initial_margin_rate_raw is not None else None
    )
    maintenance_margin_rate_percent = (
        _number(
            Decimal(str(_first(preferred, "maintenance_margin_rate")))
            * GATE_MARGIN_RATE_TO_PERCENT
        )
        if maintenance_margin_rate_raw is not None else None
    )
    return {
        "currency": str(_first(preferred, "currency", "settle", "coin", default="USDT")),
        "margin_balance": _number(
            _first(preferred, "margin_balance", "equity", "account_equity", default=0), 0
        ),
        "available_margin": _number(
            _first(preferred, "available_margin", "available", "available_balance", default=0), 0
        ),
        "initial_margin": _number(_first(preferred, "initial_margin", "total_initial_margin", default=0), 0),
        "maintenance_margin": _number(
            _first(preferred, "maintenance_margin", "total_maintenance_margin", default=0), 0
        ),
        # Public values are percentage points because the UI appends "%" and
        # Preflight compares against the generic demo threshold. Keep raw ratios for diagnostics.
        "initial_margin_rate": initial_margin_rate_percent,
        "initial_margin_rate_raw": initial_margin_rate_raw,
        "maintenance_margin_rate": maintenance_margin_rate_percent,
        "maintenance_margin_rate_raw": maintenance_margin_rate_raw,
        "unrealized_pnl": _number(
            _first(preferred, "unrealized_pnl", "unrealised_pnl", default=0), 0
        ),
        "account_mode": str(_first(preferred, "account_mode", default="")),
        "position_mode": str(_first(preferred, "position_mode", default="")),
        "assets": venue_assets,
    }


def normalize_positions(payload: Any) -> list[dict[str, Any]]:
    positions = []
    for row in _rows(payload):
        size = _number(
            _first(row, "position_qty", "size", "quantity", "position_size", "position_amt", default=0),
            0,
        ) or 0
        if size == 0:
            continue
        symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default="UNKNOWN"))
        symbol_parts = [part for part in symbol.upper().replace("-", "_").replace("/", "_").split("_") if part]
        explicit_venue = str(_first(row, "exchange", "venue", "platform", default="")).strip()
        inferred_venue = symbol_parts[0] if symbol_parts and _venue_supported(symbol_parts[0]) else "Gate"
        venue = explicit_venue if _venue_supported(explicit_venue) else inferred_venue
        try:
            _, contract_base, _ = _symbol_parts(symbol)
            multiplier = _asset_multiplier(contract_base)
        except ValueError:
            contract_base = next(
                (part for part in symbol_parts if re.match(r"^(1000000|100000|10000|1000).+", part)),
                "",
            )
            multiplier = _asset_multiplier(contract_base) if contract_base else Decimal("1")
        base_exposure = abs(Decimal(str(size))) * multiplier
        side = str(_first(row, "position_side", "side", "direction", default="")).upper()
        if side not in {"LONG", "SHORT"}:
            side = "LONG" if size > 0 else "SHORT"
        positions.append(
            {
                "asset": _canonical_asset(symbol),
                "venue": venue,
                "symbol": symbol,
                "position_id": str(_first(row, "position_id", "id", default="") or ""),
                "created_at_ms": _timestamp_milliseconds(
                    _first(row, "create_time", "created_at", "open_time", default=None)
                ),
                "side": side,
                "quantity": abs(size),
                "contract_multiplier": float(multiplier),
                "base_exposure": float(base_exposure),
                "mark_price": _number(_first(row, "mark_price", "markPrice", default=None)),
                "entry_price": _number(_first(row, "entry_price", "entryPrice", default=None)),
                "notional": abs(_number(_first(row, "position_value", "notional", default=0), 0) or 0),
                "unrealized_pnl": _number(
                    _first(row, "upnl", "unrealized_pnl", "unrealised_pnl", default=0), 0
                ),
                "leverage": _number(_first(row, "leverage", default=None)),
                "raw_updated_at": _first(row, "update_time", "updated_at", "time", default=None),
            }
        )
    return positions


def normalize_ledger(payload: Any) -> list[dict[str, Any]]:
    result = []
    for row in _rows(payload):
        symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default=""))
        parts = [part for part in symbol.upper().replace("-", "_").split("_") if part]
        inferred_venue = parts[0] if parts and parts[0].lower() in CROSSEX_VENUES else "CrossEx"
        explicit_venue = str(_first(row, "exchange", "venue", "platform", default="")).strip()
        # Gate may put the generic route label ("CROSSEX") in exchange_type
        # while the actual underlying venue is encoded in the symbol.
        venue = explicit_venue if _venue_supported(explicit_venue) else inferred_venue
        if venue == "CrossEx":
            venue = str(_first(row, "exchange_type", default="CrossEx"))
        base_asset_source = _first(
            row,
            "base_asset",
            "underlying_asset",
            "underlying",
            "base_currency",
            "base",
            default=None,
        )
        if base_asset_source in (None, ""):
            base_asset_source = symbol or _first(row, "asset", default="")
        base_asset = _canonical_asset(str(base_asset_source)) if base_asset_source else ""
        if base_asset == "UNKNOWN":
            base_asset = ""
        result.append(
            {
                "record_id": _first(row, "id", "record_id", default=None),
                "time": _first(row, "time", "created_at", "timestamp", "create_time", "update_time", default=None),
                "type": str(_first(row, "statement_type", "type", default="OTHER")),
                "venue": venue,
                "asset": symbol or str(_first(row, "coin", "currency", default="")),
                "base_asset": base_asset,
                "amount": _number(_first(row, "amount", "change", "pnl", default=0), 0),
                "currency": str(_first(row, "currency", "coin", "fee_coin", "settle", default="USDT")),
            }
        )
    return result


def _timestamp_milliseconds(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        numeric = Decimal(str(value))
        if numeric.is_finite():
            return int(numeric if abs(numeric) >= Decimal("1e12") else numeric * 1000)
    except (InvalidOperation, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _account_book_page(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next(
            (payload[key] for key in ("data", "rows", "items", "list") if isinstance(payload.get(key), list)),
            None,
        )
        if rows is None:
            raise RuntimeError("Gate account book returned no record list")
    else:
        raise RuntimeError("Gate account book response is not a record list")
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Gate account book contains a malformed record")
    return rows


def load_recent_funding_ledger(
    client: GateCrossExClient,
    *,
    now_ms: int | None = None,
    page_size: int = FUNDING_PAGE_LIMIT,
    max_pages: int = FUNDING_MAX_PAGES,
) -> dict[str, Any]:
    """Read a bounded, timestamp-filtered 24h funding window without mixing partial totals into a full-day result."""
    window_end_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    window_start_ms = window_end_ms - FUNDING_WINDOW_MS
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    exhausted = False
    valid = True
    duplicate_ids = False

    for page in range(1, max_pages + 1):
        payload = client.get(
            "/crossex/account_book",
            {
                "from": window_start_ms,
                "limit": page_size,
                "page": page,
                "statement_type": "FUNDING_FEE",
                "to": window_end_ms,
            },
            timeout=8,
        )
        rows = _account_book_page(payload)
        for row in rows:
            record_type = str(_first(row, "statement_type", "type", default="")).upper()
            timestamp = _timestamp_milliseconds(
                _first(row, "time", "created_at", "timestamp", "create_time", "update_time")
            )
            amount = _first(row, "change", "amount", "pnl")
            currency = _first(row, "coin", "currency", "fee_coin", "settle")
            record_id = _first(row, "id", "record_id")
            try:
                amount_valid = amount is not None and Decimal(str(amount)).is_finite()
            except (InvalidOperation, ValueError):
                amount_valid = False
            if (
                record_type != "FUNDING_FEE"
                or timestamp is None
                or not window_start_ms <= timestamp <= window_end_ms
                or not currency
                or not amount_valid
                or record_id in (None, "")
            ):
                valid = False
                continue
            identity = str(record_id)
            if identity in seen_ids:
                duplicate_ids = True
                continue
            seen_ids.add(identity)
            records.append(row)

        if len(rows) < page_size:
            exhausted = True
            break

    complete = exhausted and valid and not duplicate_ids
    normalized = normalize_ledger(records)
    return {
        "complete": complete,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "record_count": len(records),
        "summary": summarize_funding_ledger(normalized) if complete else [],
        "coverage_note": (
            "Gate 近24小时 FUNDING_FEE 账本完整"
            if complete
            else "账本分页达到读取上限、记录字段异常或分页重复；不展示部分合计，需重新核对"
        ),
    }


def summarize_funding_ledger(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Separate funding receipts and payments without mixing currencies."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if str(row.get("type", "")).upper() != "FUNDING_FEE":
            continue
        venue = str(row.get("venue") or "CrossEx")
        currency = str(row.get("currency") or "USDT").upper()
        key = (venue, currency)
        item = grouped.setdefault(
            key,
            {
                "venue": venue,
                "currency": currency,
                "income": Decimal("0"),
                "expense": Decimal("0"),
                "net": Decimal("0"),
                "entries": 0,
            },
        )
        amount = Decimal(str(row.get("amount") or 0))
        item["income"] += max(amount, Decimal("0"))
        item["expense"] += max(-amount, Decimal("0"))
        item["net"] += amount
        item["entries"] += 1
    return [
        {
            **grouped[key],
            "income": float(grouped[key]["income"]),
            "expense": float(grouped[key]["expense"]),
            "net": float(grouped[key]["net"]),
        }
        for key in sorted(grouped)
    ]


def summarize_daily_fee_funding(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Summarize fees and funding separately by underlying venue and currency."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        record_type = str(row.get("type", "")).upper()
        if record_type not in DAILY_LEDGER_TYPES:
            continue
        venue = str(row.get("venue") or "CrossEx")
        currency = str(row.get("currency") or "").upper()
        if not currency:
            continue
        item = grouped.setdefault(
            (venue, currency),
            {
                "venue": venue,
                "currency": currency,
                "fee_paid": Decimal("0"),
                "fee_credit": Decimal("0"),
                "funding_income": Decimal("0"),
                "funding_expense": Decimal("0"),
                "funding_net": Decimal("0"),
                "fee_entries": 0,
                "funding_entries": 0,
            },
        )
        amount = Decimal(str(row.get("amount") or 0))
        if record_type == "TRADING_FEE":
            item["fee_paid"] += max(-amount, Decimal("0"))
            item["fee_credit"] += max(amount, Decimal("0"))
            item["fee_entries"] += 1
        else:
            item["funding_income"] += max(amount, Decimal("0"))
            item["funding_expense"] += max(-amount, Decimal("0"))
            item["funding_net"] += amount
            item["funding_entries"] += 1
    return [
        {
            **grouped[key],
            **{
                field: float(grouped[key][field])
                for field in ("fee_paid", "fee_credit", "funding_income", "funding_expense", "funding_net")
            },
        }
        for key in sorted(grouped)
    ]


def summarize_daily_funding_net(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize funding-only net income, keeping non-stablecoin currencies separate."""
    stablecoins = {"USD", "USDT", "USDC"}
    by_currency: dict[str, dict[str, Any]] = {}
    by_venue_currency: dict[tuple[str, str], dict[str, Any]] = {}
    stable_income = Decimal("0")
    stable_expense = Decimal("0")
    included_currencies: set[str] = set()
    excluded_currencies: set[str] = set()

    for row in rows:
        if str(_first(row, "type", "statement_type", default="")).upper() != "FUNDING_FEE":
            continue
        currency = str(_first(row, "currency", "coin", "fee_coin", "settle", default="") or "").strip().upper()
        if not currency:
            continue
        try:
            amount = Decimal(str(_first(row, "amount", "change", "pnl", default=0) or 0))
        except (InvalidOperation, ValueError):
            continue
        if not amount.is_finite():
            continue

        income = max(amount, Decimal("0"))
        expense = max(-amount, Decimal("0"))
        item = by_currency.setdefault(
            currency,
            {"currency": currency, "income": Decimal("0"), "expense": Decimal("0"), "entries": 0},
        )
        item["income"] += income
        item["expense"] += expense
        item["entries"] += 1

        venue = str(_first(row, "venue", "exchange", "platform", "exchange_type", default="") or "").strip()
        if not _venue_supported(venue):
            symbol = str(_first(row, "symbol", "contract", "instrument", "inst_id", default=""))
            parts = [part for part in symbol.upper().replace("-", "_").split("_") if part]
            venue = parts[0] if parts and _venue_supported(parts[0]) else "CrossEx"
        venue_item = by_venue_currency.setdefault(
            (venue, currency),
            {
                "venue": venue,
                "currency": currency,
                "income": Decimal("0"),
                "expense": Decimal("0"),
                "entries": 0,
            },
        )
        venue_item["income"] += income
        venue_item["expense"] += expense
        venue_item["entries"] += 1

        if currency in stablecoins:
            included_currencies.add(currency)
            stable_income += income
            stable_expense += expense
        else:
            excluded_currencies.add(currency)

    def serialize(item: dict[str, Any]) -> dict[str, Any]:
        income = item["income"]
        expense = item["expense"]
        return {
            **item,
            "income": float(income),
            "expense": float(expense),
            "net": float(income - expense),
        }

    return {
        "currency": "USDT",
        "assumption": "1 USD = 1 USDT = 1 USDC",
        "income": float(stable_income),
        "expense": float(stable_expense),
        "net": float(stable_income - stable_expense),
        "included_currencies": sorted(included_currencies),
        "excluded_currencies": sorted(excluded_currencies),
        "by_currency": [serialize(by_currency[key]) for key in sorted(by_currency)],
        "by_venue": [serialize(by_venue_currency[key]) for key in sorted(by_venue_currency)],
    }


def summarize_daily_fee_funding_by_asset(
    rows: list[dict[str, Any]], *, group_by_day: bool = False
) -> list[dict[str, Any]]:
    """Summarize fee/funding by asset, venue, currency, optionally at Shanghai-day grain."""
    grouped: dict[tuple[str, ...], dict[str, Any]] = {}
    from zoneinfo import ZoneInfo

    timezone_info = ZoneInfo(DAILY_LEDGER_TIMEZONE) if group_by_day else None
    for row in rows:
        record_type = str(row.get("type", "")).upper()
        if record_type not in DAILY_LEDGER_TYPES:
            continue
        asset = str(row.get("base_asset") or "").strip().upper()
        currency = str(row.get("currency") or "").strip().upper()
        if not asset or not currency:
            continue
        venue = str(row.get("venue") or "CrossEx")
        date_text = None
        if group_by_day:
            timestamp_ms = _timestamp_milliseconds(row.get("time"))
            if timestamp_ms is None:
                continue
            date_text = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone_info).date().isoformat()
        key = (date_text, asset, venue, currency) if date_text is not None else (asset, venue, currency)
        item = grouped.setdefault(key, {
                **({"date": date_text} if date_text is not None else {}),
                "asset": asset,
                "venue": venue,
                "currency": currency,
                "fee_paid": Decimal("0"),
                "fee_credit": Decimal("0"),
                "funding_income": Decimal("0"),
                "funding_expense": Decimal("0"),
                "funding_net": Decimal("0"),
                "total_income": Decimal("0"),
                "total_expense": Decimal("0"),
                "total_net": Decimal("0"),
                "fee_entries": 0,
                "funding_entries": 0,
            })
        amount = Decimal(str(row.get("amount") or 0))
        item["total_income"] += max(amount, Decimal("0"))
        item["total_expense"] += max(-amount, Decimal("0"))
        item["total_net"] += amount
        if record_type == "TRADING_FEE":
            item["fee_paid"] += max(-amount, Decimal("0"))
            item["fee_credit"] += max(amount, Decimal("0"))
            item["fee_entries"] += 1
        else:
            item["funding_income"] += max(amount, Decimal("0"))
            item["funding_expense"] += max(-amount, Decimal("0"))
            item["funding_net"] += amount
            item["funding_entries"] += 1
    numeric_fields = (
        "fee_paid",
        "fee_credit",
        "funding_income",
        "funding_expense",
        "funding_net",
        "total_income",
        "total_expense",
        "total_net",
    )
    return [
        {
            **grouped[key],
            **{field: float(grouped[key][field]) for field in numeric_fields},
        }
        for key in sorted(grouped)
    ]


def summarize_daily_funding_by_contract(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate settled funding by Beijing day and exact contract symbol."""
    from zoneinfo import ZoneInfo

    timezone_info = ZoneInfo(DAILY_LEDGER_TIMEZONE)
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        if str(row.get("type", "")).upper() != "FUNDING_FEE":
            continue
        symbol = str(row.get("asset") or "").strip().upper()
        venue = str(row.get("venue") or "").strip().upper()
        currency = str(row.get("currency") or "").strip().upper()
        timestamp_ms = _timestamp_milliseconds(row.get("time"))
        if not symbol or not venue or not currency or timestamp_ms is None:
            continue
        date_text = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone_info).date().isoformat()
        key = (date_text, symbol, venue, currency)
        item = grouped.setdefault(
            key,
            {
                "date": date_text,
                "symbol": symbol,
                "venue": venue,
                "currency": currency,
                "funding_net": Decimal("0"),
                "funding_entries": 0,
            },
        )
        item["funding_net"] += Decimal(str(row.get("amount") or 0))
        item["funding_entries"] += 1
    return [
        {**grouped[key], "funding_net": float(grouped[key]["funding_net"])}
        for key in sorted(grouped)
    ]


EXIT_ADVISORY_THRESHOLDS_PCT = {7: 0.10, 14: 0.20}
EXIT_ADVISORY_STABLECOINS = {"USD", "USDT", "USDC"}


def build_exit_advisories(
    positions: list[dict[str, Any]],
    contract_funding_rows: list[dict[str, Any]],
    *,
    ledger_complete: bool,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Build read-only exit suggestions from complete, settled funding history."""
    from zoneinfo import ZoneInfo

    local_today = today or datetime.now(ZoneInfo(DAILY_LEDGER_TIMEZONE)).date()
    last_completed_day = local_today - timedelta(days=1)
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for position in positions:
        by_asset.setdefault(str(position.get("asset") or "UNKNOWN").upper(), []).append(position)

    advisories: list[dict[str, Any]] = []
    for asset, legs in sorted(by_asset.items()):
        base: dict[str, Any] = {
            "asset": asset,
            "state": "insufficient_data",
            "state_label": "数据不足",
            "legs": [
                {
                    "venue": str(leg.get("venue") or "未知平台"),
                    "symbol": str(leg.get("symbol") or ""),
                    "side": str(leg.get("side") or "UNKNOWN"),
                    "notional": float(leg.get("notional") or 0),
                    "position_id": str(leg.get("position_id") or ""),
                    "created_at_ms": leg.get("created_at_ms"),
                }
                for leg in legs
            ],
            "reference_notional": None,
            "negative_funding_streak_days": 0,
            "seven_day": {"ready": False, "threshold_pct": EXIT_ADVISORY_THRESHOLDS_PCT[7]},
            "fourteen_day": {"ready": False, "threshold_pct": EXIT_ADVISORY_THRESHOLDS_PCT[14]},
            "reasons": [],
            "data_note": "",
        }

        if not ledger_complete:
            base["data_note"] = "Gate 资金费账本未完整读取；本币种不作退出建议。"
            advisories.append(base)
            continue
        if len(legs) != 2 or {str(leg.get("side", "")).upper() for leg in legs} != {"LONG", "SHORT"}:
            base["data_note"] = "当前持仓不是恰好两条反向腿，无法可靠配对计算。"
            advisories.append(base)
            continue

        open_dates: list[date] = []
        timestamps_valid = True
        for leg in legs:
            timestamp_ms = leg.get("created_at_ms")
            parsed_ms = _timestamp_milliseconds(timestamp_ms)
            if parsed_ms is None:
                timestamps_valid = False
                break
            open_dates.append(datetime.fromtimestamp(parsed_ms / 1000, tz=ZoneInfo(DAILY_LEDGER_TIMEZONE)).date())
        if not timestamps_valid:
            base["data_note"] = "Gate 未提供当前两腿的可信创建时间，无法排除旧仓记录。"
            advisories.append(base)
            continue

        long_notional = sum(float(leg.get("notional") or 0) for leg in legs if str(leg.get("side", "")).upper() == "LONG")
        short_notional = sum(float(leg.get("notional") or 0) for leg in legs if str(leg.get("side", "")).upper() == "SHORT")
        reference_notional = min(long_notional, short_notional)
        if reference_notional <= 0:
            base["data_note"] = "当前配对名义价值缺失或为零，不能计算百分比门槛。"
            advisories.append(base)
            continue
        base["reference_notional"] = reference_notional
        pair_open_date = max(open_dates)
        symbols = {str(leg.get("symbol") or "").upper() for leg in legs}
        daily_net: dict[date, Decimal] = {}
        unsupported_currencies: set[str] = set()
        for row in contract_funding_rows:
            if str(row.get("symbol") or "").upper() not in symbols:
                continue
            currency = str(row.get("currency") or "").upper()
            row_day_text = str(row.get("date") or "")
            try:
                row_day = date.fromisoformat(row_day_text)
            except ValueError:
                continue
            if row_day <= pair_open_date or row_day > last_completed_day:
                continue
            if currency not in EXIT_ADVISORY_STABLECOINS:
                if int(row.get("funding_entries") or 0) > 0:
                    unsupported_currencies.add(currency or "未知币种")
                continue
            daily_net[row_day] = daily_net.get(row_day, Decimal("0")) + Decimal(str(row.get("funding_net") or 0))

        if unsupported_currencies:
            base["data_note"] = "发现非 USD/USDT/USDC 结算资金费（" + "、".join(sorted(unsupported_currencies)) + "），当前无法按 1:1 假设可靠折算。"
            advisories.append(base)
            continue

        def period_summary(days: int) -> dict[str, Any]:
            start_day = last_completed_day - timedelta(days=days - 1)
            ready = pair_open_date < start_day
            net = sum((daily_net.get(start_day + timedelta(days=index), Decimal("0")) for index in range(days)), Decimal("0"))
            return {
                "ready": ready,
                "start_date": start_day.isoformat(),
                "end_date": last_completed_day.isoformat(),
                "net_usdt": float(net),
                "yield_pct": float(net / Decimal(str(reference_notional)) * Decimal("100")),
                "threshold_pct": EXIT_ADVISORY_THRESHOLDS_PCT[days],
                "below_threshold": ready and net / Decimal(str(reference_notional)) * Decimal("100") < Decimal(str(EXIT_ADVISORY_THRESHOLDS_PCT[days])),
            }

        seven_day = period_summary(7)
        fourteen_day = period_summary(14)
        streak = 0
        for offset in range(3):
            day = last_completed_day - timedelta(days=offset)
            if day <= pair_open_date or daily_net.get(day, Decimal("0")) >= 0:
                break
            streak += 1
        reasons = []
        if streak >= 3:
            reasons.append("连续3个已结束的北京时间自然日资金费净额为负")
        if seven_day["below_threshold"]:
            reasons.append(f"近7日资金费净收益率低于 {seven_day['threshold_pct']:.2f}%")
        if fourteen_day["below_threshold"]:
            reasons.append(f"近14日资金费净收益率低于 {fourteen_day['threshold_pct']:.2f}%")
        base.update(
            {
                "state": "exit_review" if reasons else "normal",
                "state_label": "建议评估退出" if reasons else "未触发",
                "negative_funding_streak_days": streak,
                "seven_day": seven_day,
                "fourteen_day": fourteen_day,
                "reasons": reasons,
                "data_note": (
                    f"以当前两腿较小名义价值 {reference_notional:.8f} U 折算；仅统计当前合约创建日之后的完整自然日。"
                ),
            }
        )
        advisories.append(base)
    return advisories


def summarize_daily_stablecoin_net(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Convert USD, USDT, and USDC entries at the explicitly assumed 1:1 rate."""
    included = {"USD", "USDT", "USDC"}
    income = Decimal("0")
    expense = Decimal("0")
    included_currencies: set[str] = set()
    excluded_currencies: set[str] = set()
    for row in rows:
        record_type = str(_first(row, "statement_type", "type", default="")).upper()
        if record_type not in DAILY_LEDGER_TYPES:
            continue
        currency = str(_first(row, "coin", "currency", "fee_coin", "settle", default="")).upper()
        if currency not in included:
            if currency:
                excluded_currencies.add(currency)
            continue
        try:
            amount = Decimal(str(_first(row, "change", "amount", "pnl", default="0")))
        except (InvalidOperation, ValueError):
            continue
        if not amount.is_finite():
            continue
        included_currencies.add(currency)
        income += max(amount, Decimal("0"))
        expense += max(-amount, Decimal("0"))
    return {
        "currency": "USDT",
        "assumption": "1 USD = 1 USDT = 1 USDC",
        "income": float(income),
        "expense": float(expense),
        "net": float(income - expense),
        "included_currencies": sorted(included_currencies),
        "excluded_currencies": sorted(excluded_currencies),
    }


def load_daily_fee_funding_ledger(
    client: GateCrossExClient,
    date_text: str,
    end_date_text: str | None = None,
    *,
    page_size: int = DAILY_LEDGER_PAGE_LIMIT,
    max_pages: int = DAILY_LEDGER_MAX_PAGES,
) -> dict[str, Any]:
    """Read a complete bounded Shanghai date range of trading-fee and funding entries."""
    if not isinstance(date_text, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
        raise ValueError("日期必须使用 YYYY-MM-DD 格式")
    try:
        selected_date = date.fromisoformat(date_text)
    except ValueError as exc:
        raise ValueError("日期无效") from exc
    if end_date_text is None:
        selected_end_date = selected_date
    else:
        if not isinstance(end_date_text, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_date_text):
            raise ValueError("结束日期必须使用 YYYY-MM-DD 格式")
        try:
            selected_end_date = date.fromisoformat(end_date_text)
        except ValueError as exc:
            raise ValueError("结束日期无效") from exc
    if selected_end_date < selected_date:
        raise ValueError("结束日期不能早于开始日期")
    if (selected_end_date - selected_date).days >= 31:
        raise ValueError("单次最多查询连续 31 个自然日")
    if page_size <= 0 or page_size > DAILY_LEDGER_PAGE_LIMIT or max_pages <= 0:
        raise ValueError("账本分页参数无效")

    from zoneinfo import ZoneInfo

    timezone_info = ZoneInfo(DAILY_LEDGER_TIMEZONE)
    if selected_end_date == date.max:
        raise ValueError("结束日期超出支持范围")
    start = datetime.combine(selected_date, datetime.min.time(), tzinfo=timezone_info)
    end = datetime.combine(selected_end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone_info)
    window_start_ms = int(start.timestamp() * 1000)
    window_end_ms = int(end.timestamp() * 1000)
    queried_at_ms = int(time.time() * 1000)
    current_local_date = datetime.fromtimestamp(queried_at_ms / 1000, tz=timezone_info).date()
    if selected_end_date > current_local_date:
        raise ValueError("不能查询未来日期")
    query_end_ms = min(window_end_ms - 1, queried_at_ms)

    def read_statement_type(statement_type: str) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        exhausted = False
        valid = True
        duplicate_ids = False
        pages_read = 0
        for page in range(1, max_pages + 1):
            payload = client.get(
                "/crossex/account_book",
                {
                    "from": window_start_ms,
                    "limit": page_size,
                    "page": page,
                    "statement_type": statement_type,
                    "to": query_end_ms,
                },
                timeout=8,
            )
            rows = _account_book_page(payload)
            pages_read += 1
            for row in rows:
                record_type = str(_first(row, "statement_type", "type", default="")).upper()
                timestamp = _timestamp_milliseconds(
                    _first(row, "time", "created_at", "timestamp", "create_time", "update_time")
                )
                amount = _first(row, "change", "amount", "pnl")
                currency = _first(row, "coin", "currency", "fee_coin", "settle")
                record_id = _first(row, "id", "record_id")
                try:
                    amount_valid = amount is not None and Decimal(str(amount)).is_finite()
                except (InvalidOperation, ValueError):
                    amount_valid = False
                if (
                    record_type != statement_type
                    or timestamp is None
                    or not window_start_ms <= timestamp < window_end_ms
                    or timestamp > queried_at_ms
                    or not currency
                    or not amount_valid
                    or record_id in (None, "")
                ):
                    valid = False
                    continue
                identity = str(record_id)
                if identity in seen_ids:
                    duplicate_ids = True
                    continue
                seen_ids.add(identity)
                records.append(row)
            if len(rows) < page_size:
                exhausted = True
                break
        return {
            "records": records,
            "complete": exhausted and valid and not duplicate_ids,
            "pages_read": pages_read,
            "record_count": len(records),
        }

    # Fee and funding are independent Gate filters; fetch them concurrently to
    # avoid making the daily view wait for two serial round trips.
    with ThreadPoolExecutor(max_workers=len(DAILY_LEDGER_TYPES)) as pool:
        futures = {pool.submit(read_statement_type, kind): kind for kind in DAILY_LEDGER_TYPES}
        results = {futures[future]: future.result() for future in as_completed(futures)}

    records = [row for kind in DAILY_LEDGER_TYPES for row in results[kind]["records"]]
    complete = all(results[kind]["complete"] for kind in DAILY_LEDGER_TYPES)
    normalized = normalize_ledger(records)
    normalized.sort(key=lambda row: _timestamp_milliseconds(row.get("time")) or 0, reverse=True)
    period_closed = selected_end_date < current_local_date
    period_label = date_text if selected_end_date == selected_date else f"{date_text} — {selected_end_date.isoformat()}"
    return {
        "date": date_text if selected_end_date == selected_date else None,
        "start_date": date_text,
        "end_date": selected_end_date.isoformat(),
        "period_label": period_label,
        "timezone": DAILY_LEDGER_TIMEZONE,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "as_of_ms": queried_at_ms,
        "day_closed": period_closed,
        "period_closed": period_closed,
        "complete": complete,
        "record_count": len(records),
        "details_truncated": len(normalized) > DAILY_LEDGER_DETAIL_LIMIT,
        "pages_read": {kind: results[kind]["pages_read"] for kind in DAILY_LEDGER_TYPES},
        "type_record_counts": {kind: results[kind]["record_count"] for kind in DAILY_LEDGER_TYPES},
        "summary": summarize_daily_fee_funding(normalized) if complete else [],
        "funding_net_summary": summarize_daily_funding_net(records) if complete else None,
        "asset_summary": summarize_daily_fee_funding_by_asset(normalized) if complete else [],
        "daily_asset_summary": summarize_daily_fee_funding_by_asset(normalized, group_by_day=True) if complete else [],
        "contract_funding_summary": summarize_daily_funding_by_contract(normalized) if complete else [],
        "unattributed_record_count": sum(
            1
            for row in normalized
            if str(row.get("type", "")).upper() in DAILY_LEDGER_TYPES
            and not row.get("base_asset")
        ),
        "stablecoin_net": summarize_daily_stablecoin_net(records) if complete else None,
        "records": normalized[:DAILY_LEDGER_DETAIL_LIMIT],
        "coverage_note": (
            (
                f"{period_label}（{DAILY_LEDGER_TIMEZONE}）手续费与资金费账本完整"
                if period_closed
                else f"{period_label}（{DAILY_LEDGER_TIMEZONE}）数据截至查询时间，所选周期尚未结束"
            )
            if complete
            else "分页未读完、记录字段异常或重复；为避免少算，不提供所选周期合计"
        ),
    }


def daily_fee_funding_payload(date_text: str, end_date_text: str | None = None) -> dict[str, Any]:
    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate API 未配置，无法读取每日账本")
    return load_daily_fee_funding_ledger(GateCrossExClient(credentials), date_text, end_date_text)


def exit_advisories_payload(*, today: date | None = None) -> dict[str, Any]:
    """Return daily read-only exit suggestions for currently open matched pairs."""
    from zoneinfo import ZoneInfo

    credentials = load_credentials()
    if credentials is None:
        raise RuntimeError("Gate API 未配置，无法计算持仓退出建议")
    current_day = today or datetime.now(ZoneInfo(DAILY_LEDGER_TIMEZONE)).date()
    end_day = current_day - timedelta(days=1)
    start_day = end_day - timedelta(days=13)
    client = GateCrossExClient(credentials)
    with ThreadPoolExecutor(max_workers=2) as pool:
        positions_future = pool.submit(
            client.get, "/crossex/positions", None, 8
        )
        ledger_future = pool.submit(
            load_daily_fee_funding_ledger,
            client,
            start_day.isoformat(),
            end_day.isoformat(),
        )
        positions_payload = positions_future.result()
        ledger = ledger_future.result()
    if isinstance(positions_payload, list):
        if any(not isinstance(row, dict) for row in positions_payload):
            raise RuntimeError("Gate 当前仓位响应包含无效记录；不生成退出建议")
    elif not (
        isinstance(positions_payload, dict)
        and any(isinstance(positions_payload.get(key), list) for key in ("data", "rows", "items", "list", "positions"))
    ):
        raise RuntimeError("Gate 当前仓位响应结构无效；不生成退出建议")
    positions = normalize_positions(positions_payload)
    advisories = build_exit_advisories(
        positions,
        ledger.get("contract_funding_summary", []),
        ledger_complete=bool(ledger.get("complete")),
        today=current_day,
    )
    return {
        "complete": bool(ledger.get("complete")),
        "timezone": DAILY_LEDGER_TIMEZONE,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "as_of": datetime.now(timezone.utc).isoformat(),
        "thresholds_pct": {str(days): value for days, value in EXIT_ADVISORY_THRESHOLDS_PCT.items()},
        "coverage_note": ledger.get("coverage_note"),
        "advisories": advisories,
    }


def scanner_payload() -> dict[str, Any]:
    errors = []
    for url in SCANNER_URLS:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "fundingcarry-v4-local/0.1"})
        # The local scanner is an opportunistic fast path. Bound its wait so
        # the remote FundingCarry fallback is not delayed by a dead localhost
        # port; keep a longer but finite budget for the remote source.
        timeout = 2 if url.startswith("http://127.0.0.1:") else 8
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("response is not an object")
            payload["proxied_source"] = urlparse(url).netloc
            filtered = filter_crossex_opportunities(payload)
            filtered["stale"] = False
            filtered["scanner_errors"] = []
            with _scanner_cache_lock:
                SCANNER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
                SCANNER_CACHE_PATH.write_text(
                    json.dumps(filtered, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8",
                )
            return filtered
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{urlparse(url).netloc}: {type(exc).__name__}")
    with _scanner_cache_lock:
        try:
            cached = json.loads(SCANNER_CACHE_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            cached = None
    if isinstance(cached, dict) and isinstance(cached.get("rows"), list):
        cached["stale"] = True
        cached["scanner_errors"] = errors
        cached["note"] = str(cached.get("note", "")) + " 当前为上次成功快照，请待数据源恢复后刷新。"
        return cached
    raise RuntimeError("; ".join(errors) or "FundingCarry scanner unavailable")


def dashboard_payload() -> dict[str, Any]:
    credentials = load_credentials()
    if credentials is None:
        now_ms = int(time.time() * 1000)
        return {
            "connected": False,
            "configured": False,
            "read_only": not live_trading_enabled(),
            "trading_interface": True,
            "live_trading_enabled": live_trading_enabled(),
            "message": "Gate CrossEx API 未配置",
            "daily_funding": {
                "complete": False,
                "window_start_ms": now_ms - FUNDING_WINDOW_MS,
                "window_end_ms": now_ms,
                "record_count": None,
                "summary": [],
                "error": "Gate API 未配置",
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    client = GateCrossExClient(credentials)
    errors: dict[str, str] = {}
    now_ms = int(time.time() * 1000)
    daily_funding: dict[str, Any] = {
        "complete": False,
        "window_start_ms": now_ms - FUNDING_WINDOW_MS,
        "window_end_ms": now_ms,
        "record_count": None,
        "summary": [],
    }
    readers = {
        "account": lambda: normalize_account(client.get("/crossex/accounts")),
        "positions": lambda: normalize_positions(client.get("/crossex/positions")),
        "ledger": lambda: normalize_ledger(client.get("/crossex/account_book", {"limit": 100})),
        "daily_funding": lambda: load_recent_funding_ledger(client, now_ms=now_ms),
    }
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(readers)) as pool:
        futures = {pool.submit(reader): name for name, reader in readers.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except RuntimeError as exc:
                error_key = "funding_24h" if name == "daily_funding" else name
                errors[error_key] = str(exc)
                if name == "daily_funding":
                    daily_funding["error"] = str(exc)
    account = results.get("account")
    positions = results.get("positions", [])
    ledger = results.get("ledger", [])
    daily_funding = results.get("daily_funding", daily_funding)
    connected = account is not None and "positions" not in errors
    guards = _json_store_read(GUARD_STATE_PATH, {})
    if not isinstance(guards, dict):
        guards = {}
    return {
        "connected": connected,
        "configured": True,
        "read_only": not live_trading_enabled(),
        "trading_interface": True,
        "live_trading_enabled": live_trading_enabled(),
        "credential_source": "environment" if credentials.source == "process environment" else "restricted file",
        "key_fingerprint": hashlib.sha256(credentials.key.encode("utf-8")).hexdigest()[:10],
        "account": account,
        "positions": positions,
        "ledger": ledger,
        "funding_summary": summarize_funding_ledger(ledger),
        "daily_funding": daily_funding,
        "guards": list(guards.values()),
        "errors": errors,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _recover_pending_executions(client: GateCrossExClient) -> None:
    with _execution_lock:
        tasks = _json_store_read(EXECUTION_JOURNAL_PATH, {})
    if not isinstance(tasks, dict):
        return
    pending = [row for row in tasks.values() if isinstance(row, dict) and row.get("state") == "submitting"]
    if not pending:
        return
    positions = _signed_position_map(client.get("/crossex/positions"))
    for task in pending:
        legs = task.get("legs")
        before_raw = task.get("positions_before")
        if not isinstance(legs, list) or not isinstance(before_raw, dict):
            _journal_upsert(task["task_id"], {"state": "manual_reconcile_required", "reason": "invalid journal"})
            continue
        results = [_reconcile_order(client, leg) for leg in legs]
        before = {key: Decimal(str(value)) for key, value in before_raw.items()}
        filled = all(item["reconcile_state"] == "FILLED" for item in results)
        matched = _position_deltas_match(legs, before, positions)
        if filled and matched:
            guard_id = _register_guard(legs, positions)
            _journal_upsert(task["task_id"], {"state": "filled", "results": results, "guard_id": guard_id})
        else:
            _journal_upsert(
                task["task_id"],
                {"state": "manual_reconcile_required", "results": results, "position_deltas_match": matched},
            )


def _recover_acting_guard(
    client: GateCrossExClient,
    guard_id: str,
    guard: dict[str, Any],
) -> None:
    action = guard.get("pending_action")
    before_raw = guard.get("pending_before")
    if not isinstance(action, dict) or not isinstance(before_raw, dict):
        _guard_upsert(
            guard_id,
            {
                "state": "manual_reconcile_required",
                "reason": "acting guard is missing persisted action or pre-submit positions",
            },
        )
        return
    try:
        order = client.get(f"/crossex/orders/{quote(str(action.get('text', '')), safe='')}")
    except Exception as exc:
        _guard_upsert(
            guard_id,
            {
                "state": "acting",
                "reason": f"pending guard order lookup failed: {exc}",
            },
        )
        return
    state = str(_first(order, "state", "status", default="UNKNOWN")).upper()
    if state in {"NEW", "OPEN", "PARTIALLY_FILLED", "PENDING", "UNKNOWN"}:
        _guard_upsert(
            guard_id,
            {
                "state": "acting",
                "reason": f"pending guard order remains {state}; no duplicate submitted",
                "last_order": order,
            },
        )
        return
    if state != "FILLED":
        _guard_upsert(
            guard_id,
            {
                "state": "manual_reconcile_required",
                "reason": f"pending guard order ended as {state}",
                "last_order": order,
            },
        )
        return
    try:
        after = _signed_position_map(client.get("/crossex/positions"))
    except Exception as exc:
        _guard_upsert(
            guard_id,
            {
                "state": "acting",
                "reason": f"guard order filled but positions are unreadable: {exc}",
                "last_order": order,
            },
        )
        return
    before = {key: Decimal(str(value)) for key, value in before_raw.items()}
    if not _guard_action_delta_matches(action, before, after):
        _guard_upsert(
            guard_id,
            {
                "state": "manual_reconcile_required",
                "reason": "guard order is FILLED but the authoritative position delta does not match",
                "last_order": order,
                "positions_after": {key: format(value, "f") for key, value in after.items()},
            },
        )
        return
    symbols = [str(leg["symbol"]) for leg in guard.get("legs", [])]
    _guard_upsert(
        guard_id,
        {
            "state": "active",
            "reason": "",
            "baseline": {symbol: format(after.get(symbol, Decimal("0")), "f") for symbol in symbols},
            "last_order": order,
            "pending_action": None,
            "pending_before": None,
            "observation": None,
            "observation_count": 0,
        },
    )


def _guard_cycle(client: GateCrossExClient) -> None:
    with _trade_guard_coordination_lock:
        _guard_cycle_coordinated(client)


def _guard_cycle_coordinated(client: GateCrossExClient) -> None:
    _recover_pending_executions(client)
    with _guard_lock:
        guards = _json_store_read(GUARD_STATE_PATH, {})
    if not isinstance(guards, dict) or not guards:
        return
    # A prior version paused a guard permanently when a small imbalance could
    # not yet meet the venue's minimum order size. That is not an unknown
    # submission: no order was sent. Resume only this exact no-pending-order
    # case as a monitored wait, preserving its original balanced baseline.
    for guard_id, guard in guards.items():
        if (
            isinstance(guard, dict)
            and guard.get("state") == "manual_reconcile_required"
            and guard.get("reason") == GUARD_BELOW_MINIMUM_REASON
            and not guard.get("pending_action")
            and not guard.get("pending_before")
        ):
            _guard_upsert(
                guard_id,
                {
                    "state": "below_minimum_waiting",
                    "reason": GUARD_BELOW_MINIMUM_REASON,
                    "observation": None,
                    "observation_count": 0,
                },
            )
    # An already-submitted guard order must be reconciled before any new
    # market/rule reads or imbalance decisions.  Never downgrade ``acting``
    # to a generic remote_unknown state: doing so could forget that an order
    # is in flight and allow a duplicate reduction after connectivity returns.
    for guard_id, guard in guards.items():
        if isinstance(guard, dict) and guard.get("state") == "acting":
            _recover_acting_guard(client, guard_id, guard)
    with _guard_lock:
        guards = _json_store_read(GUARD_STATE_PATH, {})
    if not isinstance(guards, dict):
        return
    try:
        tracked_symbols = sorted(
            {
                str(leg.get("symbol", "")).upper()
                for guard in guards.values()
                if isinstance(guard, dict)
                and guard.get("state") not in {"paused", "manual_reconcile_required"}
                for leg in guard.get("legs", [])
                if isinstance(leg, dict) and leg.get("symbol")
            }
        )
        if not tracked_symbols:
            raise RuntimeError("active guard has no tracked contract symbols")
        positions = _signed_position_map(client.get("/crossex/positions"))
        rules = {
            str(row.get("symbol", "")).upper(): row
            for row in _load_rule_rows(client, symbols=tracked_symbols)
        }
        prices = _load_tickers(client, symbols=tracked_symbols)
    except Exception as exc:
        for guard_id, guard in guards.items():
            if isinstance(guard, dict) and guard.get("state") not in {
                "paused", "manual_reconcile_required", "acting"
            }:
                _guard_upsert(guard_id, {"state": "remote_unknown", "reason": str(exc)})
        return
    for guard_id, guard in guards.items():
        if not isinstance(guard, dict) or guard.get("state") in {"paused", "manual_reconcile_required"}:
            continue
        if guard.get("state") == "acting":
            continue
        decision = guard_decision(guard, positions, rules, prices)
        state = decision["state"]
        if state == "healthy":
            _guard_upsert(guard_id, {"state": "active", "reason": "", "observation": None, "observation_count": 0})
            continue
        if state == "below_minimum_waiting":
            # Keep reading authoritative positions. Do not reduce an amount
            # larger than the confirmed exposure mismatch just to satisfy a
            # venue minimum; once the accumulated mismatch is executable, the
            # normal two-observation confirmation path will submit it.
            _guard_upsert(
                guard_id,
                {
                    "state": "below_minimum_waiting",
                    "reason": decision.get("reason", GUARD_BELOW_MINIMUM_REASON),
                    "observation": None,
                    "observation_count": 0,
                },
            )
            continue
        if state in {"remote_unknown", "manual_reconcile_required"}:
            _guard_upsert(guard_id, {"state": state, "reason": decision.get("reason", "")})
            continue
        fingerprint = json.dumps(decision, sort_keys=True, separators=(",", ":"))
        count = int(guard.get("observation_count", 0)) + 1 if guard.get("observation") == fingerprint else 1
        if count < 2:
            _guard_upsert(
                guard_id,
                {"state": "confirming", "observation": fingerprint, "observation_count": count},
            )
            continue
        if state == "baseline_update":
            symbols = [str(leg["symbol"]) for leg in guard["legs"]]
            _guard_upsert(
                guard_id,
                {
                    "state": "active",
                    "baseline": {symbol: format(positions.get(symbol, Decimal("0")), "f") for symbol in symbols},
                    "observation": None,
                    "observation_count": 0,
                },
            )
            continue
        if not live_trading_enabled():
            _guard_upsert(guard_id, {"state": "action_required_locked", "pending_action": decision})
            continue
        text_id = "fc4-guard-" + uuid.uuid4().hex[:16]
        order_leg = {
            "text": text_id,
            "symbol": decision["symbol"],
            "side": decision["side"],
            "qty": decision["qty"],
            "type": "MARKET",
            "reduce_only": "true",
        }
        pending_before = {key: format(value, "f") for key, value in positions.items()}
        _guard_upsert(
            guard_id,
            {
                "state": "acting",
                "pending_action": order_leg,
                "pending_before": pending_before,
                "reason": "",
            },
        )
        try:
            response = client.post("/crossex/orders", order_leg)
            result = _reconcile_order(client, order_leg, response=response)
        except Exception as exc:
            result = _reconcile_order(client, order_leg, submit_error=str(exc))
        if result["reconcile_state"] != "FILLED":
            _guard_upsert(
                guard_id,
                {"state": "manual_reconcile_required", "reason": "guard order not confirmed filled", "last_order": result},
            )
            continue
        after = _signed_position_map(client.get("/crossex/positions"))
        before_action = {key: Decimal(str(value)) for key, value in pending_before.items()}
        if not _guard_action_delta_matches(order_leg, before_action, after):
            _guard_upsert(
                guard_id,
                {
                    "state": "manual_reconcile_required",
                    "reason": "guard order is FILLED but the authoritative position delta does not match",
                    "last_order": result,
                    "positions_after": {key: format(value, "f") for key, value in after.items()},
                },
            )
            continue
        symbols = [str(leg["symbol"]) for leg in guard["legs"]]
        _guard_upsert(
            guard_id,
            {
                "state": "active",
                "baseline": {symbol: format(after.get(symbol, Decimal("0")), "f") for symbol in symbols},
                "last_order": result,
                "pending_action": None,
                "pending_before": None,
                "observation": None,
                "observation_count": 0,
            },
        )


def _background_worker() -> None:
    while True:
        credentials = load_credentials()
        if credentials is not None:
            try:
                _guard_cycle(GateCrossExClient(credentials))
            except Exception:
                pass
        time.sleep(1)


class Handler(BaseHTTPRequestHandler):
    server_version = "FundingCarryV4/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Access logs intentionally contain no headers or request bodies.
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self) -> None:
        try:
            body = HTML_PATH.read_bytes()
        except OSError:
            self._json(500, {"error": "fundingcarryv4.html is unavailable"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Content-Length 无效") from exc
        if length <= 0 or length > 32768:
            raise ValueError("请求体必须在 1 至 32768 字节之间")
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("请求体不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return value

    def do_GET(self) -> None:  # noqa: N802
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        if path in {"/", "/fundingcarryv4.html"}:
            self._html()
            return
        if path == "/api/v4/status":
            credentials = load_credentials()
            self._json(
                200,
                {
                    "configured": credentials is not None,
                    "read_only": not live_trading_enabled(),
                    "trading_interface": True,
                    "live_trading_enabled": live_trading_enabled(),
                    "architecture": "single-gate-crossex-api",
                    "supported_venues": sorted(CROSSEX_VENUES),
                },
            )
            return
        if path == "/api/v4/dashboard":
            self._json(200, dashboard_payload())
            return
        if path == "/api/v4/daily-ledger":
            query = parse_qs(parsed_url.query, keep_blank_values=True)
            try:
                selected_date = query.get("start_date", query.get("date", [""]))[0]
                selected_end_date = query.get("end_date", [selected_date])[0]
                self._json(200, daily_fee_funding_payload(selected_date, selected_end_date))
            except ValueError as exc:
                self._json(400, {"error": str(exc)})
            except RuntimeError as exc:
                self._json(502, {"error": str(exc)})
            return
        if path == "/api/v4/exit-advisories":
            try:
                self._json(200, exit_advisories_payload())
            except ValueError as exc:
                self._json(400, {"error": str(exc)})
            except RuntimeError as exc:
                self._json(502, {"error": str(exc)})
            return
        if path == "/api/v4/opportunities":
            try:
                payload = scanner_payload()
            except RuntimeError as exc:
                self._json(502, {"error": str(exc)})
            else:
                self._json(200, payload)
            return
        if path == "/api/v4/trade/symbols":
            try:
                self._json(200, trade_symbols_payload())
            except RuntimeError as exc:
                self._json(502, {"error": str(exc)})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
            if path == "/api/v4/trade/preflight":
                self._json(200, create_preflight(payload))
                return
            if path == "/api/v4/trade/submit":
                self._json(200, submit_preflight(payload))
                return
            if path == "/api/v4/leverage/preflight":
                self._json(200, create_leverage_preflight())
                return
            if path == "/api/v4/leverage/submit":
                self._json(200, submit_leverage_preflight(payload))
                return
        except PermissionError as exc:
            self._json(403, {"error": str(exc)})
            return
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
            return
        except RuntimeError as exc:
            self._json(502, {"error": str(exc)})
            return
        except Exception as exc:
            self._json(500, {"error": f"unexpected server error: {type(exc).__name__}"})
            return
        self._json(404, {"error": "not found"})


def main() -> None:
    parser = argparse.ArgumentParser(description="FundingCarry v4 local Gate CrossEx gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8794)
    args = parser.parse_args()
    threading.Thread(target=_background_worker, name="fundingcarryv4-guard", daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"FundingCarry v4: http://{args.host}:{args.port}/")
    print(
        "Mode: "
        + ("LIVE trading enabled" if live_trading_enabled() else "trading interface locked")
        + "; one Gate CrossEx credential"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
