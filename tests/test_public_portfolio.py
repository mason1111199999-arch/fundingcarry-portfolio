from __future__ import annotations

import sys
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "funding_scanner"))
sys.path.insert(0, str(ROOT / "v4_demo"))

from collector import best_spread, millis, summarize  # noqa: E402
from fundingcarryv4_server import (  # noqa: E402
    build_exit_advisories,
    live_trading_enabled,
    submit_leverage_preflight,
    submit_preflight,
)


class PublicPortfolioTests(unittest.TestCase):
    def test_portfolio_copy_cannot_enable_live_trading_from_environment(self) -> None:
        with patch.dict("os.environ", {"FUNDINGCARRY_V4_ENABLE_LIVE_TRADING": "1"}):
            self.assertFalse(live_trading_enabled())
            with self.assertRaises(PermissionError):
                submit_preflight({})
            with self.assertRaises(PermissionError):
                submit_leverage_preflight({})

    def test_scanner_summarizes_periods_and_rejects_incomplete_coverage(self) -> None:
        now = datetime(2026, 10, 28, tzinfo=timezone.utc)
        start = now - timedelta(days=32)
        entries = []
        cursor = start
        while cursor <= now:
            entries.append((millis(cursor), 0.0001))
            cursor += timedelta(hours=8)

        periods = summarize(entries, now)

        self.assertTrue(periods["7"]["is_complete"])
        self.assertTrue(periods["14"]["is_complete"])
        self.assertTrue(periods["30"]["is_complete"])
        self.assertAlmostEqual(periods["7"]["rate"], 0.0022, places=6)

    def test_spread_requires_two_complete_venue_histories(self) -> None:
        venues = {
            "VenueA": {"periods": {"7": {"rate": -0.01, "is_complete": True}}},
            "VenueB": {"periods": {"7": {"rate": 0.02, "is_complete": True}}},
            "VenueC": {"periods": {"7": {"rate": 0.50, "is_complete": False}}},
        }

        result = best_spread(venues, "7")

        self.assertEqual(result["long"], "VenueA")
        self.assertEqual(result["short"], "VenueB")
        self.assertAlmostEqual(result["spread"], 0.03)

    def test_exit_advisory_uses_demo_thresholds_and_only_completed_funding(self) -> None:
        today = datetime(2026, 10, 28).date()
        opened_ms = int(datetime(2026, 9, 1, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
        positions = [
            {"asset": "ASSETX", "venue": "VENUEA", "symbol": "VENUEA_FUTURE_ASSETX_USDT", "side": "LONG", "notional": 1000, "created_at_ms": opened_ms},
            {"asset": "ASSETX", "venue": "VENUEB", "symbol": "VENUEB_FUTURE_ASSETX_USDC", "side": "SHORT", "notional": 1000, "created_at_ms": opened_ms},
        ]
        rows = [
            {"date": (today - timedelta(days=offset)).isoformat(), "symbol": "VENUEA_FUTURE_ASSETX_USDT", "currency": "USDT", "funding_net": 0.1, "funding_entries": 1}
            for offset in range(1, 15)
        ]

        advice = build_exit_advisories(positions, rows, ledger_complete=True, today=today)[0]

        self.assertEqual(advice["state"], "exit_review")
        self.assertAlmostEqual(advice["seven_day"]["threshold_pct"], 0.10)
        self.assertAlmostEqual(advice["fourteen_day"]["threshold_pct"], 0.20)

    def test_incomplete_ledger_fails_closed(self) -> None:
        positions = [
            {"asset": "ASSETX", "venue": "VENUEA", "symbol": "VENUEA_FUTURE_ASSETX_USDT", "side": "LONG", "notional": 1000, "created_at_ms": 1_780_000_000_000},
            {"asset": "ASSETX", "venue": "VENUEB", "symbol": "VENUEB_FUTURE_ASSETX_USDC", "side": "SHORT", "notional": 1000, "created_at_ms": 1_780_000_000_000},
        ]

        advice = build_exit_advisories(positions, [], ledger_complete=False, today=datetime(2026, 10, 28).date())[0]

        self.assertEqual(advice["state"], "insufficient_data")


if __name__ == "__main__":
    unittest.main()
