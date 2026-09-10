"""Tests du RateLimitMonitor — attente reset et enregistrement."""
import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

os.environ.setdefault("GITHUB_TOKEN", "fake_token_for_tests")

from monitoring.rate_limit import RateLimitMonitor


def _future_reset(seconds: int = 120) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestRateLimitMonitor:

    def setup_method(self):
        self.monitor = RateLimitMonitor()

    def test_pas_attente_si_remaining_suffisant(self):
        rate_limit = {"remaining": 500, "limit": 5000, "cost": 1, "resetAt": _future_reset()}
        with patch("time.sleep") as mock_sleep:
            self.monitor.check_and_wait(rate_limit)
        mock_sleep.assert_not_called()

    def test_attente_si_remaining_faible(self):
        rate_limit = {"remaining": 50, "limit": 5000, "cost": 1, "resetAt": _future_reset(60)}
        with patch("time.sleep") as mock_sleep:
            self.monitor.check_and_wait(rate_limit)
        mock_sleep.assert_called_once()
        wait_seconds = mock_sleep.call_args[0][0]
        assert wait_seconds >= 60

    def test_attente_minimale_si_reset_passe(self):
        past_reset = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        rate_limit = {"remaining": 0, "limit": 5000, "cost": 1, "resetAt": past_reset}
        with patch("time.sleep") as mock_sleep:
            self.monitor.check_and_wait(rate_limit)
        wait_seconds = mock_sleep.call_args[0][0]
        # max(0, ...) + 5 → doit être ~5 secondes
        assert 0 <= wait_seconds <= 10

    def test_record_insere_dans_db(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur

        rate_limit = {
            "limit": 5000,
            "remaining": 4800,
            "cost": 2,
            "resetAt": _future_reset(),
        }
        self.monitor.record(conn, rate_limit, operation_name="TestOp")

        cur.execute.assert_called_once()
        args = cur.execute.call_args[0][1]
        assert args[0] == 5000   # limit_value
        assert args[1] == 4800   # remaining
        assert args[3] == 2      # cost
        assert args[5] == "TestOp"
        conn.commit.assert_called_once()

    def test_record_sans_rate_limit_ne_crash_pas(self):
        conn = MagicMock()
        self.monitor.record(conn, {})
        self.monitor.record(conn, None)
        conn.cursor.assert_not_called()
