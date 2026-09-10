import logging
import time
from datetime import datetime, timezone
from typing import Any

DBConn = Any

logger = logging.getLogger(__name__)


class RateLimitMonitor:

    def record(self, conn: DBConn, rate_limit: dict | None, operation_name: str = "unknown") -> None:
        if not rate_limit:
            return

        reset_at = None
        raw_reset = rate_limit.get("resetAt")
        if raw_reset:
            reset_at = datetime.fromisoformat(raw_reset.replace("Z", "+00:00"))

        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO monitoring.rate_limit_usage
                    (limit_value, remaining, used, cost, reset_at, operation_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    rate_limit.get("limit"),
                    rate_limit.get("remaining"),
                    rate_limit.get("used"),
                    rate_limit.get("cost"),
                    reset_at,
                    operation_name,
                ),
            )
            conn.commit()
        finally:
            cur.close()

    def check_and_wait(self, rate_limit: dict | None) -> None:
        if not rate_limit:
            return

        remaining = rate_limit.get("remaining", 9999)
        limit = rate_limit.get("limit", 5000)

        logger.debug(
            "Rate limit : %d/%d restant (coût : %d)",
            remaining,
            limit,
            rate_limit.get("cost", 0),
        )

        if remaining < 100:
            raw_reset = rate_limit.get("resetAt")
            if raw_reset:
                reset_dt = datetime.fromisoformat(raw_reset.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                wait_seconds = max(0, (reset_dt - now).total_seconds()) + 5
            else:
                wait_seconds = 60

            logger.warning(
                "Rate limit faible (%d restant) — attente de %.0fs jusqu'au reset.",
                remaining,
                wait_seconds,
            )
            time.sleep(wait_seconds)
