"""Anomaly notification — log to file for now; pluggable for email/Slack later.

The daily updater calls into this module when it detects an anomaly:

* a constituent diff vs Wikipedia that doesn't match S&P press releases
* a verify check that flips from passing to failing
* an abnormally large number of price/fundamentals fetch failures

For Phase C the policy is "log loudly, don't auto-fix." Surface the
issue so a human can investigate against the canonical S&P press releases
or filing announcements.

The log goes to ``data/raw/anomalies.log`` (gitignored, append-only).
"""

from __future__ import annotations

import datetime as dt
import logging

from ..config import settings

log = logging.getLogger(__name__)


def _log_path():
    return settings.raw_dir / "anomalies.log"


def notify(category: str, message: str, *, severity: str = "warning") -> None:
    """Append an anomaly entry to the local notification log.

    *category* is a short tag (e.g. "constituent_diff", "verify_failure",
    "fetch_failures_high"). *severity* is "info" | "warning" | "critical".
    """
    line = f"{dt.datetime.now().isoformat(timespec='seconds')}\t[{severity}]\t{category}\t{message}\n"
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(line)
    if severity == "critical":
        log.error("[ANOMALY] %s: %s", category, message)
    elif severity == "warning":
        log.warning("[ANOMALY] %s: %s", category, message)
    else:
        log.info("[ANOMALY] %s: %s", category, message)


def recent_anomalies(*, limit: int = 50) -> list[str]:
    """Return the last *limit* anomaly entries (most recent last)."""
    path = _log_path()
    if not path.exists():
        return []
    lines = path.read_text().splitlines()
    return lines[-limit:]
