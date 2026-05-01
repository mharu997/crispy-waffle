"""SEC EDGAR identity setup.

EDGAR requires every HTTP client to send a ``User-Agent`` header containing a
real name and email. The 10 req/s rate limit applies to every requester. This
module sets the global identity for both the ``edgartools`` library and our
direct ``httpx`` clients.

References:
    - https://www.sec.gov/os/accessing-edgar-data
    - https://www.sec.gov/oit/announcement/new-rate-control-limits

Usage::

    from pitds.identity import setup_sec_identity
    setup_sec_identity()  # call once at program start
"""

from __future__ import annotations

import logging

from .config import settings

log = logging.getLogger(__name__)


def setup_sec_identity() -> str:
    """Configure SEC identity globally; return the user-agent string in use."""
    user_agent = settings.require_sec_user_agent()

    # edgartools reads SEC_IDENTITY env var, but also exposes set_identity().
    # Setting it via the library function is more robust to import order.
    try:
        import edgar  # type: ignore

        edgar.set_identity(user_agent)
        log.debug("edgar.set_identity configured to %s", user_agent)
    except ImportError:
        log.warning("edgartools not installed; install with `pip install edgartools`")

    return user_agent


def sec_headers() -> dict[str, str]:
    """Headers for direct (non-edgartools) calls to data.sec.gov / sec.gov."""
    return {
        "User-Agent": settings.require_sec_user_agent(),
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }
