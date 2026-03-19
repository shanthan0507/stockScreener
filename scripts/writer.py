import json
import logging
import os
from datetime import datetime, timezone

import pytz

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_WATCHLIST_PATH = os.path.join(_DATA_DIR, "watchlist.json")
_META_PATH = os.path.join(_DATA_DIR, "meta.json")

_ET = pytz.timezone("America/New_York")


def write_watchlist(buckets: list[dict], date_str: str | None = None) -> None:
    """
    Write watchlist.json and meta.json to data/.

    Args:
        buckets: list of {"name": str, "tickers": list[str]}
        date_str: optional ISO date string (YYYY-MM-DD). Defaults to today ET.
    """
    os.makedirs(_DATA_DIR, exist_ok=True)

    if date_str is None:
        date_str = datetime.now(_ET).strftime("%Y-%m-%d")

    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")

    watchlist = {
        "date": date_str,
        "buckets": buckets,
    }

    meta = {
        "updated": now_utc,
        "date": date_str,
        "total_tickers": sum(len(b["tickers"]) for b in buckets),
    }

    _write_json(_WATCHLIST_PATH, watchlist)
    _write_json(_META_PATH, meta)

    logger.info(
        "Wrote watchlist.json (%d buckets, %d total tickers) and meta.json",
        len(buckets),
        meta["total_tickers"],
    )


def _write_json(path: str, data: dict) -> None:
    abs_path = os.path.abspath(path)
    with open(abs_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.debug("Wrote %s", abs_path)
