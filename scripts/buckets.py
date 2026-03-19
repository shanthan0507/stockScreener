import logging
from typing import Optional

logger = logging.getLogger(__name__)

BUCKETS = [
    "21EMA",
    "4% Bullish",
    "Vol Up",
    "Momentum 97",
    "97 Club",
    "VCS",
    "Pocket Pivot",
    "PP Count",
    "Weekly 20%+ Gainers",
]


def _get(d: dict, key: str, default=None):
    """Safe getter that returns default if key missing or value is None."""
    v = d.get(key)
    return default if v is None else v


def filter_21ema(ind: dict) -> bool:
    return (
        _get(ind, "dcr_pct", -1) > 20
        and -0.5 <= _get(ind, "ema21_atr", float("nan")) <= 1.0
        and 0.0 <= _get(ind, "sma50_atr", float("nan")) <= 3.0
        and _get(ind, "pp_count_30d", 0) >= 1
        and _get(ind, "trend_base", False) is True
    )


def filter_4pct_bullish(ind: dict) -> bool:
    return (
        _get(ind, "rel_vol", 0) >= 1.0
        and _get(ind, "daily_pct", -999) >= 4.0
        and _get(ind, "from_open_pct", -999) >= 0.0
        and _get(ind, "rs_1m", 0) >= 60
    )


def filter_vol_up(ind: dict) -> bool:
    return (
        _get(ind, "rel_vol", 0) >= 1.5
        and _get(ind, "daily_pct", -999) >= 0.0
    )


def filter_momentum_97(ind: dict) -> bool:
    return (
        _get(ind, "avg_vol_50d", 0) >= 1_000_000
        and _get(ind, "weekly_pct_rank", 0) >= 97
        and _get(ind, "monthly_pct_rank", 0) >= 85
        and _get(ind, "trend_base", False) is True
    )


def filter_97_club(ind: dict) -> bool:
    return (
        _get(ind, "hybrid_rs", 0) >= 90
        and _get(ind, "rs_1m", 0) >= 97
        and _get(ind, "trend_base", False) is True
    )


def filter_vcs(ind: dict) -> bool:
    vcs = ind.get("vcs")
    if vcs is None:
        return False
    return (
        60 <= vcs <= 100
        and _get(ind, "rs_1m", 0) >= 60
    )


def filter_pocket_pivot(ind: dict) -> bool:
    return (
        _get(ind, "close", 0) > _get(ind, "sma50", float("inf"))
        and _get(ind, "daily_pct", -999) >= 0.0
        and _get(ind, "pp_count_30d", 0) >= 1
    )


def filter_pp_count(ind: dict) -> bool:
    return (
        _get(ind, "pp_count_30d", 0) >= 3
        and _get(ind, "trend_base", False) is True
    )


def filter_weekly_20(ind: dict) -> bool:
    return _get(ind, "weekly_pct", -999) >= 20.0


_FILTERS = [
    filter_21ema,
    filter_4pct_bullish,
    filter_vol_up,
    filter_momentum_97,
    filter_97_club,
    filter_vcs,
    filter_pocket_pivot,
    filter_pp_count,
    filter_weekly_20,
]

assert len(_FILTERS) == len(BUCKETS), "BUCKETS and _FILTERS must have same length"


def classify(ticker: str, ind: dict) -> list[str]:
    """
    Return list of bucket names this ticker belongs to.
    ind must include all indicator keys plus rs_1m, hybrid_rs, weekly_pct_rank,
    monthly_pct_rank, close set by the orchestrator.
    """
    result = []
    for name, fn in zip(BUCKETS, _FILTERS):
        try:
            if fn(ind):
                result.append(name)
        except Exception as exc:
            logger.warning("classify %s in %s failed: %s", ticker, name, exc)
    return result
