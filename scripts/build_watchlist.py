#!/usr/bin/env python3
"""
Main pipeline: finviz universe → yfinance OHLCV → indicators → 9 buckets → JSON.
Run: python scripts/build_watchlist.py
"""
import logging
import sys
from collections import defaultdict

import numpy as np
import pandas as pd

from scripts.universe import get_universe
from scripts.fetcher import fetch_ohlcv
from scripts.indicators import compute
from scripts.buckets import BUCKETS, classify
from scripts.writer import write_watchlist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _percentile_rank(series: pd.Series) -> pd.Series:
    """Return percentile rank (0-100) for each value in series."""
    return series.rank(pct=True, method="average") * 100


def run() -> None:
    # 1. Universe
    tickers = get_universe()
    logger.info("Universe: %d tickers", len(tickers))

    # 2. Fetch OHLCV (include SPY)
    all_tickers = sorted(set(tickers + ["SPY"]))
    ohlcv = fetch_ohlcv(all_tickers)

    spy_df = ohlcv.pop("SPY", pd.DataFrame())
    if spy_df.empty:
        logger.warning("SPY data missing — RS excess calculations will be None")

    # Filter tickers that have data
    tickers = [t for t in tickers if t in ohlcv]
    logger.info("OHLCV available for %d tickers", len(tickers))

    # 3. Build universe_closes (needed by indicators.compute signature, currently unused internally)
    universe_closes = pd.DataFrame({t: ohlcv[t]["Close"] for t in tickers})

    # 4. Compute indicators for each ticker
    indicators: dict[str, dict] = {}
    for ticker in tickers:
        ind = compute(ohlcv[ticker], spy_df, universe_closes)
        if ind:
            ind["close"] = float(ohlcv[ticker]["Close"].iloc[-1])
            indicators[ticker] = ind

    logger.info("Indicators computed for %d tickers", len(indicators))

    # 5. Percentile-rank across universe
    valid = {t: ind for t, ind in indicators.items() if ind}

    def _series(key: str) -> pd.Series:
        return pd.Series({t: ind.get(key) for t, ind in valid.items()}).dropna()

    # RS 1M percentile
    rs1m_ranks = _percentile_rank(_series("rs_1m_raw"))
    # Hybrid RS: weighted composite of 1M + 3M + 6M
    raw_composite = pd.Series({
        t: (
            ind.get("rs_1m_raw", 0) * 0.4
            + ind.get("rs_3m_raw", 0) * 0.3
            + ind.get("rs_6m_raw", 0) * 0.3
        )
        for t, ind in valid.items()
        if ind.get("rs_1m_raw") is not None
        and ind.get("rs_3m_raw") is not None
        and ind.get("rs_6m_raw") is not None
    })
    hybrid_ranks = _percentile_rank(raw_composite)

    # Weekly % rank
    weekly_ranks = _percentile_rank(_series("weekly_pct"))
    # Monthly (3M) % rank
    monthly_ranks = _percentile_rank(_series("rs_3m_raw"))

    # Inject percentile ranks back into each indicator dict
    for ticker, ind in indicators.items():
        ind["rs_1m"] = float(rs1m_ranks.get(ticker, 0))
        ind["hybrid_rs"] = float(hybrid_ranks.get(ticker, 0))
        ind["weekly_pct_rank"] = float(weekly_ranks.get(ticker, 0))
        ind["monthly_pct_rank"] = float(monthly_ranks.get(ticker, 0))

    # 6. Classify into buckets
    bucket_map: dict[str, list[str]] = defaultdict(list)
    for ticker, ind in indicators.items():
        for bucket_name in classify(ticker, ind):
            bucket_map[bucket_name].append(ticker)

    # 7. Sort each bucket by rs_1m descending
    def _rs1m(t: str) -> float:
        return indicators.get(t, {}).get("rs_1m", 0)

    buckets_output = []
    for name in BUCKETS:
        tickers_in_bucket = sorted(bucket_map.get(name, []), key=_rs1m, reverse=True)
        buckets_output.append({"name": name, "tickers": tickers_in_bucket})

    # 8. Write output
    write_watchlist(buckets_output)

    total = sum(len(b["tickers"]) for b in buckets_output)
    logger.info("Done. %d bucket entries written.", total)


if __name__ == "__main__":
    run()
