"""Fetch pre-filtered universe of US stocks from finviz."""
import logging
from finviz.screener import Screener

logger = logging.getLogger(__name__)

_FILTERS = [
    "cap_midover",              # Market Cap > $2B (mid cap and over — closest to >$1B in finviz)
    "sh_avgvol_o1000",          # Avg Vol > 1M
    "sec_ex_healthcare",        # Exclude Healthcare
]


def get_universe() -> list[str]:
    """Fetch pre-filtered ticker universe from finviz. Returns sorted list of tickers."""
    logger.info("Fetching universe from finviz...")
    screen = Screener(filters=_FILTERS, table="Overview", order="ticker", rows=None)
    tickers = sorted({row["Ticker"] for row in screen if row.get("Ticker")})
    logger.info("Universe: %d tickers", len(tickers))
    return tickers
