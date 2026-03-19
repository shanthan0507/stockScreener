"""Fetch pre-filtered universe of US stocks from finviz."""
import logging
from finviz.screener import Screener

logger = logging.getLogger(__name__)

_FILTERS = [
    "cap_largeover",            # Market Cap > $1B (large + mega)
    "sh_avgvol_o1000",          # Avg Vol > 1M (finviz uses 1000 = 1M shares)
    "ta_averagetruerange_o3.5", # ADR% > 3.5
    "ta_averagetruerange_u10",  # ADR% < 10
    "sec_ex_healthcare",        # Exclude Healthcare
]


def get_universe() -> list[str]:
    """Fetch pre-filtered ticker universe from finviz. Returns sorted list of tickers."""
    logger.info("Fetching universe from finviz...")
    screen = Screener(filters=_FILTERS, table="Overview", order="ticker")
    tickers = sorted({row["Ticker"] for row in screen if row.get("Ticker")})
    logger.info("Universe: %d tickers", len(tickers))
    return tickers
