"""Fetch universe of US stocks from Wikipedia (S&P 500 + S&P 400)."""
import io
import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; stockscreener-bot/1.0)"}


def _fetch_table(url: str, col: str) -> list[str]:
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    table = pd.read_html(io.StringIO(resp.text))[0]
    return table[col].tolist()


def get_universe() -> list[str]:
    """
    Fetch S&P 500 + S&P 400 Mid-cap tickers from Wikipedia (~900 tickers).
    Dots replaced with dashes for yfinance compatibility (BRK.B -> BRK-B).
    """
    logger.info("Fetching S&P 500 from Wikipedia...")
    sp500 = _fetch_table(_SP500_URL, "Symbol")

    logger.info("Fetching S&P 400 from Wikipedia...")
    sp400 = _fetch_table(_SP400_URL, "Symbol")

    raw = [t for t in sp500 + sp400 if isinstance(t, str) and t.strip()]
    tickers = sorted({t.strip().replace(".", "-") for t in raw})

    logger.info("Universe: %d tickers (S&P 500 + S&P 400)", len(tickers))
    return tickers
