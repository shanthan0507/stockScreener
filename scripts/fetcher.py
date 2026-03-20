import logging
import time
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

_PERIOD = "6mo"        # ~125 trading days, enough for all indicators
_MAX_RETRIES = 3
_BACKOFF = [15, 45, 90]  # seconds between retries (longer for rate-limit recovery)
_BATCH_SLEEP = 8         # seconds between batches to avoid rate limiting


def fetch_ohlcv(tickers: list[str], batch_size: int = 100) -> dict[str, pd.DataFrame]:
    """
    Fetch OHLCV history for each ticker.
    Returns dict mapping ticker -> DataFrame(Open,High,Low,Close,Volume).
    Tickers with missing/empty data are silently skipped.
    """
    result: dict[str, pd.DataFrame] = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        logger.info("Fetching batch %d/%d (%d tickers)...",
                    i // batch_size + 1,
                    -(-len(tickers) // batch_size),
                    len(batch))

        for attempt in range(_MAX_RETRIES):
            try:
                raw = yf.download(
                    tickers=batch,
                    period=_PERIOD,
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )
                break
            except Exception as exc:
                if attempt < _MAX_RETRIES - 1:
                    wait = _BACKOFF[attempt]
                    logger.warning("yfinance error (attempt %d): %s. Retrying in %ds...", attempt + 1, exc, wait)
                    time.sleep(wait)
                else:
                    logger.error("yfinance failed after %d attempts: %s", _MAX_RETRIES, exc)
                    raw = pd.DataFrame()

        if i + batch_size < len(tickers):
            time.sleep(_BATCH_SLEEP)

        if raw.empty:
            continue

        # yf.download with multiple tickers returns MultiIndex columns: (field, ticker)
        # With a single ticker it returns flat columns
        if isinstance(raw.columns, pd.MultiIndex):
            for ticker in batch:
                try:
                    df = raw.xs(ticker, axis=1, level=1)[["Open", "High", "Low", "Close", "Volume"]]
                    df = df.dropna(how="all")
                    if not df.empty:
                        result[ticker] = df
                except KeyError:
                    pass  # ticker not in response
        else:
            # single ticker batch
            ticker = batch[0]
            df = raw[["Open", "High", "Low", "Close", "Volume"]].dropna(how="all")
            if not df.empty:
                result[ticker] = df

    logger.info("Fetched OHLCV for %d/%d tickers.", len(result), len(tickers))
    return result
