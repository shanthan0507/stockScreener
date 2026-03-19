import pandas as pd
import pytest
from unittest.mock import patch
from scripts.fetcher import fetch_ohlcv
from tests.conftest import make_ohlcv


def _multi_index_df(tickers, n=60):
    """Build a MultiIndex DataFrame like yf.download returns for multiple tickers."""
    frames = {t: make_ohlcv(n=n, seed=hash(t) % 1000) for t in tickers}
    cols = pd.MultiIndex.from_product(
        [["Open", "High", "Low", "Close", "Volume"], tickers]
    )
    data = {}
    for field in ["Open", "High", "Low", "Close", "Volume"]:
        for t in tickers:
            data[(field, t)] = frames[t][field].values
    idx = frames[tickers[0]].index
    return pd.DataFrame(data, index=idx, columns=cols)


def test_fetch_returns_dict_of_dataframes():
    tickers = ["AAPL", "MSFT"]
    mock_df = _multi_index_df(tickers)
    with patch("scripts.fetcher.yf.download", return_value=mock_df):
        result = fetch_ohlcv(tickers)
    assert set(result.keys()) == {"AAPL", "MSFT"}
    for df in result.values():
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]


def test_fetch_skips_empty_ticker():
    tickers = ["AAPL", "BAD"]
    # BAD ticker returns all NaN — simulate by only having AAPL in the MultiIndex
    mock_df = _multi_index_df(["AAPL"])
    with patch("scripts.fetcher.yf.download", return_value=mock_df):
        result = fetch_ohlcv(tickers, batch_size=10)
    assert "AAPL" in result
    assert "BAD" not in result


def test_fetch_retries_on_exception(mocker):
    tickers = ["AAPL"]
    mock_df = _multi_index_df(["AAPL"])
    download = mocker.patch("scripts.fetcher.yf.download",
                            side_effect=[Exception("rate limit"), mock_df])
    mocker.patch("scripts.fetcher.time.sleep")
    result = fetch_ohlcv(tickers)
    assert "AAPL" in result
    assert download.call_count == 2


def test_fetch_returns_empty_after_all_retries_fail(mocker):
    tickers = ["AAPL"]
    mocker.patch("scripts.fetcher.yf.download", side_effect=Exception("fail"))
    mocker.patch("scripts.fetcher.time.sleep")
    result = fetch_ohlcv(tickers)
    assert result == {}
