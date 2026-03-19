from unittest.mock import patch, MagicMock
import pytest
from scripts.universe import get_universe


def _make_rows(tickers):
    return [{"Ticker": t, "Market Cap": "2B"} for t in tickers]


def test_get_universe_returns_sorted_list():
    rows = _make_rows(["MSFT", "AAPL", "GOOG"])
    with patch("scripts.universe.Screener") as mock_cls:
        mock_cls.return_value.__iter__ = MagicMock(return_value=iter(rows))
        result = get_universe()
    assert result == ["AAPL", "GOOG", "MSFT"]


def test_get_universe_deduplicates():
    rows = _make_rows(["AAPL", "AAPL", "MSFT"])
    with patch("scripts.universe.Screener") as mock_cls:
        mock_cls.return_value.__iter__ = MagicMock(return_value=iter(rows))
        result = get_universe()
    assert result.count("AAPL") == 1


def test_get_universe_skips_empty_ticker():
    rows = _make_rows(["AAPL"]) + [{"Ticker": "", "Market Cap": "2B"}]
    with patch("scripts.universe.Screener") as mock_cls:
        mock_cls.return_value.__iter__ = MagicMock(return_value=iter(rows))
        result = get_universe()
    assert "" not in result
