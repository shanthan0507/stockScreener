from unittest.mock import patch
import pandas as pd
import pytest
from scripts.universe import get_universe


def _mock_read_html(sp500_tickers, sp400_tickers):
    calls = []

    def _side_effect(html_str):
        calls.append(html_str)
        if not calls or len(calls) == 1:
            return [pd.DataFrame({"Symbol": sp500_tickers})]
        return [pd.DataFrame({"Symbol": sp400_tickers})]

    return _side_effect


def _mock_requests_get(sp500_tickers, sp400_tickers):
    call_count = [0]

    class FakeResp:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def _side_effect(url, **kwargs):
        call_count[0] += 1
        if "500" in url:
            return FakeResp(",".join(sp500_tickers))
        return FakeResp(",".join(sp400_tickers))

    return _side_effect


def test_get_universe_returns_sorted_list():
    with patch("scripts.universe.requests.get") as mock_get, \
         patch("scripts.universe.pd.read_html") as mock_html:
        mock_get.return_value.__enter__ = mock_get.return_value
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "html"

        call_count = [0]
        def html_side(html):
            call_count[0] += 1
            if call_count[0] == 1:
                return [pd.DataFrame({"Symbol": ["MSFT", "AAPL"]})]
            return [pd.DataFrame({"Symbol": ["GOOG"]})]
        mock_html.side_effect = html_side

        result = get_universe()

    assert result == ["AAPL", "GOOG", "MSFT"]


def test_get_universe_deduplicates():
    with patch("scripts.universe.requests.get") as mock_get, \
         patch("scripts.universe.pd.read_html") as mock_html:
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "html"

        call_count = [0]
        def html_side(html):
            call_count[0] += 1
            if call_count[0] == 1:
                return [pd.DataFrame({"Symbol": ["AAPL", "MSFT"]})]
            return [pd.DataFrame({"Symbol": ["AAPL"]})]
        mock_html.side_effect = html_side

        result = get_universe()

    assert result.count("AAPL") == 1


def test_get_universe_fixes_dot_tickers():
    with patch("scripts.universe.requests.get") as mock_get, \
         patch("scripts.universe.pd.read_html") as mock_html:
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "html"

        call_count = [0]
        def html_side(html):
            call_count[0] += 1
            if call_count[0] == 1:
                return [pd.DataFrame({"Symbol": ["BRK.B", "BF.B"]})]
            return [pd.DataFrame({"Symbol": []})]
        mock_html.side_effect = html_side

        result = get_universe()

    assert "BRK-B" in result
    assert "BF-B" in result
    assert "BRK.B" not in result
