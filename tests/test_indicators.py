import numpy as np
import pandas as pd
import pytest
from tests.conftest import make_ohlcv
from scripts.indicators import compute, _pocket_pivot_count


@pytest.fixture
def ticker_df():
    return make_ohlcv(n=200)

@pytest.fixture
def spy():
    return make_ohlcv(n=200, seed=99, trend=0.0005)

@pytest.fixture
def universe_closes(ticker_df):
    return pd.DataFrame({"TEST": ticker_df["Close"], "SPY": ticker_df["Close"] * 0.98})


def test_compute_returns_dict(ticker_df, spy, universe_closes):
    result = compute(ticker_df, spy, universe_closes)
    assert isinstance(result, dict)
    assert len(result) > 0


def test_compute_has_required_keys(ticker_df, spy, universe_closes):
    result = compute(ticker_df, spy, universe_closes)
    required = [
        "ema21", "sma50", "atr14", "adr_pct", "dcr_pct", "rel_vol",
        "daily_pct", "from_open_pct", "weekly_pct", "rs_1m_raw", "rs_3m_raw",
        "rs_6m_raw", "rs_1m_excess", "vcs", "ema21_atr", "sma50_atr", "pp_count_30d",
        "trend_base", "avg_vol_50d",
    ]
    for key in required:
        assert key in result, f"Missing key: {key}"


def test_compute_returns_empty_for_short_series(spy, universe_closes):
    short_df = make_ohlcv(n=30)
    result = compute(short_df, spy, universe_closes)
    assert result == {}


def test_vcs_in_range(ticker_df, spy, universe_closes):
    result = compute(ticker_df, spy, universe_closes)
    if result.get("vcs") is not None:
        assert 0 <= result["vcs"] <= 100


def test_trend_base_is_bool(ticker_df, spy, universe_closes):
    result = compute(ticker_df, spy, universe_closes)
    assert isinstance(result["trend_base"], bool)


def test_pocket_pivot_count_non_negative(ticker_df):
    count = _pocket_pivot_count(ticker_df, days=30)
    assert count >= 0
    assert isinstance(count, int)
