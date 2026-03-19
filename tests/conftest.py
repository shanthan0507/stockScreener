import numpy as np
import pandas as pd
import pytest
from datetime import date


def make_ohlcv(n=200, base_price=100.0, trend=0.001, vol_base=1_000_000, seed=42):
    """Generate synthetic OHLCV DataFrame with n business days ending today."""
    np.random.seed(seed)
    dates = pd.bdate_range(end=date.today(), periods=n)
    close = base_price * np.cumprod(1 + np.random.normal(trend, 0.015, n))
    high = close * (1 + np.abs(np.random.normal(0.005, 0.003, n)))
    low = close * (1 - np.abs(np.random.normal(0.005, 0.003, n)))
    open_ = close * (1 + np.random.normal(0, 0.005, n))
    volume = (vol_base * np.random.lognormal(0, 0.3, n)).astype(int)
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=dates,
    )


@pytest.fixture
def sample_df():
    return make_ohlcv()


@pytest.fixture
def spy_df():
    return make_ohlcv(seed=99, trend=0.0005)
