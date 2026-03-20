"""Tests for scripts/buckets.py — 9 bucket filter functions."""

import pytest
from scripts.buckets import (
    BUCKETS,
    _get,
    classify,
    filter_21ema,
    filter_4pct_bullish,
    filter_vol_up,
    filter_momentum_97,
    filter_97_club,
    filter_vcs,
    filter_pocket_pivot,
    filter_pp_count,
    filter_weekly_20,
)


# ---------------------------------------------------------------------------
# Helper: build a "strong" indicator dict that passes most/all filters
# ---------------------------------------------------------------------------
def _strong_ind() -> dict:
    return dict(
        dcr_pct=30,
        ema21_atr=0.5,
        sma50_atr=1.0,
        pp_count_30d=5,
        trend_base=True,
        rel_vol=2.0,
        daily_pct=5.0,
        from_open_pct=2.0,
        rs_1m=98,
        hybrid_rs=95,
        avg_vol_50d=2_000_000,
        weekly_pct_rank=98,
        monthly_pct_rank=90,
        vcs=80,
        close=150.0,
        sma50=140.0,
        weekly_pct=25.0,
    )


def _weak_ind() -> dict:
    return dict(
        dcr_pct=5,
        ema21_atr=5.0,
        sma50_atr=-2.0,
        pp_count_30d=0,
        trend_base=False,
        rel_vol=0.3,
        daily_pct=-2.0,
        from_open_pct=-3.0,
        rs_1m=10,
        hybrid_rs=20,
        avg_vol_50d=100_000,
        weekly_pct_rank=30,
        monthly_pct_rank=20,
        vcs=None,
        close=50.0,
        sma50=80.0,
        weekly_pct=-5.0,
    )


# ---------------------------------------------------------------------------
# _get helper
# ---------------------------------------------------------------------------
class TestGet:
    def test_returns_value(self):
        assert _get({"a": 10}, "a", 0) == 10

    def test_returns_default_on_missing(self):
        assert _get({}, "a", 42) == 42

    def test_returns_default_on_none(self):
        assert _get({"a": None}, "a", 42) == 42


# ---------------------------------------------------------------------------
# Individual bucket filters
# ---------------------------------------------------------------------------
class TestFilter21EMA:
    def test_pass(self):
        ind = dict(dcr_pct=25, ema21_atr=0.0, sma50_atr=1.5, pp_count_30d=2, trend_base=True)
        assert filter_21ema(ind) is True

    def test_fail_dcr_low(self):
        ind = dict(dcr_pct=10, ema21_atr=0.0, sma50_atr=1.5, pp_count_30d=2, trend_base=True)
        assert filter_21ema(ind) is False

    def test_fail_ema21_atr_out_of_range(self):
        ind = dict(dcr_pct=25, ema21_atr=2.0, sma50_atr=1.5, pp_count_30d=2, trend_base=True)
        assert filter_21ema(ind) is False

    def test_fail_missing_key(self):
        assert filter_21ema({}) is False


class TestFilter4PctBullish:
    def test_pass(self):
        ind = dict(rel_vol=1.5, daily_pct=5.0, from_open_pct=1.0, rs_1m=70)
        assert filter_4pct_bullish(ind) is True

    def test_fail_low_rs(self):
        ind = dict(rel_vol=1.5, daily_pct=5.0, from_open_pct=1.0, rs_1m=50)
        assert filter_4pct_bullish(ind) is False


class TestFilterVolUp:
    def test_pass(self):
        assert filter_vol_up(dict(rel_vol=2.0, daily_pct=1.0)) is True

    def test_fail_low_rel_vol(self):
        assert filter_vol_up(dict(rel_vol=1.0, daily_pct=1.0)) is False

    def test_fail_negative_daily(self):
        assert filter_vol_up(dict(rel_vol=2.0, daily_pct=-0.5)) is False


class TestFilterMomentum97:
    def test_pass(self):
        ind = dict(avg_vol_50d=2_000_000, weekly_pct_rank=98, monthly_pct_rank=90, trend_base=True)
        assert filter_momentum_97(ind) is True

    def test_fail_low_weekly_rank(self):
        ind = dict(avg_vol_50d=2_000_000, weekly_pct_rank=90, monthly_pct_rank=90, trend_base=True)
        assert filter_momentum_97(ind) is False


class TestFilter97Club:
    def test_pass(self):
        ind = dict(hybrid_rs=92, rs_1m=98, trend_base=True)
        assert filter_97_club(ind) is True

    def test_fail_low_hybrid(self):
        ind = dict(hybrid_rs=80, rs_1m=98, trend_base=True)
        assert filter_97_club(ind) is False


class TestFilterVCS:
    def test_pass(self):
        assert filter_vcs(dict(vcs=75, rs_1m=65)) is True

    def test_fail_vcs_none(self):
        assert filter_vcs(dict(vcs=None, rs_1m=65)) is False

    def test_fail_vcs_out_of_range(self):
        assert filter_vcs(dict(vcs=50, rs_1m=65)) is False


class TestFilterPocketPivot:
    def test_pass(self):
        assert filter_pocket_pivot(dict(today_is_pp=True)) is True

    def test_fail_not_pp_today(self):
        assert filter_pocket_pivot(dict(today_is_pp=False)) is False

    def test_fail_missing_today_is_pp(self):
        assert filter_pocket_pivot(dict(close=100.0, sma50=90.0)) is False


class TestFilterPPCount:
    def test_pass(self):
        assert filter_pp_count(dict(pp_count_30d=4, trend_base=True)) is True

    def test_fail_low_count(self):
        assert filter_pp_count(dict(pp_count_30d=2, trend_base=True)) is False


class TestFilterWeekly20:
    def test_pass(self):
        assert filter_weekly_20(dict(weekly_pct=25.0)) is True

    def test_fail(self):
        assert filter_weekly_20(dict(weekly_pct=15.0)) is False


# ---------------------------------------------------------------------------
# classify()
# ---------------------------------------------------------------------------
class TestClassify:
    def test_strong_ticker_multiple_buckets(self):
        result = classify("AAPL", _strong_ind())
        # Should appear in many buckets
        assert len(result) >= 5
        assert "21EMA" in result
        assert "Vol Up" in result
        assert "97 Club" in result
        assert "Weekly 20%+ Gainers" in result

    def test_weak_ticker_no_buckets(self):
        result = classify("WEAK", _weak_ind())
        assert result == []

    def test_empty_dict(self):
        result = classify("EMPTY", {})
        assert result == []

    def test_buckets_list_length(self):
        assert len(BUCKETS) == 9
