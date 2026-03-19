import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def compute(df: pd.DataFrame, spy_df: pd.DataFrame, universe_closes: pd.DataFrame) -> dict:
    """Compute all indicators for a single ticker. Returns flat dict."""
    try:
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]
        n = len(close)

        if n < 60:
            return {}

        # --- Trend indicators ---
        ema21 = close.ewm(span=21, adjust=False).mean()
        sma50 = close.rolling(50).mean()

        # Weekly resampled close
        weekly_close = close.resample("W").last().dropna()
        wma10 = weekly_close.rolling(10).mean()
        wma30 = weekly_close.rolling(30).mean()

        # --- ATR (14-day) ---
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr14 = tr.rolling(14).mean()

        # --- ADR% (20-day average daily range) ---
        adr_pct = ((high - low) / low * 100).rolling(20).mean()

        # --- DCR% (today's daily closing range) ---
        today_high = high.iloc[-1]
        today_low = low.iloc[-1]
        today_close = close.iloc[-1]
        dcr_pct = (today_close - today_low) / (today_high - today_low) * 100 if today_high != today_low else None

        # --- Relative Volume ---
        avg_vol_50 = volume.rolling(50).mean()
        rel_vol = float(volume.iloc[-1] / avg_vol_50.iloc[-1]) if avg_vol_50.iloc[-1] > 0 else None

        # --- Daily % change ---
        daily_pct = float(close.pct_change().iloc[-1] * 100)

        # --- From Open % ---
        from_open_pct = float((close.iloc[-1] - df["Open"].iloc[-1]) / df["Open"].iloc[-1] * 100)

        # --- Weekly % change ---
        weekly_close_series = close.resample("W").last().dropna()
        weekly_pct = float(weekly_close_series.pct_change().iloc[-1] * 100) if len(weekly_close_series) >= 2 else None

        # --- RS 1M (1-month relative strength vs SPY, raw percentile computed externally) ---
        # Return raw 1M return for percentile ranking in orchestrator
        rs_1m_raw = float(close.iloc[-1] / close.iloc[-21] - 1) if n >= 21 else None
        spy_1m_raw = float(spy_df["Close"].iloc[-1] / spy_df["Close"].iloc[-21] - 1) if len(spy_df) >= 21 else None
        rs_1m_excess = float(rs_1m_raw - spy_1m_raw) if (rs_1m_raw is not None and spy_1m_raw is not None) else None

        # --- RS 3M raw ---
        rs_3m_raw = float(close.iloc[-1] / close.iloc[-63] - 1) if n >= 63 else None

        # --- Hybrid RS: composite of RS 1M + RS 3M + RS 6M (raw, percentile-rank externally) ---
        rs_6m_raw = float(close.iloc[-1] / close.iloc[-126] - 1) if n >= 126 else None

        # --- VCS: Volatility Contraction Score (0-100) ---
        # ATR contraction: compare recent 10-day ATR to 60-day ATR
        atr_10 = tr.rolling(10).mean().iloc[-1]
        atr_60 = tr.rolling(60).mean().iloc[-1]
        if pd.notna(atr_10) and pd.notna(atr_60) and atr_60 > 0:
            contraction_ratio = atr_10 / atr_60  # < 1 = contracting
            vcs = float(np.clip((1 - contraction_ratio) * 100, 0, 100))
        else:
            vcs = None

        # --- 21EMA distance in ATR units ---
        ema21_atr = float((close.iloc[-1] - ema21.iloc[-1]) / atr14.iloc[-1]) if pd.notna(atr14.iloc[-1]) and atr14.iloc[-1] > 0 else None
        sma50_atr = float((close.iloc[-1] - sma50.iloc[-1]) / atr14.iloc[-1]) if pd.notna(sma50.iloc[-1]) and pd.notna(atr14.iloc[-1]) and atr14.iloc[-1] > 0 else None

        # --- Pocket Pivot Count (30d) ---
        pp_count = _pocket_pivot_count(df, days=30)

        # --- Trend Base ---
        trend_base = bool(
            pd.notna(sma50.iloc[-1]) and
            close.iloc[-1] > sma50.iloc[-1] and
            pd.notna(wma10.iloc[-1]) and pd.notna(wma30.iloc[-1]) and
            wma10.iloc[-1] > wma30.iloc[-1]
        )

        # --- Avg Vol 50d ---
        avg_vol_50d = float(avg_vol_50.iloc[-1]) if pd.notna(avg_vol_50.iloc[-1]) else None

        return {
            "ema21": float(ema21.iloc[-1]),
            "sma50": float(sma50.iloc[-1]) if pd.notna(sma50.iloc[-1]) else None,
            "atr14": float(atr14.iloc[-1]) if pd.notna(atr14.iloc[-1]) else None,
            "adr_pct": float(adr_pct.iloc[-1]) if pd.notna(adr_pct.iloc[-1]) else None,
            "dcr_pct": dcr_pct,
            "rel_vol": rel_vol,
            "daily_pct": daily_pct,
            "from_open_pct": from_open_pct,
            "weekly_pct": weekly_pct,
            "rs_1m_raw": rs_1m_raw,
            "rs_3m_raw": rs_3m_raw,
            "rs_6m_raw": rs_6m_raw,
            "rs_1m_excess": rs_1m_excess,
            "vcs": vcs,
            "ema21_atr": ema21_atr,
            "sma50_atr": sma50_atr,
            "pp_count_30d": pp_count,
            "trend_base": trend_base,
            "avg_vol_50d": avg_vol_50d,
            # These are set externally by orchestrator after percentile-ranking across universe:
            # "rs_1m", "rs_3m", "hybrid_rs", "1w_pct_rank", "3m_pct_rank"
        }
    except Exception as exc:
        logger.warning("compute() failed: %s", exc)
        return {}


def _pocket_pivot_count(df: pd.DataFrame, days: int = 30) -> int:
    """
    Count pocket pivot days in the past `days` days.
    Pocket pivot: green candle, close > 50SMA, volume > max(volume[-10d] on down days).
    """
    close = df["Close"]
    volume = df["Volume"]
    sma50 = close.rolling(50).mean()

    count = 0
    window = df.iloc[-days:]
    for i in range(len(window)):
        idx = window.index[i]
        pos = df.index.get_loc(idx)
        if pos < 11:
            continue
        day_close = df["Close"].iloc[pos]
        day_open = df["Open"].iloc[pos]
        day_vol = df["Volume"].iloc[pos]
        day_sma50 = sma50.iloc[pos]

        if pd.isna(day_sma50) or day_close <= day_sma50:
            continue
        if day_close <= day_open:  # not a green candle
            continue

        # Volume > highest down-day volume in prior 10 sessions
        prior_10 = df.iloc[pos - 10 : pos]
        down_days = prior_10[prior_10["Close"] < prior_10["Open"]]
        if down_days.empty:
            continue
        max_down_vol = down_days["Volume"].max()
        if day_vol > max_down_vol:
            count += 1

    return count
