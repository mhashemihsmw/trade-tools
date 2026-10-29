"""Pure point-in-time price features used by the research core."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd


RETURN_WINDOWS = {"r1": 21, "r3": 63, "r6": 126, "r12": 252}


def _valid_prices(prices: pd.Series) -> pd.Series:
    values = pd.to_numeric(prices, errors="coerce").astype(float)
    return values.where(np.isfinite(values) & (values > 0))


def historical_returns(prices: pd.Series, windows: Mapping[str, int] = RETURN_WINDOWS) -> pd.DataFrame:
    """Calculate simple total returns at exact trading-observation lags."""
    clean = _valid_prices(prices)
    return pd.DataFrame({name: clean.div(clean.shift(days)).sub(1) for name, days in windows.items()})


def realized_volatility(prices: pd.Series, window: int = 63) -> pd.Series:
    """Annualized sample volatility of daily simple returns; warm-up remains missing."""
    if window < 2:
        raise ValueError("window must be at least 2")
    returns = _valid_prices(prices).pct_change(fill_method=None)
    return returns.rolling(window=window, min_periods=window).std(ddof=1) * np.sqrt(252)


def aligned_daily_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """Align each asset's observed daily simple returns on UTC calendar dates, without filling."""
    series_by_asset: dict[str, pd.Series] = {}
    for ticker in prices.columns:
        # A wide input has NaNs for other exchanges' sessions; compute each
        # asset's returns on its own observed price timestamps before joining.
        observed_prices = _valid_prices(prices[ticker]).dropna()
        series = observed_prices.pct_change(fill_method=None)
        series.index = pd.DatetimeIndex(series.index).normalize()
        series_by_asset[ticker] = series.groupby(level=0).last()
    if not series_by_asset:
        return pd.DataFrame()
    return pd.concat(series_by_asset, axis=1).dropna(how="any")


def sample_covariance(prices: pd.DataFrame, window: int = 63) -> pd.DataFrame:
    """Return the latest sample covariance matrix of aligned daily returns."""
    if window < 2:
        raise ValueError("window must be at least 2")
    returns = aligned_daily_returns(prices).tail(window)
    if len(returns) < window or returns.isna().any().any():
        return pd.DataFrame(np.nan, index=prices.columns, columns=prices.columns)
    return returns.cov(ddof=1) * 252


def month_end_moving_average(prices: pd.Series, months: int = 10) -> pd.Series:
    """Map the trailing month-end SMA to each date without using future closes."""
    if months < 1:
        raise ValueError("months must be positive")
    clean = _valid_prices(prices)
    month_ends = clean.resample("ME").last()
    average = month_ends.rolling(months, min_periods=months).mean()
    mapped = average.reindex(clean.index, method="ffill")
    return mapped.reindex(prices.index)


def rolling_max_drawdown(prices: pd.Series, window: int = 252) -> pd.Series:
    """Rolling worst peak-to-trough drawdown within each trailing window."""
    if window < 2:
        raise ValueError("window must be at least 2")
    clean = _valid_prices(prices)

    def drawdown(values: np.ndarray) -> float:
        if not np.isfinite(values).all() or np.any(values <= 0):
            return np.nan
        peaks = np.maximum.accumulate(values)
        return float(np.min(values / peaks - 1))

    return clean.rolling(window=window, min_periods=window).apply(drawdown, raw=True)


def distance_from_rolling_high(prices: pd.Series, window: int = 252) -> pd.Series:
    """Price distance from a full-window rolling high, as a simple fraction."""
    if window < 1:
        raise ValueError("window must be positive")
    clean = _valid_prices(prices)
    high = clean.rolling(window=window, min_periods=window).max()
    return clean.div(high).sub(1)


def feature_frame(
    prices: pd.Series,
    volatility_window: int = 63,
    trend_months: int = 10,
    trend_days: int | None = None,
    drawdown_window: int = 252,
) -> pd.DataFrame:
    """Build the baseline and ML-ready features for one asset's point-in-time prices."""
    clean = _valid_prices(prices)
    frame = historical_returns(clean)
    frame["mom_13612w"] = (
        12 * frame["r1"] + 4 * frame["r3"] + 2 * frame["r6"] + frame["r12"]
    ) / 4
    frame["realized_vol"] = realized_volatility(clean, volatility_window)
    if trend_days is not None:
        if trend_days < 1:
            raise ValueError("trend_days must be positive")
        frame["month_end_sma"] = clean.rolling(trend_days, min_periods=trend_days).mean()
    else:
        frame["month_end_sma"] = month_end_moving_average(clean, trend_months)
    frame["trend_distance"] = clean.div(frame["month_end_sma"]).sub(1)
    frame["rolling_max_drawdown"] = rolling_max_drawdown(clean, drawdown_window)
    frame["distance_from_rolling_high"] = distance_from_rolling_high(clean, drawdown_window)
    frame["price"] = clean
    return frame


def normalize_cross_section(
    panel: pd.DataFrame,
    value_column: str,
    date_column: str = "decision_date",
    winsor_limits: tuple[float, float] = (0.05, 0.95),
) -> pd.DataFrame:
    """Add per-date percentile ranks and winsorized z-scores for a value column."""
    lower, upper = winsor_limits
    if not 0 <= lower < upper <= 1:
        raise ValueError("winsor_limits must satisfy 0 <= lower < upper <= 1")
    result = panel.copy()
    def rank_01(values: pd.Series) -> pd.Series:
        ranks = values.rank(method="average")
        distinct = ranks.count()
        return (ranks - 1) / (distinct - 1) if distinct > 1 else ranks * 0.5

    result["rank_01"] = result.groupby(date_column)[value_column].transform(rank_01)

    def zscore(values: pd.Series) -> pd.Series:
        finite = values.dropna()
        if finite.empty:
            return pd.Series(np.nan, index=values.index)
        clipped = finite.clip(finite.quantile(lower), finite.quantile(upper))
        std = clipped.std(ddof=1)
        scaled = (clipped - clipped.mean()) / std if std and np.isfinite(std) else clipped * 0
        return scaled.reindex(values.index)

    result["winsorized_zscore"] = result.groupby(date_column)[value_column].transform(zscore)
    return result


def build_feature_panel(
    prices_by_asset: Mapping[str, pd.Series],
    decision_dates: Sequence[pd.Timestamp],
    *,
    volatility_window: int = 63,
    trend_months: int = 10,
    trend_days: int | None = None,
) -> pd.DataFrame:
    """Build decision-date × asset features using only observations at or before each date."""
    rows: list[dict[str, Any]] = []
    for ticker, series in prices_by_asset.items():
        frame = feature_frame(
            series,
            volatility_window=volatility_window,
            trend_months=trend_months,
            trend_days=trend_days,
        )
        valid = _valid_prices(series)
        for raw_date in decision_dates:
            decision_date = pd.Timestamp(raw_date)
            if isinstance(series.index, pd.DatetimeIndex):
                if series.index.tz is None and decision_date.tzinfo is not None:
                    decision_date = decision_date.tz_localize(None)
                elif series.index.tz is not None and decision_date.tzinfo is None:
                    decision_date = decision_date.tz_localize(series.index.tz)
                elif series.index.tz is not None:
                    decision_date = decision_date.tz_convert(series.index.tz)
            point_in_time = frame.loc[frame.index <= decision_date]
            observed = valid.loc[valid.index <= decision_date]
            raw_observations = series.loc[series.index <= decision_date]
            if point_in_time.empty:
                continue
            features = point_in_time.iloc[-1].to_dict()
            features.update({
                "decision_date": decision_date,
                "ticker": ticker,
                "observations": int(observed.count()),
                "completeness": float(observed.count() / max(1, len(raw_observations))),
                "latest_observation": observed.index[-1] if not observed.empty else pd.NaT,
                "freshness_days": (
                    (decision_date - observed.index[-1]).total_seconds() / 86400
                    if not observed.empty else np.nan
                ),
            })
            rows.append(features)
    return pd.DataFrame(rows)
