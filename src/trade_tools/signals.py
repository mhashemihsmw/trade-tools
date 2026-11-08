"""Per-asset signals at a decision date, using only prices observed up to that date.

For each asset (on its own trading days, so exchange holidays don't distort lags):
    r1, r3, r6, r12   simple returns over 21, 63, 126, 252 trading days
    momentum          (12*r1 + 4*r3 + 2*r6 + 1*r12) / 4   ("13612W"; weights are configurable)
    volatility        annualised std of the last 63 daily returns
    trend_distance    price / (average of the last 10 month-end prices) - 1, where the
                      current price counts as the latest month
    score             momentum / volatility (or momentum if volatility_adjusted = false)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from trade_tools.settings import Settings

RETURN_LAGS = {"r1": 21, "r3": 63, "r6": 126, "r12": 252}
TRADING_DAYS_PER_YEAR = 252
COLUMNS = [
    "n_obs", "last_date", "price", *RETURN_LAGS, "momentum",
    "volatility", "trend_distance", "score",
]


def asset_signals(prices: pd.Series, settings: Settings) -> dict:
    """Signals for one asset from its price history (already cut at the decision date)."""
    series = prices.dropna()
    result = dict.fromkeys(COLUMNS, np.nan)
    result["n_obs"] = len(series)
    if series.empty:
        result["last_date"] = pd.NaT
        return result
    # The longest look-back is 12 months of returns or `trend_months` month-ends.
    recent = series.iloc[-max(300, 23 * settings.trend_months):]
    price = recent.iloc[-1]
    result["last_date"] = recent.index[-1]
    result["price"] = price

    for name, lag in RETURN_LAGS.items():
        if len(recent) > lag:
            result[name] = price / recent.iloc[-1 - lag] - 1
    returns = [result[name] for name in RETURN_LAGS]
    result["momentum"] = float(np.dot(settings.momentum_weights, returns)) / len(returns)

    daily = recent.pct_change().iloc[1:].tail(settings.volatility_window)
    if len(daily) == settings.volatility_window:
        result["volatility"] = daily.std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)

    month_ends = recent.groupby(recent.index.to_period("M")).last()
    if len(month_ends) >= settings.trend_months:
        result["trend_distance"] = price / month_ends.tail(settings.trend_months).mean() - 1

    if settings.volatility_adjusted:
        volatility = result["volatility"]
        result["score"] = result["momentum"] / volatility if volatility > 0 else np.nan
    else:
        result["score"] = result["momentum"]
    return result


def compute_signals(
    adj_close: pd.DataFrame,
    as_of: pd.Timestamp,
    tickers: list[str] | pd.Index,
    settings: Settings,
) -> pd.DataFrame:
    """One row per ticker with the signals known at `as_of`."""
    known = adj_close.loc[:as_of]
    rows = {
        ticker: asset_signals(
            known[ticker] if ticker in known else pd.Series(dtype=float), settings
        )
        for ticker in tickers
    }
    frame = pd.DataFrame.from_dict(rows, orient="index", columns=COLUMNS)
    frame.index.name = "ticker"
    return frame.astype({"n_obs": int})
