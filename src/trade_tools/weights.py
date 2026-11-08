"""Turn the selected assets into portfolio weights; whatever is not invested is cash.

1. Budget: each of the `top_n` slots is worth 1/top_n, so with 3 of 5 assets selected
   60% is invested and 40% stays in cash.
2. Split the budget by inverse volatility (calmer assets get more), or equally.
3. Cap each asset at `max_weight`; the excess goes to the uncapped assets (cash only
   if every asset is at the cap).
4. Volatility target: if the estimated portfolio volatility (from the last
   `covariance_window` common trading days) exceeds `volatility_target`, scale all
   weights down proportionally; the rest goes to cash.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from trade_tools.settings import Settings
from trade_tools.signals import TRADING_DAYS_PER_YEAR


def cap_weights(weights: pd.Series, cap: float) -> pd.Series:
    """Cap each weight, handing the excess to the uncapped weights pro rata."""
    weights = weights.astype(float).copy()
    capped = pd.Series(False, index=weights.index)
    while True:
        over = (weights > cap + 1e-12) & ~capped
        if not over.any():
            return weights
        excess = float((weights[over] - cap).sum())
        weights[over] = cap
        capped |= over
        free = weights[~capped]
        if free.empty or free.sum() <= 0:
            return weights  # everyone is at the cap; the excess stays in cash
        weights[~capped] += excess * free / free.sum()


def aligned_daily_returns(adj_close: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Daily returns per asset on its own trading days, kept only on dates all assets traded."""
    columns = {ticker: adj_close[ticker].dropna().pct_change().iloc[1:] for ticker in tickers}
    return pd.DataFrame(columns).dropna()


def portfolio_volatility(
    weights: pd.Series, adj_close: pd.DataFrame, window: int
) -> float:
    """Annualised volatility of the weighted portfolio over the last `window` common days."""
    if weights.empty:
        return 0.0
    returns = aligned_daily_returns(adj_close, list(weights.index)).tail(window)
    if len(returns) < window:
        return np.nan
    covariance = returns.cov().to_numpy() * TRADING_DAYS_PER_YEAR
    w = weights.to_numpy()
    return float(np.sqrt(max(w @ covariance @ w, 0.0)))


def compute_weights(
    selection: pd.DataFrame,
    adj_close: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.Series, float]:
    """Weights of the selected assets (summing to <= 1) and estimated portfolio volatility.

    `adj_close` must already be cut at the decision date.
    """
    chosen = selection.loc[selection["selected"]]
    if chosen.empty:
        return pd.Series(dtype=float, name="weight"), 0.0
    budget = len(chosen) / settings.top_n
    if settings.inverse_volatility:
        inverse = 1 / chosen["volatility"].astype(float)
        weights = inverse / inverse.sum() * budget
    else:
        weights = pd.Series(budget / len(chosen), index=chosen.index)
    weights = cap_weights(weights, settings.max_weight)

    volatility = portfolio_volatility(weights, adj_close, settings.covariance_window)
    if settings.volatility_target > 0 and volatility > settings.volatility_target:
        weights *= settings.volatility_target / volatility
        volatility = settings.volatility_target
    return weights.rename("weight"), volatility
