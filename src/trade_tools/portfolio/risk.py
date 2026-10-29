"""Independent target-weight risk checks and concentration summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RiskLimits:
    max_asset_weight: float = 0.30
    max_exposure_group_weight: float = 0.30
    volatility_target: float = 0.10
    volatility_window: int = 63


def validate_target_weights(
    weights: pd.DataFrame,
    limits: RiskLimits | None = None,
    *,
    asset_returns: pd.DataFrame | None = None,
    asset_prices: pd.DataFrame | None = None,
    portfolio_returns: pd.Series | None = None,
) -> dict[str, Any]:
    """Report violations without modifying the supplied allocations."""
    settings = limits or RiskLimits()
    violations: list[str] = []
    warnings: list[str] = ["Look-through constituent exposure is not available."]
    if weights.empty:
        return {
            "status": "valid",
            "limits": settings.__dict__,
            "violations": violations,
            "warnings": warnings,
            "concentrations": {},
            "metrics": {},
        }
    positions = weights.copy()
    values = pd.to_numeric(positions["weight"], errors="coerce")
    positions["weight"] = values
    if not np.isfinite(values.to_numpy(dtype=float)).all() or (values < 0).any():
        violations.append("Weights must be finite and long-only.")
    if values.sum() > 1 + 1e-9:
        violations.append("Risky weights exceed 100% of capital.")
    if "cash_weight" in positions:
        cash = pd.to_numeric(positions["cash_weight"], errors="coerce")
        if (
            not np.isfinite(cash.to_numpy(dtype=float)).all()
            or (cash < 0).any()
            or cash.max() - cash.min() > 1e-9
            or abs(float(values.sum() + cash.iloc[0]) - 1.0) > 1e-9
        ):
            violations.append("Risky and cash weights must be finite, non-negative, and sum to 100%.")
    if (values > settings.max_asset_weight + 1e-9).any():
        violations.append("One or more asset weights exceed the position limit.")
    if "exposure_group" in positions:
        group_weights = positions.groupby("exposure_group")["weight"].sum()
        if (group_weights > settings.max_exposure_group_weight + 1e-9).any():
            violations.append("One or more exposure-group weights exceed the group limit.")
    else:
        group_weights = pd.Series(dtype=float)

    concentrations: dict[str, dict[str, float]] = {}
    for field in ("ticker", "exposure_group", "type", "country", "sector"):
        if field in positions:
            group_labels = positions[field].astype("string").fillna("Unknown")
            concentrations[field] = positions.groupby(group_labels)["weight"].sum().to_dict()
    portfolio_vol = np.nan
    if asset_returns is not None and not asset_returns.empty:
        tickers = positions["ticker"].tolist()
        returns = asset_returns.reindex(columns=tickers).tail(settings.volatility_window)
        if len(returns) == settings.volatility_window and not returns.isna().any().any():
            w = positions.set_index("ticker").loc[tickers, "weight"].to_numpy(dtype=float)
            cov = returns.cov(ddof=1).to_numpy(dtype=float) * 252
            portfolio_vol = float(np.sqrt(max(w @ cov @ w, 0)))
            if settings.volatility_target and portfolio_vol > settings.volatility_target + 1e-9:
                warnings.append("Estimated portfolio volatility exceeds the configured target.")
        else:
            warnings.append("Portfolio volatility unavailable: insufficient aligned return history.")
    elif portfolio_returns is not None and len(portfolio_returns.dropna()) > 1:
        portfolio_vol = float(portfolio_returns.std(ddof=1) * np.sqrt(252))

    drawdowns: dict[str, float] = {}
    if asset_prices is not None:
        for ticker in positions.get("ticker", []):
            prices = pd.to_numeric(asset_prices[ticker], errors="coerce").dropna() if ticker in asset_prices else pd.Series(dtype=float)
            if not prices.empty:
                wealth = prices / prices.iloc[0]
                drawdowns[ticker] = float((wealth / wealth.cummax() - 1).min())
    elif asset_returns is not None:
        for ticker in positions.get("ticker", []):
            returns = asset_returns[ticker].dropna() if ticker in asset_returns else pd.Series(dtype=float)
            if not returns.empty:
                wealth = (1 + returns).cumprod()
                drawdowns[ticker] = float((wealth / wealth.cummax() - 1).min())
    if portfolio_returns is None and asset_returns is not None and not positions.empty:
        tickers = positions["ticker"].tolist()
        returns = asset_returns.reindex(columns=tickers).dropna(how="any")
        if not returns.empty:
            target_weights = positions.set_index("ticker").loc[tickers, "weight"]
            portfolio_returns = returns.mul(target_weights, axis=1).sum(axis=1)
    if portfolio_returns is not None:
        returns = portfolio_returns.dropna()
        if not returns.empty:
            wealth = (1 + returns).cumprod()
            drawdowns["portfolio"] = float((wealth / wealth.cummax() - 1).min())
    return {
        "status": "invalid" if violations else "valid",
        "limits": settings.__dict__,
        "violations": violations,
        "warnings": warnings,
        "concentrations": concentrations,
        "metrics": {"annualized_realized_volatility": portfolio_vol, "maximum_drawdown": drawdowns},
    }
