"""Long-only portfolio target construction; selection remains in the strategy layer."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioConfig:
    top_n: int = 5
    inverse_volatility: bool = True
    max_asset_weight: float = 0.30
    max_exposure_group_weight: float = 0.30
    volatility_target: float = 0.10
    volatility_targeting: bool = True
    covariance_window: int = 63


def construct_target_weights(
    selected: pd.DataFrame,
    config: PortfolioConfig | None = None,
    covariance: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Create target weights from selected assets, leaving unallocated budget as cash."""
    settings = config or PortfolioConfig()
    if settings.top_n < 1:
        raise ValueError("top_n must be positive")
    if not 0 < settings.max_asset_weight <= 1 or not 0 < settings.max_exposure_group_weight <= 1:
        raise ValueError("weight caps must be in (0, 1]")
    if settings.volatility_target < 0:
        raise ValueError("volatility_target cannot be negative")
    if settings.covariance_window < 2:
        raise ValueError("covariance_window must be at least 2")
    positions = selected.loc[selected["selected"]].copy() if "selected" in selected else selected.copy()
    positions = positions.head(settings.top_n)
    if positions.empty:
        return pd.DataFrame(columns=["ticker", "exposure_group", "weight", "cash_weight"])
    risky_budget = len(positions) / settings.top_n
    if settings.inverse_volatility:
        if "realized_vol" not in positions:
            raise ValueError("Selected assets must include realized_vol for inverse-volatility weights")
        vol = pd.to_numeric(positions["realized_vol"], errors="coerce")
        inverse = 1 / vol.where(vol > 0)
        if inverse.isna().any() or inverse.sum() <= 0:
            raise ValueError("Selected assets must have positive realized volatility for inverse-volatility weights")
        weights = inverse / inverse.sum() * risky_budget
    else:
        weights = pd.Series(risky_budget / len(positions), index=positions.index)

    positions["weight"] = weights
    group_key = positions.get("exposure_group", positions["ticker"])
    positions["weight"] = positions.groupby(group_key)["weight"].transform(
        lambda values: min(float(values.sum()), settings.max_exposure_group_weight)
        * values / values.sum()
    )
    positions["weight"] = positions["weight"].clip(upper=settings.max_asset_weight)
    positions["weight"] = positions["weight"].fillna(0.0)

    estimated_volatility = np.nan
    if covariance is not None and not covariance.empty:
        tickers = positions["ticker"].tolist()
        matrix = covariance.reindex(index=tickers, columns=tickers).to_numpy(dtype=float)
        target = positions.set_index("ticker").loc[tickers, "weight"].to_numpy(dtype=float)
        if np.isfinite(matrix).all():
            variance = float(target @ matrix @ target)
            estimated_volatility = float(np.sqrt(max(variance, 0.0)))
    if settings.volatility_targeting and settings.volatility_target > 0 and np.isfinite(estimated_volatility):
        scale = min(1.0, settings.volatility_target / estimated_volatility) if estimated_volatility > 0 else 1.0
        positions["weight"] *= scale

    positions["cash_weight"] = max(0.0, 1.0 - float(positions["weight"].sum()))
    positions["estimated_portfolio_volatility"] = estimated_volatility
    return positions.reset_index(drop=True)


def target_values(weights: pd.DataFrame, capital: float) -> pd.DataFrame:
    """Add target monetary values; the final cash row is represented by cash_weight."""
    if capital < 0 or not np.isfinite(capital):
        raise ValueError("capital must be a finite non-negative amount")
    result = weights.copy()
    if not result.empty:
        result["target_value"] = result["weight"] * capital
        result["cash_value"] = result["cash_weight"] * capital
    return result
