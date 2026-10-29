"""Long-only portfolio target construction; selection remains in the strategy layer."""

from __future__ import annotations

from dataclasses import dataclass, field

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
    fractional_trading: bool = False
    var_confidence: float = 0.95
    var_horizons: tuple[int, ...] = (1, 21)
    var_lookback: int = 756
    max_var_1d: float | None = 0.05
    max_cvar_21d: float | None = 0.20
    max_stress_loss: float | None = 0.35
    stress_scenarios: dict[str, dict[str, object]] = field(default_factory=lambda: {
        "equity_bond_gold_scenario": {
            "exposure_groups": {
                "sp500": -0.30,
                "sp500_index": -0.30,
                "global_equity_all_world": -0.30,
                "developed_world": -0.30,
                "nasdaq100": -0.35,
                "europe_equity": -0.30,
                "europe_equity_600": -0.30,
                "germany_equity": -0.30,
                "developed_ex_us": -0.30,
                "emerging_markets": -0.30,
                "china_equity": -0.30,
                "asia_pacific_ex_japan": -0.30,
                "japan_equity": -0.30,
                "us_small_cap": -0.30,
                "world_small_cap": -0.30,
                "us_total_market": -0.30,
                "gold": 0.10,
                "us_aggregate_bonds": -0.10,
                "us_corporate_bonds": -0.10,
                "euro_corporate_bonds": -0.10,
                "global_aggregate_bonds_eur_hedged": -0.10,
                "global_inflation_linked_bonds": -0.10,
                "development_bank_bonds": -0.10,
                "us_high_yield_bonds": -0.10,
                "em_bonds_usd": -0.10,
                "long_us_treasuries": -0.10,
                "us_tips": -0.10,
            },
            "types": {"equity": -0.30},
            "default": 0.0,
        },
    })

    def __post_init__(self) -> None:
        if self.top_n < 1:
            raise ValueError("top_n must be positive")
        if not 0 < self.max_asset_weight <= 1 or not 0 < self.max_exposure_group_weight <= 1:
            raise ValueError("weight caps must be in (0, 1]")
        if self.volatility_target < 0 or self.covariance_window < 2:
            raise ValueError("volatility_target must be non-negative and covariance_window at least 2")
        if not 0 < self.var_confidence < 1:
            raise ValueError("var_confidence must be between 0 and 1")
        if not self.var_horizons or any(horizon < 1 for horizon in self.var_horizons):
            raise ValueError("var_horizons must contain positive day counts")
        if self.var_lookback < max(self.var_horizons):
            raise ValueError("var_lookback must be at least the longest VaR horizon")
        if any(value is not None and value < 0 for value in (
            self.max_var_1d, self.max_cvar_21d, self.max_stress_loss
        )):
            raise ValueError("VaR, CVaR, and stress-loss limits must be non-negative")
        for scenario_name, scenario in self.stress_scenarios.items():
            if not isinstance(scenario, dict):
                raise ValueError(f"Invalid stress scenario: {scenario_name}")
            for shock_map in (
                scenario.get("exposure_groups", {}),
                scenario.get("types", {}),
            ):
                if not isinstance(shock_map, dict) or any(
                    not np.isfinite(float(shock)) or float(shock) < -1
                    for shock in shock_map.values()
                ):
                    raise ValueError(f"Invalid shock values in scenario {scenario_name}")
            default = float(scenario.get("default", 0.0))
            if not np.isfinite(default) or default < -1:
                raise ValueError(f"Invalid default shock in scenario {scenario_name}")


def construct_target_weights(
    selected: pd.DataFrame,
    config: PortfolioConfig | None = None,
    covariance: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Create target weights from selected assets, leaving unallocated budget as cash."""
    settings = config or PortfolioConfig()
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
