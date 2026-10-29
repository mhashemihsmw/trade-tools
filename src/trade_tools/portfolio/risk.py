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
    var_confidence: float = 0.95
    var_horizons: tuple[int, ...] = (1, 21)
    var_lookback: int = 756
    max_var_1d: float | None = None
    max_cvar_21d: float | None = None
    max_stress_loss: float | None = None

    def __post_init__(self) -> None:
        if not 0 < self.var_confidence < 1:
            raise ValueError("var_confidence must be between 0 and 1")
        if not self.var_horizons or any(horizon < 1 for horizon in self.var_horizons):
            raise ValueError("var_horizons must contain positive day counts")
        if self.var_lookback < max(self.var_horizons):
            raise ValueError("var_lookback must be at least the longest VaR horizon")
        if any(value is not None and (not np.isfinite(value) or value < 0) for value in (
            self.max_var_1d, self.max_cvar_21d, self.max_stress_loss
        )):
            raise ValueError("VaR, CVaR, and stress-loss limits must be non-negative")


def historical_var_cvar(
    returns: pd.Series,
    *,
    confidence: float = 0.95,
    horizons: tuple[int, ...] = (1, 21),
    lookback: int = 756,
) -> dict[str, Any]:
    """Historical overlapping-window VaR/CVaR reported as positive loss fractions.

    Each horizon uses all rolling compounded-return windows. The empirical
    threshold is NumPy's lower order statistic; CVaR averages every window at
    or below that threshold, including observations equal to the threshold.
    """
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    if lookback < max(horizons, default=1) or any(horizon < 1 for horizon in horizons):
        raise ValueError("lookback must cover positive horizons")
    values = pd.to_numeric(returns, errors="coerce").dropna().tail(lookback).to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= -1).any():
        raise ValueError("returns must be finite and greater than -100%")
    result: dict[str, Any] = {}
    for horizon in horizons:
        if len(values) < horizon:
            result[f"{horizon}d"] = {
                "observations": 0, "threshold_return": None, "var_loss": None, "cvar_loss": None,
            }
            continue
        windows = np.lib.stride_tricks.sliding_window_view(values, horizon)
        compounded = np.prod(1 + windows, axis=1) - 1
        threshold = float(np.quantile(compounded, 1 - confidence, method="lower"))
        tail = compounded[compounded <= threshold]
        result[f"{horizon}d"] = {
            "observations": int(len(compounded)),
            "threshold_return": threshold,
            "var_loss": max(0.0, -threshold),
            "cvar_loss": max(0.0, -float(tail.mean())),
        }
    return result


def stress_scenario_returns(
    weights: pd.DataFrame,
    scenarios: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Apply configured exposure-group/type shocks as hypothetical scenarios."""
    outputs: dict[str, dict[str, float]] = {}
    for name, scenario in scenarios.items():
        portfolio_return = 0.0
        for row in weights.itertuples(index=False):
            weight = float(row.weight)
            exposure = getattr(row, "exposure_group", None)
            asset_type = getattr(row, "type", None)
            shock = scenario.get("default", 0.0)
            if exposure in scenario.get("exposure_groups", {}):
                shock = scenario["exposure_groups"][exposure]
            elif asset_type in scenario.get("types", {}):
                shock = scenario["types"][asset_type]
            portfolio_return += weight * float(shock)
        outputs[name] = {
            "portfolio_return": portfolio_return,
            "loss": max(0.0, -portfolio_return),
        }
    return outputs


def validate_target_weights(
    weights: pd.DataFrame,
    limits: RiskLimits | None = None,
    *,
    asset_returns: pd.DataFrame | None = None,
    asset_prices: pd.DataFrame | None = None,
    portfolio_returns: pd.Series | None = None,
    stress_scenarios: dict[str, dict[str, Any]] | None = None,
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
        group_labels = positions["exposure_group"].astype("string").fillna("Unknown")
        group_weights = positions.groupby(group_labels)["weight"].sum()
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
    if portfolio_returns is None and asset_returns is not None and not positions.empty:
        tickers = positions["ticker"].tolist()
        returns = asset_returns.reindex(columns=tickers).dropna(how="any")
        if not returns.empty:
            target_weights = positions.set_index("ticker").loc[tickers, "weight"]
            portfolio_returns = returns.mul(target_weights, axis=1).sum(axis=1)
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
    historical_risk = (
        historical_var_cvar(
            portfolio_returns,
            confidence=settings.var_confidence,
            horizons=settings.var_horizons,
            lookback=settings.var_lookback,
        )
        if portfolio_returns is not None else {}
    )
    one_day = historical_risk.get("1d", {}).get("var_loss")
    twenty_one_day = historical_risk.get("21d", {}).get("cvar_loss")
    if settings.max_var_1d is not None and one_day is not None and one_day > settings.max_var_1d:
        violations.append("Historical 1-day VaR exceeds the configured limit.")
    if settings.max_cvar_21d is not None and twenty_one_day is not None and twenty_one_day > settings.max_cvar_21d:
        violations.append("Historical 21-day CVaR exceeds the configured limit.")
    scenario_results = stress_scenario_returns(positions, stress_scenarios or {})
    if settings.max_stress_loss is not None and any(
        scenario["loss"] > settings.max_stress_loss
        for scenario in scenario_results.values()
    ):
        violations.append("One or more configured stress-scenario losses exceed the limit.")
    if portfolio_returns is not None and len(portfolio_returns.dropna()) < settings.var_lookback:
        warnings.append("Historical VaR/CVaR uses less data than the configured look-back.")
    if portfolio_returns is None:
        warnings.append("Historical VaR/CVaR unavailable: portfolio returns are missing.")
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
        "metrics": {
            "annualized_realized_volatility": portfolio_vol,
            "maximum_drawdown": drawdowns,
            "historical_var_cvar": historical_risk,
            "stress_scenarios": scenario_results,
        },
    }
