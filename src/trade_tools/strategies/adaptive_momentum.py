"""Deterministic adaptive momentum ranking and point-in-time eligibility."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Any, Protocol

import numpy as np
import pandas as pd

from trade_tools.market_data.research_universe import classification_for


@dataclass(frozen=True)
class StrategyConfig:
    top_n: int = 5
    universe_mode: str = "tradable"
    minimum_history: int = 252
    history_buffer: int = 0
    max_stale_trading_days: int = 5
    volatility_window: int = 63
    trend_months: int = 10
    trend_days: int | None = None
    max_assets_per_exposure_group: int = 1
    cash_threshold: float = 0.0
    volatility_adjusted: bool = True
    absolute_momentum_filter: bool = True
    trend_filter: bool = True
    horizon_weights: tuple[float, float, float, float] = (12.0, 4.0, 2.0, 1.0)
    include_tickers: tuple[str, ...] = ()
    exclude_tickers: tuple[str, ...] = ()
    include_exposure_groups: tuple[str, ...] = ()
    exclude_exposure_groups: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.universe_mode not in {"tradable", "research"}:
            raise ValueError("universe_mode must be 'tradable' or 'research'")
        if self.top_n < 1 or self.minimum_history < 1 or self.history_buffer < 0:
            raise ValueError("top_n and minimum_history must be positive; history_buffer cannot be negative")
        if self.volatility_window < 2 or self.trend_months < 1:
            raise ValueError("volatility_window must be at least 2 and trend_months must be positive")
        if self.trend_days is not None and self.trend_days < 1:
            raise ValueError("trend_days must be positive when configured")
        if self.max_assets_per_exposure_group < 1:
            raise ValueError("max_assets_per_exposure_group must be positive")
        if len(self.horizon_weights) != 4:
            raise ValueError("horizon_weights must contain weights for r1, r3, r6 and r12")
        if any(weight < 0 for weight in self.horizon_weights) or sum(self.horizon_weights) <= 0:
            raise ValueError("horizon_weights must be non-negative with a positive sum")

    @property
    def config_hash(self) -> str:
        encoded = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return sha256(encoded.encode()).hexdigest()[:16]


class Strategy(Protocol):
    name: str
    version: str
    required_features: tuple[str, ...]

    def fit(self, feature_panel: pd.DataFrame, labels: pd.DataFrame, as_of: pd.Timestamp) -> None: ...
    def score(self, features_at_t: pd.DataFrame) -> pd.Series: ...


class AdaptiveMomentumStrategy:
    name = "adaptive_momentum"
    version = "1.0"
    required_features = ("r1", "r3", "r6", "r12", "realized_vol", "trend_distance")

    def __init__(self, config: StrategyConfig | None = None):
        self.config = config or StrategyConfig()

    @property
    def parameters(self) -> dict[str, Any]:
        return asdict(self.config)

    def fit(self, feature_panel: pd.DataFrame, labels: pd.DataFrame, as_of: pd.Timestamp) -> None:
        """Baseline is rule-based and does not fit on labels."""

    def score(self, features_at_t: pd.DataFrame) -> pd.Series:
        weights = self.config.horizon_weights
        momentum = (
            weights[0] * features_at_t["r1"]
            + weights[1] * features_at_t["r3"]
            + weights[2] * features_at_t["r6"]
            + weights[3] * features_at_t["r12"]
        ) / len(weights)
        if self.config.volatility_adjusted:
            volatility = features_at_t["realized_vol"].where(features_at_t["realized_vol"] > 0)
            momentum = momentum / volatility
        return momentum.rename("score")

    def rank(
        self,
        features_at_t: pd.DataFrame,
        metadata: dict[str, dict[str, Any]],
        decision_date: pd.Timestamp,
        *,
        latest_observations: dict[str, pd.Timestamp] | None = None,
        history_counts: dict[str, int] | None = None,
        labels: pd.DataFrame | None = None,
        feature_panel: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Return one report row per asset, including ineligible rows with reasons."""
        point_in_time_labels = labels if labels is not None else pd.DataFrame()
        as_of = pd.Timestamp(decision_date)
        if as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        if not point_in_time_labels.empty and "decision_date" in point_in_time_labels:
            point_in_time_labels = point_in_time_labels.loc[
                pd.to_datetime(point_in_time_labels["decision_date"], utc=True)
                < as_of
            ]
        point_in_time_panel = feature_panel if feature_panel is not None else features_at_t
        if not point_in_time_panel.empty and "decision_date" in point_in_time_panel:
            point_in_time_panel = point_in_time_panel.loc[
                pd.to_datetime(point_in_time_panel["decision_date"], utc=True) < as_of
            ]
        self.fit(
            point_in_time_panel,
            point_in_time_labels,
            pd.Timestamp(decision_date),
        )
        data = features_at_t.copy()
        if "ticker" not in data:
            data["ticker"] = data.index.astype(str)
        data = data.set_index("ticker", drop=False)
        scores = self.score(data)
        minimum = self.config.minimum_history + self.config.history_buffer
        rows: list[dict[str, Any]] = []
        decision = pd.Timestamp(decision_date)
        if decision.tzinfo is None:
            decision = decision.tz_localize("UTC")
        for ticker, asset in metadata.items():
            classification = classification_for(ticker)
            reason = None
            row = data.loc[ticker] if ticker in data.index else pd.Series(dtype=object)
            if classification is None:
                reason = "unclassified_asset"
            elif not classification.is_fund:
                reason = "not_a_fund"
            elif self.config.universe_mode == "tradable" and not classification.tradable:
                reason = "not_tradable"
            elif not asset.get("currency") or asset.get("conversion_reason") in {
                "missing_currency", "unsupported_currency"
            }:
                reason = "missing_currency"
            elif ticker in self.config.exclude_tickers:
                reason = "configured_exclusion"
            elif classification.exposure_group in self.config.exclude_exposure_groups:
                reason = "configured_exclusion"
            elif self.config.include_tickers and ticker not in self.config.include_tickers:
                reason = "configured_exclusion"
            elif (
                self.config.include_exposure_groups
                and classification.exposure_group not in self.config.include_exposure_groups
            ):
                reason = "configured_exclusion"
            elif history_counts is not None and history_counts.get(ticker, 0) < minimum:
                reason = "insufficient_history"
            elif latest_observations is not None and ticker not in latest_observations:
                reason = "stale_data"
            elif latest_observations is not None:
                observed = pd.Timestamp(latest_observations[ticker])
                if observed.tzinfo is None:
                    observed = observed.tz_localize("UTC")
                trading_days_stale = len(pd.bdate_range(observed.normalize(), decision.normalize())) - 1
                if trading_days_stale > self.config.max_stale_trading_days:
                    reason = "stale_data"
            if reason is None and (ticker not in scores.index or not np.isfinite(scores.loc[ticker])):
                reason = "insufficient_history"
            values = row.to_dict()
            rows.append({
                **{key: asset.get(key) for key in ("id", "ticker", "type", "country", "sector")},
                "ticker": ticker,
                "decision_timestamp": decision,
                "strategy_name": self.name,
                "strategy_version": self.version,
                "score": float(scores.loc[ticker]) if ticker in scores.index and pd.notna(scores.loc[ticker]) else np.nan,
                "exposure_group": classification.exposure_group if classification else None,
                "preferred": classification.preferred if classification else False,
                "history_count": history_counts.get(ticker, 0) if history_counts is not None else 0,
                "eligible": reason is None,
                "exclusion_reason": reason,
                "selected": False,
                "rank": None,
                "data_quality_status": "usable" if reason is None else reason,
                "explanation": None,
                **{key: values.get(key) for key in self.required_features},
            })

        report = pd.DataFrame(rows)
        eligible = report.loc[report["eligible"]].sort_values(
            ["score", "ticker"], ascending=[False, True], kind="mergesort"
        )
        report.loc[eligible.index, "rank"] = np.arange(1, len(eligible) + 1)
        group_counts: dict[str, int] = {}
        selected: list[int] = []
        for index, row in eligible.iterrows():
            group = row["exposure_group"]
            if group_counts.get(group, 0) >= self.config.max_assets_per_exposure_group:
                report.loc[index, "exclusion_reason"] = "exposure_group_limit"
                report.loc[index, "data_quality_status"] = "eligible_not_selected"
                continue
            momentum = sum(
                weight * (row.get(feature) if pd.notna(row.get(feature)) else 0)
                for weight, feature in zip(self.config.horizon_weights, ("r1", "r3", "r6", "r12"))
            ) / len(self.config.horizon_weights)
            passes = (
                (not self.config.absolute_momentum_filter or momentum > self.config.cash_threshold)
                and (
                    not self.config.trend_filter
                    or (pd.notna(row.get("trend_distance")) and row["trend_distance"] > 0)
                )
            )
            if passes and len(selected) < self.config.top_n:
                group_counts[group] = group_counts.get(group, 0) + 1
                selected.append(index)
                report.loc[index, "selected"] = True
                report.loc[index, "explanation"] = (
                    "Momentum and trend filters passed"
                    if self.config.absolute_momentum_filter or self.config.trend_filter
                    else "Highest-ranked eligible asset"
                )
            elif not passes:
                report.loc[index, "exclusion_reason"] = (
                    "absolute_momentum_filter"
                    if self.config.absolute_momentum_filter and momentum <= self.config.cash_threshold
                    else "trend_filter"
                )
                report.loc[index, "data_quality_status"] = "eligible_not_selected"
            else:
                report.loc[index, "data_quality_status"] = "eligible_not_selected"
        return report.sort_values(["rank", "ticker"], na_position="last", kind="mergesort").reset_index(drop=True)
