"""Chronological monthly research backtester and reproducible report writer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trade_tools.features.research import feature_frame, sample_covariance
from trade_tools.features.research import aligned_daily_returns
from trade_tools.market_data.research_universe import classification_for
from trade_tools.portfolio.construction import PortfolioConfig, construct_target_weights
from trade_tools.portfolio.risk import RiskLimits, validate_target_weights
from trade_tools.strategies.adaptive_momentum import AdaptiveMomentumStrategy, StrategyConfig


@dataclass(frozen=True)
class BacktestConfig:
    start: str | None = None
    end: str | None = None
    universe_mode: str = "tradable"
    base_currency: str = "EUR"
    backtest_capital: float = 10_000.0
    proportional_cost_bps: float = 10.0
    fixed_fee: float = 2.0
    minimum_trade_size: float = 0.0
    annual_cash_rate: float = 0.0
    annual_risk_free_rate: float = 0.0
    holdout_months: int = 36
    include_holdout: bool = False
    passive_proxy_enabled: bool = True
    monthly_contribution: float = 0.0

    def __post_init__(self) -> None:
        if self.universe_mode not in {"tradable", "research"}:
            raise ValueError("universe_mode must be 'tradable' or 'research'")
        if self.base_currency.upper() not in {"EUR", "USD", "GBP", "JPY", "CHF"}:
            raise ValueError("base_currency must be one of EUR, USD, GBP, JPY or CHF")
        if self.backtest_capital <= 0 or self.minimum_trade_size < 0:
            raise ValueError("backtest_capital must be positive and minimum_trade_size non-negative")
        if self.proportional_cost_bps < 0 or self.fixed_fee < 0:
            raise ValueError("transaction costs cannot be negative")
        if self.holdout_months < 0 or self.monthly_contribution < 0:
            raise ValueError("holdout_months and monthly_contribution cannot be negative")


def monthly_decision_dates(prices_by_ticker: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    """Use fund-market observations for month ends, avoiding weekend-only assets."""
    fund_frames = {}
    for ticker, frame in prices_by_ticker.items():
        classification = classification_for(ticker)
        if classification is not None and classification.is_fund:
            fund_frames[ticker] = frame
    source_frames = fund_frames or prices_by_ticker
    timestamps = pd.DatetimeIndex(sorted({
        timestamp
        for frame in source_frames.values()
        for timestamp in frame.index
    }))
    if timestamps.empty:
        return []
    periods = timestamps.tz_localize(None).to_period("M")
    return list(pd.Series(timestamps, index=timestamps).groupby(periods).last())


def forward_holding_returns(
    prices_by_ticker: dict[str, pd.DataFrame],
    decision_dates: list[pd.Timestamp],
    *,
    volatility_by_date: dict[tuple[pd.Timestamp, str], float] | None = None,
) -> pd.DataFrame:
    """Shared forward-label definition: adjusted open after decision to next rebalance open."""
    rows: list[dict[str, Any]] = []
    for current, following in zip(decision_dates, decision_dates[1:]):
        for ticker, frame in prices_by_ticker.items():
            if "adjusted_open" not in frame:
                continue
            opens = pd.to_numeric(frame["adjusted_open"], errors="coerce").dropna()
            entry = opens.loc[opens.index > current]
            exit_ = opens.loc[(opens.index > following)]
            if entry.empty or exit_.empty:
                value = np.nan
            else:
                entry_time, exit_time = entry.index[0], exit_.index[0]
                value = float(exit_.iloc[0] / entry.iloc[0] - 1) if exit_time > entry_time else np.nan
            label: dict[str, Any] = {
                "decision_date": current,
                "next_decision_date": following,
                "ticker": ticker,
                "forward_return": value,
            }
            if volatility_by_date is not None:
                known_volatility = volatility_by_date.get((current, ticker), np.nan)
                label["volatility_scaled_forward_return"] = (
                    value / known_volatility if np.isfinite(value) and known_volatility > 0 else np.nan
                )
            rows.append(label)
    return pd.DataFrame(rows)


def portfolio_metrics(
    returns: pd.Series,
    *,
    risk_free_rate: float = 0.0,
    turnover: pd.Series | None = None,
    costs: pd.Series | None = None,
    holdings: pd.Series | None = None,
    cash_weight: pd.Series | None = None,
) -> dict[str, Any]:
    """Calculate consistent monthly portfolio and trading metrics."""
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return {
            "cumulative_return": np.nan,
            "annualized_return": np.nan,
            "annualized_volatility": np.nan,
            "sharpe_ratio": np.nan,
            "maximum_drawdown": np.nan,
            "average_monthly_turnover": np.nan,
            "annualized_turnover": np.nan,
            "total_costs": float(costs.sum()) if costs is not None else 0.0,
            "periods": 0,
            "average_holdings": float(holdings.mean()) if holdings is not None and not holdings.empty else 0.0,
            "average_cash_weight": float(cash_weight.mean()) if cash_weight is not None and not cash_weight.empty else np.nan,
        }
    wealth = (1 + values).cumprod()
    years = len(values) / 12
    cumulative = float(wealth.iloc[-1] - 1)
    annualized = float(wealth.iloc[-1] ** (1 / years) - 1) if years else np.nan
    volatility = float(values.std(ddof=1) * np.sqrt(12)) if len(values) > 1 else np.nan
    monthly_rf = (1 + risk_free_rate) ** (1 / 12) - 1
    excess = values - monthly_rf
    sharpe = (
        float(excess.mean() / excess.std(ddof=1) * np.sqrt(12))
        if len(values) > 1 and excess.std(ddof=1) > 0 else np.nan
    )
    average_turnover = float(turnover.mean()) if turnover is not None and not turnover.empty else np.nan
    return {
        "cumulative_return": cumulative,
        "annualized_return": annualized,
        "annualized_volatility": volatility,
        "sharpe_ratio": sharpe,
        "maximum_drawdown": float((wealth / wealth.cummax() - 1).min()),
        "average_monthly_turnover": average_turnover,
        "annualized_turnover": average_turnover * 12 if np.isfinite(average_turnover) else np.nan,
        "total_costs": float(costs.sum()) if costs is not None else 0.0,
        "periods": int(len(values)),
        "average_holdings": float(holdings.mean()) if holdings is not None and not holdings.empty else 0.0,
        "average_cash_weight": float(cash_weight.mean()) if cash_weight is not None and not cash_weight.empty else np.nan,
    }


def ranking_quality_metrics(
    rankings: pd.DataFrame,
    labels: pd.DataFrame,
    equal_weight_returns: dict[pd.Timestamp, float] | None = None,
) -> pd.DataFrame:
    """Compute monthly rank IC, quintile spread and a labeled selection hit rate."""
    combined = rankings.merge(labels, on=["decision_timestamp", "ticker"], how="left")
    if "exposure_group" in combined:
        history = pd.to_numeric(combined["history_count"], errors="coerce").fillna(0)
        combined["_history"] = history
        combined["_preferred"] = combined["preferred"].astype(bool)
        combined = combined.sort_values(
            ["decision_timestamp", "exposure_group", "_preferred", "_history", "ticker"],
            ascending=[True, True, False, False, True],
            kind="mergesort",
        ).drop_duplicates(["decision_timestamp", "exposure_group"], keep="first")
    rows: list[dict[str, Any]] = []
    for decision_date, group in combined.loc[combined["eligible"]].groupby("decision_timestamp"):
        valid = group[["score", "forward_return"]].dropna()
        if len(valid) >= 2:
            score_ranks = valid["score"].rank(method="average")
            return_ranks = valid["forward_return"].rank(method="average")
            ic = score_ranks.corr(return_ranks) if score_ranks.nunique() > 1 and return_ranks.nunique() > 1 else np.nan
        else:
            ic = np.nan
        if len(valid) >= 2:
            ordered = valid.sort_values("score", ascending=False)
            quintile_size = max(1, math.ceil(len(ordered) / 5))
            spread = float(
                ordered["forward_return"].head(quintile_size).mean()
                - ordered["forward_return"].tail(quintile_size).mean()
            )
        else:
            spread = np.nan
        benchmark = (equal_weight_returns or {}).get(decision_date, np.nan)
        selected = group.loc[group["selected"] & group["forward_return"].notna()]
        hit_rate = (
            float((selected["forward_return"] > benchmark).mean())
            if len(selected) and np.isfinite(benchmark) else np.nan
        )
        rows.append({
            "decision_date": decision_date,
            "rank_information_coefficient": ic,
            "quintile_spread": spread,
            "selection_hit_rate_vs_equal_weight": hit_rate,
            "label": "share of selected assets beating equal-weight benchmark; not a trade win rate",
        })
    return pd.DataFrame(rows)


def _representatives(rankings: pd.DataFrame, metadata: dict[str, dict[str, Any]], counts: dict[str, int]) -> pd.DataFrame:
    eligible = rankings.loc[rankings["eligible"]].copy()
    if eligible.empty:
        return eligible
    eligible["_history"] = eligible["ticker"].map(counts).fillna(0)
    eligible["_preferred"] = eligible["preferred"].astype(bool)
    eligible = eligible.sort_values(
        ["exposure_group", "_preferred", "_history", "ticker"],
        ascending=[True, False, False, True],
        kind="mergesort",
    )
    return eligible.drop_duplicates("exposure_group", keep="first").drop(columns=["_history", "_preferred"])


def run_backtest(
    prices_by_ticker: dict[str, pd.DataFrame],
    metadata: dict[str, dict[str, Any]],
    *,
    strategy_config: StrategyConfig | None = None,
    portfolio_config: PortfolioConfig | None = None,
    config: BacktestConfig | None = None,
) -> dict[str, Any]:
    """Run the rule-based monthly strategy on already-loaded base-currency data."""
    settings = config or BacktestConfig()
    sc = strategy_config or StrategyConfig(universe_mode=settings.universe_mode)
    if sc.universe_mode != settings.universe_mode:
        raise ValueError("strategy and backtest universe_mode must match")
    pc = portfolio_config or PortfolioConfig(top_n=sc.top_n)
    decisions = monthly_decision_dates(prices_by_ticker)
    if settings.start:
        lower = pd.Timestamp(settings.start)
        lower = lower.tz_localize("UTC") if lower.tzinfo is None else lower.tz_convert("UTC")
        decisions = [date for date in decisions if date >= lower]
    if settings.end:
        upper = pd.Timestamp(settings.end)
        upper = upper.tz_localize("UTC") if upper.tzinfo is None else upper.tz_convert("UTC")
        decisions = [date for date in decisions if date <= upper]
    if decisions and not any(
        "adjusted_open" in frame and (pd.DatetimeIndex(frame.index) > decisions[-1]).any()
        for frame in prices_by_ticker.values()
    ):
        decisions = decisions[:-1]
    if len(decisions) < 2:
        raise ValueError("At least two monthly decision dates with data are required")
    strategy = AdaptiveMomentumStrategy(sc)
    feature_cache = {
        ticker: feature_frame(
            frame["adjusted_close"].dropna(),
            volatility_window=sc.volatility_window,
            trend_months=sc.trend_months,
            trend_days=sc.trend_days,
        )
        for ticker, frame in prices_by_ticker.items()
        if "adjusted_close" in frame
    }
    observation_cache = {
        ticker: frame["adjusted_close"].dropna()
        for ticker, frame in prices_by_ticker.items()
        if "adjusted_close" in frame
    }
    volatility_at_decision = {
        (decision, ticker): float(frame.loc[frame.index <= decision, "realized_vol"].iloc[-1])
        for ticker, frame in feature_cache.items()
        for decision in decisions
        if not frame.loc[frame.index <= decision, "realized_vol"].empty
        and pd.notna(frame.loc[frame.index <= decision, "realized_vol"].iloc[-1])
    }
    labels = forward_holding_returns(
        prices_by_ticker,
        decisions,
        volatility_by_date=volatility_at_decision,
    )
    labels_by_date = {
        pd.Timestamp(date): group.set_index("ticker")["forward_return"]
        for date, group in labels.groupby("decision_date")
    }
    all_rankings: list[pd.DataFrame] = []
    all_selections: list[pd.DataFrame] = []
    risk_validations: list[dict[str, Any]] = []
    returns_records: list[dict[str, Any]] = []
    turnover_values: list[float] = []
    cost_values: list[float] = []
    holdings_values: list[float] = []
    cash_values: list[float] = []
    current_weights: dict[str, float] = {}
    equal_current_weights: dict[str, float] = {}
    capital = settings.backtest_capital
    equal_capital = settings.backtest_capital
    passive_capital = settings.backtest_capital
    equal_weight_returns: dict[pd.Timestamp, float] = {}
    passive_ticker: str | None = None

    for position, decision in enumerate(decisions[:-1]):
        if settings.monthly_contribution:
            capital += settings.monthly_contribution
            equal_capital += settings.monthly_contribution
            passive_capital += settings.monthly_contribution
        features: list[pd.Series] = []
        latest: dict[str, pd.Timestamp] = {}
        counts: dict[str, int] = {}
        for ticker, frame in feature_cache.items():
            point = frame.loc[frame.index <= decision]
            observed = observation_cache[ticker].loc[observation_cache[ticker].index <= decision]
            if not point.empty:
                row = point.iloc[-1].copy()
                row["ticker"] = ticker
                features.append(row)
                latest[ticker] = observed.index[-1]
                counts[ticker] = int(len(observed))
        features_at_date = pd.DataFrame(features).set_index("ticker", drop=False) if features else pd.DataFrame()
        historical_panel_rows = []
        for ticker, frame in feature_cache.items():
            old = frame.loc[frame.index < decision]
            if not old.empty:
                hist = old.copy()
                hist["ticker"] = ticker
                hist["decision_date"] = hist.index
                historical_panel_rows.append(hist)
        historical_panel = pd.concat(historical_panel_rows) if historical_panel_rows else pd.DataFrame()
        past_labels = labels.loc[labels["decision_date"] < decision] if not labels.empty else pd.DataFrame()
        ranked = strategy.rank(
            features_at_date,
            metadata,
            decision,
            latest_observations=latest,
            history_counts=counts,
            labels=past_labels,
            feature_panel=historical_panel,
        )
        ranked["decision_timestamp"] = decision
        all_rankings.append(ranked)
        representatives = _representatives(ranked, metadata, counts)

        selected = ranked.loc[ranked["selected"]]
        prices_history = pd.DataFrame({
            ticker: series.loc[series.index <= decision]
            for ticker, series in observation_cache.items()
        })
        covariance = sample_covariance(
            prices_history.tail(pc.covariance_window + 1),
            window=pc.covariance_window,
        )
        targets = construct_target_weights(selected, pc, covariance)
        history_prices_for_risk = pd.DataFrame({
            ticker: series.loc[series.index <= decision]
            for ticker, series in observation_cache.items()
        })
        risk = validate_target_weights(
            targets,
            RiskLimits(
                max_asset_weight=pc.max_asset_weight,
                max_exposure_group_weight=pc.max_exposure_group_weight,
                volatility_target=pc.volatility_target,
                volatility_window=pc.covariance_window,
            ),
            asset_returns=aligned_daily_returns(history_prices_for_risk),
            asset_prices=history_prices_for_risk,
        )
        risk["decision_date"] = decision
        risk_validations.append(risk)
        if not targets.empty:
            targets["risk_status"] = risk["status"]
            targets["risk_violations"] = "; ".join(risk["violations"])
            targets["decision_timestamp"] = decision
            all_selections.append(targets)

        labels_this_period = labels_by_date.get(pd.Timestamp(decision), pd.Series(dtype=float))
        weighted_targets = dict(zip(targets.get("ticker", []), targets.get("weight", [])))
        # Equal-weight benchmark uses one eligible listing per exposure group.
        eligible_reps = representatives["ticker"].tolist()
        equal_values = labels_this_period.reindex(eligible_reps).dropna()
        desired_equal = (
            {ticker: 1 / len(eligible_reps) for ticker in eligible_reps}
            if eligible_reps else {}
        )

        cash_return = (1 + settings.annual_cash_rate) ** (1 / 12) - 1

        drifted = dict(current_weights)
        # Mark-to-market weights after the last holding period.
        if position > 0 and not returns_records[-1]["strategy_return_missing"]:
            previous_returns = labels_by_date.get(pd.Timestamp(decisions[position - 1]), pd.Series(dtype=float))
            prior_cash_return = cash_return
            gross = 1 + returns_records[-1]["strategy_return"]
            drifted = {
                ticker: weight * (1 + float(previous_returns.get(ticker, 0))) / gross
                for ticker, weight in current_weights.items()
            }
            prior_cash = max(0.0, 1.0 - sum(current_weights.values()))
            drifted["__cash__"] = prior_cash * (1 + prior_cash_return) / gross
        else:
            drifted = {"__cash__": 1.0}
        target_map = dict(drifted)
        order_values: dict[str, float] = {}
        for ticker in set(weighted_targets) | (set(drifted) - {"__cash__"}):
            desired = weighted_targets.get(ticker, 0.0)
            current = drifted.get(ticker, 0.0)
            if abs(desired - current) * equal_capital >= settings.minimum_trade_size:
                target_map[ticker] = desired
                if abs(desired - current) > 1e-12:
                    order_values[ticker] = abs(desired - current) * capital
        target_map.pop("__cash__", None)
        risky_total = sum(target_map.values())
        if risky_total > 1:
            excess = risky_total - 1
            buys = {
                ticker: target_map[ticker] - drifted.get(ticker, 0.0)
                for ticker in target_map
                if target_map[ticker] > drifted.get(ticker, 0.0)
            }
            total_buys = sum(buys.values())
            if total_buys < excess - 1e-9:
                raise ValueError("Minimum trade size made the target portfolio infeasible.")
            for ticker, buy_weight in buys.items():
                target_map[ticker] -= excess * buy_weight / total_buys
        order_values = {
            ticker: abs(target_map.get(ticker, 0.0) - drifted.get(ticker, 0.0)) * capital
            for ticker in set(target_map) | (set(drifted) - {"__cash__"})
            if abs(target_map.get(ticker, 0.0) - drifted.get(ticker, 0.0)) > 1e-12
        }
        cash_weight = max(0.0, 1.0 - sum(target_map.values()))
        portfolio_return = 0.0
        missing_position_return = False
        for ticker, weight in target_map.items():
            asset_return = labels_this_period.get(ticker, np.nan)
            if pd.isna(asset_return):
                missing_position_return = True
            else:
                portfolio_return += weight * float(asset_return)
        if missing_position_return:
            missing = [
                ticker for ticker in target_map
                if pd.isna(labels_this_period.get(ticker, np.nan))
            ]
            raise ValueError(
                f"Missing adjusted execution-price return after {decision.date()} for held assets: "
                + ", ".join(sorted(missing))
            )
        portfolio_return += cash_weight * cash_return
        turnover = 0.5 * (
            sum(abs(target_map.get(ticker, 0.0) - drifted.get(ticker, 0.0))
                for ticker in set(target_map) | (set(drifted) - {"__cash__"}))
            + abs(cash_weight - drifted.get("__cash__", 0.0))
        )
        costs = sum(value * settings.proportional_cost_bps / 10_000 + settings.fixed_fee for value in order_values.values())
        cost_fraction = costs / capital
        portfolio_return -= cost_fraction
        current_weights = {ticker: weight for ticker, weight in target_map.items() if weight > 0}
        capital *= 1 + portfolio_return
        turnover_values.append(turnover)
        cost_values.append(costs)
        holdings_values.append(float(len(current_weights)))
        cash_values.append(max(0.0, 1.0 - sum(current_weights.values())))

        if position == 0:
            equal_drifted = {"__cash__": 1.0}
        else:
            prior_equal = equal_current_weights
            previous_equal_returns = labels_by_date.get(pd.Timestamp(decisions[position - 1]), pd.Series(dtype=float))
            equal_gross = 1 + returns_records[-1]["equal_weight_return"]
            if equal_gross <= 0 or not np.isfinite(equal_gross):
                equal_drifted = {"__cash__": 1.0}
            else:
                equal_drifted = {
                    ticker: weight * (1 + float(previous_equal_returns.get(ticker, 0))) / equal_gross
                    for ticker, weight in prior_equal.items()
                }
                equal_drifted["__cash__"] = (
                    max(0.0, 1.0 - sum(prior_equal.values())) * (1 + cash_return) / equal_gross
                )
        equal_executed = dict(equal_drifted)
        equal_orders: dict[str, float] = {}
        for ticker in set(desired_equal) | (set(equal_drifted) - {"__cash__"}):
            desired = desired_equal.get(ticker, 0.0)
            current = equal_drifted.get(ticker, 0.0)
            if abs(desired - current) * capital >= settings.minimum_trade_size:
                equal_executed[ticker] = desired
                if abs(desired - current) > 1e-12:
                    equal_orders[ticker] = abs(desired - current) * capital
        equal_executed.pop("__cash__", None)
        equal_total = sum(equal_executed.values())
        if equal_total > 1:
            excess = equal_total - 1
            buys = {
                ticker: equal_executed[ticker] - equal_drifted.get(ticker, 0.0)
                for ticker in equal_executed
                if equal_executed[ticker] > equal_drifted.get(ticker, 0.0)
            }
            total_buys = sum(buys.values())
            if total_buys < excess - 1e-9:
                raise ValueError("Minimum trade size made equal-weight benchmark infeasible.")
            for ticker, buy_weight in buys.items():
                equal_executed[ticker] -= excess * buy_weight / total_buys
        equal_cash = max(0.0, 1.0 - sum(equal_executed.values()))
        equal_costs = sum(
            abs(equal_executed.get(ticker, 0.0) - equal_drifted.get(ticker, 0.0))
            * equal_capital * settings.proportional_cost_bps / 10_000 + settings.fixed_fee
            for ticker in set(equal_executed) | (set(equal_drifted) - {"__cash__"})
            if abs(equal_executed.get(ticker, 0.0) - equal_drifted.get(ticker, 0.0)) > 1e-12
        )
        equal_missing_return = any(
            ticker not in labels_this_period or pd.isna(labels_this_period[ticker])
            for ticker in equal_executed
        )
        if equal_missing_return:
            missing = [
                ticker for ticker in equal_executed
                if ticker not in labels_this_period or pd.isna(labels_this_period[ticker])
            ]
            raise ValueError(
                f"Missing forward-price label after {decision.date()} for equal-weight benchmark assets: "
                + ", ".join(sorted(missing))
            )
        equal_return = sum(
            weight * float(labels_this_period[ticker])
            for ticker, weight in equal_executed.items()
            if ticker in labels_this_period and pd.notna(labels_this_period[ticker])
        ) + equal_cash * cash_return
        equal_return -= equal_costs / equal_capital
        equal_turnover = 0.5 * (
            sum(abs(equal_executed.get(ticker, 0.0) - equal_drifted.get(ticker, 0.0))
                for ticker in set(equal_executed) | (set(equal_drifted) - {"__cash__"}))
            + abs(equal_cash - equal_drifted.get("__cash__", 0.0))
        )
        equal_return = np.nan if equal_missing_return else equal_return
        equal_current_weights = {ticker: weight for ticker, weight in equal_executed.items() if weight > 0}
        if np.isfinite(equal_return):
            equal_capital *= 1 + equal_return
        equal_weight_returns[decision] = equal_return

        requested = (
            targets[["ticker", "weight", "risk_status", "risk_violations"]]
            .rename(columns={"weight": "requested_weight"})
            if not targets.empty else pd.DataFrame(columns=[
                "ticker", "requested_weight", "risk_status", "risk_violations"
            ])
        )
        executed = pd.DataFrame(
            [{"ticker": ticker, "executed_weight": weight} for ticker, weight in target_map.items()],
            columns=["ticker", "executed_weight"],
        )
        all_rankings[-1] = ranked.merge(requested, on="ticker", how="left").merge(
            executed, on="ticker", how="left"
        )
        returns_records.append({
            "decision_date": decision,
            "strategy_return": np.nan if missing_position_return else portfolio_return,
            "strategy_return_missing": missing_position_return,
            "equal_weight_return": equal_return,
            "equal_weight_turnover": equal_turnover,
            "equal_weight_costs": equal_costs,
            "passive_return": np.nan,
            "passive_costs": 0.0,
            "passive_turnover": 0.0,
            "turnover": turnover,
            "costs": costs,
            "holdings": len(current_weights),
            "cash_weight": max(0.0, 1.0 - sum(current_weights.values())),
            "eligible_assets": int(ranked["eligible"].sum()),
        })

        passive_source: str | None = None
        for candidate in ("VWCE.DE", "VT"):
            if candidate == "VT" and not settings.passive_proxy_enabled:
                continue
            if candidate not in observation_cache:
                continue
            prices = observation_cache[candidate]
            entry = prices.loc[prices.index > decision]
            exit_ = prices.loc[prices.index > decisions[position + 1]]
            if not entry.empty and not exit_.empty:
                passive_source = candidate
                passive = float(exit_.iloc[0] / entry.iloc[0] - 1)
                break
        if passive_source:
            passive_return = passive
            passive_orders = 1 if passive_ticker is None else (2 if passive_ticker != passive_source else 0)
            passive_cost = (
                passive_capital * settings.proportional_cost_bps / 10_000 * passive_orders
                + settings.fixed_fee * passive_orders
            )
            passive_return -= passive_cost / passive_capital
            passive_capital *= 1 + passive_return
            returns_records[-1]["passive_return"] = passive_return
            returns_records[-1]["passive_costs"] = passive_cost
            returns_records[-1]["passive_turnover"] = 1.0 if passive_ticker is None else (
                1.0 if passive_ticker != passive_source else 0.0
            )
            passive_ticker = passive_source

    monthly = pd.DataFrame(returns_records)
    rank_frame = pd.concat(all_rankings, ignore_index=True) if all_rankings else pd.DataFrame()
    label_quality = labels.rename(columns={"decision_date": "decision_timestamp"})
    quality = ranking_quality_metrics(rank_frame, label_quality, equal_weight_returns) if not rank_frame.empty else pd.DataFrame()
    # Default output excludes the most recent holdout from headline metrics.
    split = max(0, len(monthly) - settings.holdout_months)
    if settings.include_holdout:
        monthly["period"] = ["in_sample" if i < split else "holdout" for i in range(len(monthly))]
        date_period = dict(zip(monthly["decision_date"], monthly["period"]))
        if not quality.empty:
            quality["period"] = quality["decision_date"].map(date_period)
    else:
        monthly = monthly.iloc[:split].copy()
        if monthly.empty:
            raise ValueError("No in-sample periods remain after reserving the configured holdout")
        if not quality.empty:
            quality = quality.loc[quality["decision_date"].isin(monthly["decision_date"])].copy()
    ic_values = quality["rank_information_coefficient"].dropna() if not quality.empty else pd.Series(dtype=float)
    ic_std = float(ic_values.std(ddof=1)) if len(ic_values) > 1 else np.nan
    ranking_summary = {
        "monthly_rank_information_coefficient_mean": float(ic_values.mean()) if len(ic_values) else np.nan,
        "monthly_rank_information_coefficient_std": ic_std,
        "monthly_rank_information_coefficient_t_statistic": (
            float(ic_values.mean() / (ic_std / np.sqrt(len(ic_values))))
            if len(ic_values) > 1 and ic_std > 0 else np.nan
        ),
        "share_of_months_with_positive_ic": float((ic_values > 0).mean()) if len(ic_values) else np.nan,
        "mean_top_minus_bottom_quintile_spread": (
            float(quality["quintile_spread"].mean()) if not quality.empty else np.nan
        ),
        "selection_hit_rate_vs_equal_weight": (
            float(quality["selection_hit_rate_vs_equal_weight"].mean())
            if not quality.empty else np.nan
        ),
    }
    metrics = {
        "strategy": portfolio_metrics(
            monthly["strategy_return"],
            risk_free_rate=settings.annual_risk_free_rate,
            turnover=monthly["turnover"],
            costs=monthly["costs"],
            holdings=monthly["holdings"],
            cash_weight=monthly["cash_weight"],
        ),
        "equal_weight": portfolio_metrics(
            monthly["equal_weight_return"],
            risk_free_rate=settings.annual_risk_free_rate,
            turnover=monthly["equal_weight_turnover"],
            costs=monthly["equal_weight_costs"],
        ),
        "passive": portfolio_metrics(
            monthly["passive_return"],
            risk_free_rate=settings.annual_risk_free_rate,
            turnover=monthly["passive_turnover"],
            costs=monthly["passive_costs"],
            holdings=pd.Series(1, index=monthly.index),
            cash_weight=pd.Series(0, index=monthly.index),
        ),
        "ranking_quality": ranking_summary,
    }
    if settings.include_holdout:
        metrics["by_period"] = {
            period: {
                "strategy": portfolio_metrics(
                    group["strategy_return"],
                    risk_free_rate=settings.annual_risk_free_rate,
                    turnover=group["turnover"],
                    costs=group["costs"],
                    holdings=group["holdings"],
                    cash_weight=group["cash_weight"],
                ),
                "equal_weight": portfolio_metrics(
                    group["equal_weight_return"],
                    risk_free_rate=settings.annual_risk_free_rate,
                    turnover=group["equal_weight_turnover"],
                    costs=group["equal_weight_costs"],
                ),
                "passive": portfolio_metrics(
                    group["passive_return"],
                    risk_free_rate=settings.annual_risk_free_rate,
                    turnover=group["passive_turnover"],
                    costs=group["passive_costs"],
                    holdings=pd.Series(1, index=group.index),
                    cash_weight=pd.Series(0, index=group.index),
                ),
            }
            for period, group in monthly.groupby("period")
        }
        metrics["ranking_quality_by_period"] = {
            period: {
                "mean_ic": float(group["rank_information_coefficient"].mean()),
                "mean_quintile_spread": float(group["quintile_spread"].mean()),
                "selection_hit_rate_vs_equal_weight": float(
                    group["selection_hit_rate_vs_equal_weight"].mean()
                ),
            }
            for period, group in quality.groupby("period")
        } if not quality.empty else {}
    all_dates = sorted({
        timestamp for series in observation_cache.values() for timestamp in series.index
    })
    next_used_date = next((timestamp for timestamp in all_dates if timestamp > decisions[-1]), decisions[-1])
    return {
        "config": {
            "backtest": asdict(settings),
            "strategy": asdict(sc),
            "portfolio": asdict(pc),
            "strategy_name": strategy.name,
            "strategy_version": strategy.version,
            "universe_mode": settings.universe_mode,
            "data_start": min((series.index.min() for series in observation_cache.values()), default=None),
            "data_end": next_used_date,
        },
        "monthly_returns": monthly,
        "rankings": rank_frame,
        "selections": pd.concat(all_selections, ignore_index=True) if all_selections else pd.DataFrame(),
        "risk_validations": risk_validations,
        "ic": quality,
        "metrics": metrics,
        "disclosures": [
            f"Universe mode: {settings.universe_mode}.",
            "Survivorship bias: the universe was hand-picked in 2025–26.",
            "Most UCITS listings start between about 2010 and 2019; early research history is US-heavy and tradable history is smaller.",
            "Yahoo adjusted-close history can be restated; run the full adjusted-history refresh before research runs.",
            "FX uses latest available stored close at or before each observation; no constituent look-through is available.",
            "Baseline parameters are fixed in advance and were not tuned. Positive results are not evidence of future profitability.",
            f"Cash earns {settings.annual_cash_rate:.2%} annually; risk-free rate for Sharpe is {settings.annual_risk_free_rate:.2%}.",
            f"Monthly contributions: EUR {settings.monthly_contribution:.2f}; reported returns are time-weighted.",
            f"Eligible asset counts per year: {monthly.assign(year=pd.to_datetime(monthly['decision_date'], utc=True).dt.year).groupby('year')['eligible_assets'].mean().round(2).to_dict() if not monthly.empty else {}}",
        ],
    }


def write_backtest_report(result: dict[str, Any], reports_dir: str | Path = "reports/backtests") -> Path:
    """Persist deterministic report files under a configuration/data-derived run ID."""
    output = Path(reports_dir)
    config_json = json.dumps(result["config"], sort_keys=True, default=str)
    data_end = str(result["config"].get("data_end"))
    run_id = sha256(f"{config_json}|{data_end}".encode()).hexdigest()[:16]
    run_dir = output / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    serializable_config = json.loads(config_json)
    (run_dir / "config.json").write_text(json.dumps(serializable_config, indent=2, sort_keys=True) + "\n")
    summary = {
        "run_id": run_id,
        "metrics": result["metrics"],
        "risk_validations": result["risk_validations"],
        "disclosures": result["disclosures"],
    }
    (run_dir / "summary.json").write_text(
        json.dumps(_json_value(summary), indent=2, sort_keys=True, default=_json_value, allow_nan=False) + "\n"
    )
    lines = ["# Backtest Summary", "", f"Run ID: `{run_id}`", ""]
    lines += [f"- {line}" for line in result["disclosures"]]
    lines += [
        "", "## Metrics", "", "```json",
        json.dumps(_json_value(result["metrics"]), indent=2, default=_json_value, allow_nan=False),
        "```", "",
    ]
    (run_dir / "summary.md").write_text("\n".join(lines))
    result["monthly_returns"].to_csv(run_dir / "monthly_returns.csv", index=False)
    result["rankings"].to_csv(run_dir / "selections.csv", index=False)
    result["ic"].to_csv(run_dir / "ic.csv", index=False)
    return run_dir


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.floating, float)) and not np.isfinite(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value
