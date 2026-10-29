"""Load and validate the small JSON configuration used by research commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from trade_tools.portfolio.construction import PortfolioConfig
from trade_tools.strategies.adaptive_momentum import StrategyConfig
from trade_tools.strategies.backtest import BacktestConfig


DEFAULT_RESEARCH_CONFIG = Path("config/research.json")


def load_research_config(
    path: str | Path = DEFAULT_RESEARCH_CONFIG,
) -> tuple[BacktestConfig, StrategyConfig, PortfolioConfig]:
    config_path = Path(path)
    try:
        raw: dict[str, Any] = json.loads(config_path.read_text())
    except FileNotFoundError as exc:
        raise ValueError(f"Research configuration file not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in research configuration file: {config_path}") from exc
    try:
        backtest = dict(raw.get("backtest", {}))
        strategy = dict(raw.get("strategy", {}))
        portfolio = dict(raw.get("portfolio", {}))
        portfolio["var_horizons"] = tuple(portfolio.get("var_horizons", (1, 21)))
        strategy["horizon_weights"] = tuple(strategy.get("horizon_weights", (12, 4, 2, 1)))
        strategy["include_tickers"] = tuple(strategy.get("include_tickers", ()))
        strategy["exclude_tickers"] = tuple(strategy.get("exclude_tickers", ()))
        strategy["include_exposure_groups"] = tuple(strategy.get("include_exposure_groups", ()))
        strategy["exclude_exposure_groups"] = tuple(strategy.get("exclude_exposure_groups", ()))
        return (
            BacktestConfig(**backtest),
            StrategyConfig(**strategy),
            PortfolioConfig(**portfolio),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid research configuration values in: {config_path}") from exc
