"""Strategy settings (config/strategy.toml) and the candidate universe (config/universe.csv)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, fields
from pathlib import Path

import pandas as pd

DEFAULT_SETTINGS_PATH = Path("config/strategy.toml")
DEFAULT_UNIVERSE_PATH = Path("config/universe.csv")


@dataclass(frozen=True)
class Settings:
    # selection
    top_n: int = 5
    tradable_only: bool = True
    min_history_days: int = 252
    max_stale_days: int = 5
    absolute_momentum_filter: bool = True
    trend_filter: bool = True
    # signals
    momentum_weights: tuple[float, float, float, float] = (12, 4, 2, 1)
    volatility_window: int = 63
    volatility_adjusted: bool = True
    trend_months: int = 10
    # weights
    inverse_volatility: bool = True
    max_weight: float = 0.30
    volatility_target: float = 0.10
    covariance_window: int = 63
    # costs
    cost_bps: float = 10.0
    fixed_fee: float = 2.0
    min_trade_eur: float = 0.0
    fractional_shares: bool = False
    # backtest
    start: str = ""
    end: str = ""
    initial_capital: float = 10_000.0
    monthly_contribution: float = 0.0
    cash_rate: float = 0.0
    risk_free_rate: float = 0.0
    holdout_months: int = 36
    benchmark: tuple[str, ...] = ("VWCE.DE", "VT")

    def __post_init__(self) -> None:
        object.__setattr__(self, "momentum_weights", tuple(float(w) for w in self.momentum_weights))
        object.__setattr__(self, "benchmark", tuple(self.benchmark))
        checks = {
            "top_n must be >= 1": self.top_n >= 1,
            "momentum_weights needs 4 non-negative values": (
                len(self.momentum_weights) == 4 and min(self.momentum_weights) >= 0
                and sum(self.momentum_weights) > 0
            ),
            "volatility_window and covariance_window must be >= 2": (
                self.volatility_window >= 2 and self.covariance_window >= 2
            ),
            "max_weight must be in (0, 1]": 0 < self.max_weight <= 1,
            "volatility_target cannot be negative": self.volatility_target >= 0,
            "costs cannot be negative": min(self.cost_bps, self.fixed_fee, self.min_trade_eur) >= 0,
            "initial_capital must be positive": self.initial_capital > 0,
        }
        problems = [message for message, ok in checks.items() if not ok]
        if problems:
            raise ValueError("Invalid settings: " + "; ".join(problems))


def load_settings(path: str | Path = DEFAULT_SETTINGS_PATH) -> Settings:
    """Read the TOML file; its sections are only for readability and are flattened."""
    raw = tomllib.loads(Path(path).read_text())
    flat = {key: value for section in raw.values() for key, value in section.items()}
    known = {field.name for field in fields(Settings)}
    unknown = sorted(set(flat) - known)
    if unknown:
        raise ValueError(f"Unknown settings in {path}: {', '.join(unknown)}")
    return Settings(**flat)


def load_universe(path: str | Path = DEFAULT_UNIVERSE_PATH) -> pd.DataFrame:
    """Candidate funds, one row per ticker: group, tradable, preferred, note."""
    universe = pd.read_csv(path, dtype={"note": str}).fillna({"note": ""})
    for column in ("tradable", "preferred"):
        universe[column] = universe[column].astype(str).str.lower().eq("true")
    if universe["ticker"].duplicated().any():
        raise ValueError(f"Duplicate tickers in {path}")
    return universe.set_index("ticker", drop=False)
