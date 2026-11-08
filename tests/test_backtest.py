"""Backtest timing, accounting and outputs on synthetic prices."""

import numpy as np
import pandas as pd
import pytest

from trade_tools.backtest import (
    decision_dates, forward_returns, performance_metrics, run_backtest, save_backtest, simulate,
)
from trade_tools.prices import Prices
from trade_tools.settings import Settings


def synthetic_prices(end="2024-03-08", crypto_until=None) -> Prices:
    days = pd.bdate_range("2020-01-01", end)
    rng = np.random.default_rng(3)
    data = {
        "FUND_A": 100 * np.exp(np.cumsum(0.0006 + rng.normal(0, 0.006, len(days)))),
        "FUND_B": 100 * np.exp(np.cumsum(0.0002 + rng.normal(0, 0.003, len(days)))),
        "BENCH": 100 * np.exp(np.cumsum(0.0003 + rng.normal(0, 0.008, len(days)))),
    }
    close = pd.DataFrame(data, index=days)
    if crypto_until:  # a weekend-trading asset with prices after the last fund day
        crypto_days = pd.date_range("2020-01-01", crypto_until)
        close = close.reindex(days.union(crypto_days))
        close["COIN"] = np.linspace(100, 200, len(close))
    return Prices(close=close, adj_close=close, adj_open=close * 0.999)


UNIVERSE = pd.DataFrame({
    "ticker": ["FUND_A", "FUND_B"], "group": ["a", "b"],
    "tradable": True, "preferred": False, "note": "",
}).set_index("ticker", drop=False)
SETTINGS = Settings(top_n=2, max_weight=1.0, volatility_target=0, benchmark=("BENCH",), holdout_months=6)


def test_month_in_progress_is_excluded_even_if_other_assets_trade_later():
    prices = synthetic_prices(end="2024-03-08", crypto_until="2024-03-10")
    dates = decision_dates(prices, UNIVERSE.index, SETTINGS)
    assert dates[-1] == pd.Timestamp("2024-02-29")  # March has not ended


def test_forward_returns_run_from_next_open_to_next_open():
    prices = synthetic_prices()
    dates = pd.DatetimeIndex(["2024-01-31", "2024-02-29"])
    returns, warnings = forward_returns(prices, dates, pd.Index(["FUND_A"]))
    entry = prices.adj_open.loc["2024-02-01", "FUND_A"]
    exit_ = prices.adj_open.loc["2024-03-01", "FUND_A"]
    assert returns.loc["2024-01-31", "FUND_A"] == pytest.approx(exit_ / entry - 1)
    assert warnings == []


def test_missing_exit_open_falls_back_to_last_close():
    prices = synthetic_prices()
    prices.adj_open.loc["2024-02-01":, "FUND_A"] = np.nan  # e.g. delisted
    prices.adj_open.loc["2024-01-02":"2024-01-31", "FUND_A"] = 1.0
    dates = pd.DatetimeIndex(["2023-12-29", "2024-01-31"])
    returns, warnings = forward_returns(prices, dates, pd.Index(["FUND_A"]))
    assert np.isfinite(returns.iloc[0, 0]) and len(warnings) == 1


def test_simulate_applies_costs_and_drift():
    dates = pd.DatetimeIndex(["2024-01-31", "2024-02-29"])
    targets = pd.DataFrame({"A": [0.5, 0.5]}, index=dates)
    returns = pd.DataFrame({"A": [0.10, 0.0]}, index=dates)
    result = simulate(targets, returns, Settings(cost_bps=10, fixed_fee=0, initial_capital=1000))
    first = result.iloc[0]
    assert first["return"] == pytest.approx(0.5 * 0.10 - 0.5 * 0.001)
    assert first["turnover"] == pytest.approx(0.5)
    # After A rose 10%, its weight drifted above 50%; the second month trims it back.
    drifted = 0.55 / (1 + first["return"])
    assert result.iloc[1]["costs_eur"] == pytest.approx((drifted - 0.5) * 0.001 * first["capital_eur"])


def test_performance_metrics():
    stats = performance_metrics(pd.Series([0.1, -0.1, 0.05]))
    assert stats["cumulative_return"] == pytest.approx(1.1 * 0.9 * 1.05 - 1)
    assert stats["max_drawdown"] == pytest.approx(-0.1)


def test_run_backtest_end_to_end(tmp_path):
    result = run_backtest(synthetic_prices(), UNIVERSE, SETTINGS)
    portfolios = set(result.monthly.index.get_level_values("portfolio"))
    assert portfolios == {"strategy", "equal_weight", "benchmark"}
    strategy = result.monthly.xs("strategy", level="portfolio")
    assert strategy.index.max() == pd.Timestamp("2024-01-31")
    assert (result.weights.sum(axis=1) <= 1 + 1e-9).all()
    assert {"full", "in_sample", "holdout"} <= set(result.metrics.index.get_level_values("period"))
    assert result.candidates["status"].notna().all()

    folder = save_backtest(result, tmp_path / "run")
    assert {"monthly.csv", "weights.csv", "metrics.csv", "summary.md", "equity_curve.png"} <= {
        p.name for p in folder.iterdir()
    }
