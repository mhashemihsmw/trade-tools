"""Monthly backtest: run the live decision at every past month-end and track the result.

Timing: the decision uses prices up to the last trading day of month t. Trades
execute at the next available adjusted open, and positions are held until the open
after the next month-end. A month is only included once that exit open exists.

Three portfolios go through the same accounting (`simulate`):
    strategy      the decision's target weights
    equal_weight  equal weights over every eligible group (one listing per group)
    benchmark     100% in the first `benchmark` ticker with data that month
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from trade_tools.decision import make_decision
from trade_tools.prices import Prices, month_end_dates
from trade_tools.selection import candidate_universe
from trade_tools.settings import Settings

MAX_OPEN_GAP_ROWS = 10  # how far ahead to look for the next open (holidays, missing bars)
PORTFOLIOS = ("strategy", "equal_weight", "benchmark")


@dataclass
class BacktestResult:
    monthly: pd.DataFrame     # per decision date and portfolio: return, turnover, costs, cash...
    weights: pd.DataFrame     # strategy target weights, decision date x ticker
    candidates: pd.DataFrame  # every candidate at every date: signals, status, forward return
    metrics: pd.DataFrame     # performance per portfolio and period
    warnings: list[str]
    settings: Settings


def execution_prices(prices: Prices) -> pd.DataFrame:
    """Row d = price at which a decision taken at the close of d is executed (next open)."""
    opens = prices.adj_open.reindex(prices.adj_close.index)
    return opens.bfill(limit=MAX_OPEN_GAP_ROWS).shift(-1)


def decision_dates(prices: Prices, tickers: pd.Index, settings: Settings) -> pd.DatetimeIndex:
    """Month-ends with a known exit price, i.e. excluding the month in progress."""
    dates = month_end_dates(prices.adj_close, tickers)
    if settings.start:
        dates = dates[dates >= pd.Timestamp(settings.start)]
    if settings.end:
        dates = dates[dates <= pd.Timestamp(settings.end)]
    last_open = prices.adj_open.reindex(columns=tickers).dropna(how="all").index.max()
    return dates[dates < last_open]


def forward_returns(
    prices: Prices, dates: pd.DatetimeIndex, tickers: pd.Index
) -> tuple[pd.DataFrame, list[str]]:
    """Return from the open after each date to the open after the next date.

    If an asset has no exit open (e.g. delisted), its last close is used instead.
    """
    execution = execution_prices(prices).reindex(columns=tickers)
    last_close = prices.adj_close.reindex(columns=tickers).ffill()
    entry = execution.loc[dates[:-1]].to_numpy()
    exit_ = execution.loc[dates[1:]]
    fallback = exit_.isna() & np.isfinite(entry)
    exit_ = exit_.fillna(last_close.loc[dates[1:]])
    returns = pd.DataFrame(exit_.to_numpy() / entry - 1, index=dates[:-1], columns=tickers)
    warnings = [
        f"{date.date()} {ticker}: no exit open, used last close"
        for date, ticker in fallback.stack().loc[lambda s: s].index
    ]
    return returns, warnings


def simulate(targets: pd.DataFrame, returns: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Rebalance to `targets` each month, apply costs, and drift with `returns`.

    Weights are fractions of current capital; whatever isn't invested earns `cash_rate`.
    Costs = cost_bps on traded value + fixed_fee per traded asset. Trades smaller than
    `min_trade_eur` are skipped. Contributions are added as cash before each rebalance.
    """
    monthly_cash_rate = (1 + settings.cash_rate) ** (1 / 12) - 1
    capital = settings.initial_capital
    held = pd.Series(dtype=float)  # weights after last month's price moves
    held_cash = 1.0
    rows = []
    for i, date in enumerate(targets.index):
        if i > 0 and settings.monthly_contribution:
            new_capital = capital + settings.monthly_contribution
            held = held * capital / new_capital
            held_cash = (held_cash * capital + settings.monthly_contribution) / new_capital
            capital = new_capital
        target = targets.loc[date]
        target = target[target > 0]
        tickers = target.index.union(held.index)
        target = target.reindex(tickers, fill_value=0.0)
        current = held.reindex(tickers, fill_value=0.0)
        small = (target - current).abs() * capital < settings.min_trade_eur
        target[small] = current[small]
        if target.sum() > 1:
            target /= target.sum()
        trade = (target - current).abs()
        traded = trade > 1e-9
        cost = trade.sum() * settings.cost_bps / 10_000 + settings.fixed_fee * traded.sum() / capital
        cash = 1.0 - target.sum()

        asset_returns = returns.loc[date].reindex(tickers).fillna(0.0)
        values = target * (1 + asset_returns)
        cash_value = cash * (1 + monthly_cash_rate) - cost
        growth = values.sum() + cash_value
        rows.append({
            "date": date,
            "return": growth - 1,
            "turnover": 0.5 * (trade.sum() + abs(cash - held_cash)),
            "costs_eur": cost * capital,
            "cash_weight": cash,
            "holdings": int((target > 0).sum()),
            "capital_eur": capital * growth,
        })
        held, held_cash = (values / growth)[lambda w: w > 1e-12], cash_value / growth
        capital *= growth
    return pd.DataFrame(rows).set_index("date")


def performance_metrics(returns: pd.Series, risk_free_rate: float = 0.0) -> dict[str, float]:
    """Standard metrics for a series of monthly returns."""
    r = returns.dropna()
    if r.empty:
        return {}
    wealth = (1 + r).cumprod()
    years = len(r) / 12
    excess = r - ((1 + risk_free_rate) ** (1 / 12) - 1)
    return {
        "months": len(r),
        "cumulative_return": wealth.iloc[-1] - 1,
        "annual_return": wealth.iloc[-1] ** (1 / years) - 1,
        "annual_volatility": r.std() * np.sqrt(12),
        "sharpe": excess.mean() / excess.std() * np.sqrt(12) if excess.std() > 0 else np.nan,
        "max_drawdown": (wealth / wealth.cummax() - 1).min(),
        "best_month": r.max(),
        "worst_month": r.min(),
        "share_positive_months": (r > 0).mean(),
    }


def run_backtest(prices: Prices, universe: pd.DataFrame, settings: Settings) -> BacktestResult:
    candidates = candidate_universe(universe, settings).index
    candidates = candidates[candidates.isin(prices.adj_close.columns)]
    dates = decision_dates(prices, candidates, settings)
    if len(dates) < 2:
        raise ValueError("Need at least two complete months of data to backtest")
    benchmarks = pd.Index([t for t in settings.benchmark if t in prices.adj_close.columns])
    returns, warnings = forward_returns(prices, dates, candidates.union(benchmarks))
    warnings = prices.notes + warnings

    targets = {name: {} for name in PORTFOLIOS}
    tables = []
    for date in dates[:-1]:
        decision = make_decision(prices, universe, settings, date)
        table = decision.table.assign(date=date, forward_return=returns.loc[date])
        tables.append(table)
        targets["strategy"][date] = decision.weights
        representatives = table.index[table["representative"]]
        targets["equal_weight"][date] = pd.Series(1 / len(representatives), index=representatives) \
            if len(representatives) else pd.Series(dtype=float)
        available = [t for t in benchmarks if np.isfinite(returns.loc[date, t])]
        targets["benchmark"][date] = pd.Series({available[0]: 1.0}) if available else pd.Series(dtype=float)

    monthly = []
    for name in PORTFOLIOS:
        weights = pd.DataFrame(targets[name]).T.reindex(dates[:-1]).fillna(0.0)
        monthly.append(simulate(weights, returns, settings).assign(portfolio=name))
    monthly = pd.concat(monthly).reset_index().set_index(["date", "portfolio"]).sort_index()
    candidates_long = pd.concat(tables).reset_index().set_index(["date", "ticker"])

    holdout_start = dates[:-1][-settings.holdout_months] if settings.holdout_months else None
    metrics = []
    for name in PORTFOLIOS:
        series = monthly.xs(name, level="portfolio")["return"]
        periods = {"full": series}
        if holdout_start is not None:
            periods["in_sample"] = series.loc[: holdout_start - pd.Timedelta(days=1)]
            periods["holdout"] = series.loc[holdout_start:]
        for period, values in periods.items():
            stats = performance_metrics(values, settings.risk_free_rate)
            if stats:
                metrics.append({"portfolio": name, "period": period, **stats})
    weights = pd.DataFrame(targets["strategy"]).T.reindex(dates[:-1]).fillna(0.0)
    weights = weights.loc[:, (weights > 0).any()]
    return BacktestResult(
        monthly=monthly,
        weights=weights,
        candidates=candidates_long,
        metrics=pd.DataFrame(metrics).set_index(["portfolio", "period"]),
        warnings=warnings,
        settings=settings,
    )


def rank_ic(candidates: pd.DataFrame) -> pd.Series:
    """Per date: rank correlation between score and next month's return (eligible groups only)."""
    eligible = candidates.loc[candidates["representative"]].dropna(subset=["score", "forward_return"])
    return eligible.groupby(level="date").apply(
        # Spearman = Pearson correlation of ranks
        lambda g: g["score"].rank().corr(g["forward_return"].rank()) if len(g) > 2 else np.nan
    ).rename("rank_ic")


def save_backtest(result: BacktestResult, folder: Path) -> Path:
    """Write CSVs, settings, an equity-curve chart and a short summary.md."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    folder.mkdir(parents=True, exist_ok=True)
    result.monthly.to_csv(folder / "monthly.csv")
    result.weights.to_csv(folder / "weights.csv")
    result.candidates.to_csv(folder / "candidates.csv")
    result.metrics.to_csv(folder / "metrics.csv")
    (folder / "settings.json").write_text(json.dumps(asdict(result.settings), indent=2))

    returns = result.monthly["return"].unstack("portfolio")
    wealth = (1 + returns).cumprod()
    fig, ax = plt.subplots(figsize=(10, 5))
    wealth.plot(ax=ax, logy=True)
    ax.set_title("Growth of 1 (log scale)")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(folder / "equity_curve.png", dpi=120)
    plt.close(fig)

    ic = rank_ic(result.candidates)
    table = result.metrics.copy()
    lines = [
        "# Backtest summary",
        "",
        f"Months: {returns.index.min().date()} to {returns.index.max().date()}",
        f"Mean rank IC (score vs next-month return): {ic.mean():.3f}",
        "",
        "| Portfolio | Period | Annual return | Volatility | Sharpe | Max drawdown |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for (portfolio, period), row in table.iterrows():
        lines.append(
            f"| {portfolio} | {period} | {row['annual_return']:.2%} | {row['annual_volatility']:.2%} "
            f"| {row['sharpe']:.2f} | {row['max_drawdown']:.2%} |"
        )
    strategy = result.monthly.xs("strategy", level="portfolio")
    lines += [
        "",
        f"Strategy average monthly turnover: {strategy['turnover'].mean():.1%}; "
        f"total costs: €{strategy['costs_eur'].sum():,.0f}; "
        f"average cash: {strategy['cash_weight'].mean():.1%}",
    ]
    if result.warnings:
        lines += ["", "## Warnings", ""] + [f"- {w}" for w in result.warnings]
    (folder / "summary.md").write_text("\n".join(lines) + "\n")
    return folder
