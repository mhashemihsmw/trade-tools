"""The monthly decision: signals -> selection -> weights, plus suggested trades.

`make_decision` is the single entry point used both by the live monthly run and by
every month of the backtest, so the two can never disagree.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trade_tools.prices import Prices
from trade_tools.selection import candidate_universe, select_assets
from trade_tools.settings import Settings
from trade_tools.signals import compute_signals
from trade_tools.weights import compute_weights


@dataclass
class Decision:
    date: pd.Timestamp     # last trading day on or before the requested date
    table: pd.DataFrame    # every candidate: signals, status, rank, weight
    weights: pd.Series     # selected tickers -> weight (sum <= 1)
    volatility: float      # estimated annualised portfolio volatility

    @property
    def cash_weight(self) -> float:
        return max(0.0, 1.0 - float(self.weights.sum()))


def decision_date(prices: Prices, tickers: pd.Index, as_of: pd.Timestamp) -> pd.Timestamp:
    """Last date on or before `as_of` on which any candidate fund traded."""
    traded = prices.adj_close.reindex(columns=tickers).loc[:as_of].dropna(how="all").index
    if traded.empty:
        raise ValueError(f"No candidate prices on or before {as_of.date()}")
    return traded[-1]


def make_decision(
    prices: Prices,
    universe: pd.DataFrame,
    settings: Settings,
    as_of: pd.Timestamp,
) -> Decision:
    candidates = candidate_universe(universe, settings).index
    date = decision_date(prices, candidates, pd.Timestamp(as_of))
    known = prices.adj_close.loc[:date]
    signals = compute_signals(known, date, candidates, settings)
    table = select_assets(signals, universe, date, settings)
    weights, volatility = compute_weights(table, known, settings)
    table["weight"] = weights.reindex(table.index).fillna(0.0)
    return Decision(date, table, weights, volatility)


def suggest_trades(
    weights: pd.Series,
    holdings: pd.Series,
    cash_eur: float,
    latest_prices: pd.Series,
    settings: Settings,
    contribution_eur: float = 0.0,
) -> pd.DataFrame:
    """Trades that move current holdings (units per ticker) to the target weights.

    Capital = holdings at `latest_prices` + cash + new contribution. Units are rounded
    toward zero unless fractional shares are allowed. Nothing is recorded; you record
    the fills you actually get with `trade-tools tx add`.
    """
    tickers = sorted(set(weights.index) | set(holdings[holdings > 0].index))
    price = latest_prices.reindex(tickers)
    missing = price[price.isna()].index.tolist()
    if missing:
        raise ValueError(f"No current price for: {', '.join(missing)}")
    units = holdings.reindex(tickers).fillna(0.0)
    capital = float((units * price).sum()) + cash_eur + contribution_eur
    target_value = weights.reindex(tickers).fillna(0.0) * capital
    raw_units = (target_value - units * price) / price
    round_ = (lambda x: x) if settings.fractional_shares else (lambda x: math.trunc(x))
    trade_units = raw_units.map(round_)

    trades = pd.DataFrame({
        "side": trade_units.map(lambda u: "buy" if u > 0 else "sell"),
        "units": trade_units.abs(),
        "price_eur": price,
        "value_eur": trade_units.abs() * price,
        "current_units": units,
        "target_weight": weights.reindex(tickers).fillna(0.0),
        "target_value_eur": target_value,
    })
    trades = trades.loc[(trades["units"] > 0) & (trades["value_eur"] >= settings.min_trade_eur)]
    trades["est_cost_eur"] = trades["value_eur"] * settings.cost_bps / 10_000 + settings.fixed_fee
    trades = trades.sort_values(["side", "value_eur"], ascending=[False, False])  # sells first
    trades.index.name = "ticker"
    signed = trades["value_eur"].where(trades["side"] == "sell", -trades["value_eur"])
    trades.attrs["capital_eur"] = capital
    trades.attrs["cash_after_eur"] = cash_eur + contribution_eur + float(
        (signed - trades["est_cost_eur"]).sum()
    )
    return trades


def save_decision(decision: Decision, trades: pd.DataFrame | None, folder: Path) -> Path:
    """Write candidates.csv, trades.csv and a short summary.md; returns the folder."""
    folder.mkdir(parents=True, exist_ok=True)
    decision.table.to_csv(folder / "candidates.csv")
    lines = [
        f"# Monthly decision {decision.date.date()}",
        "",
        "## Target portfolio",
        "",
        "| Asset | Group | Weight | Momentum | Volatility | Score |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for ticker, row in decision.table.loc[decision.table["selected"]].iterrows():
        lines.append(
            f"| {ticker} | {row['group']} | {row['weight']:.1%} | {row['momentum']:.2%} "
            f"| {row['volatility']:.1%} | {row['score']:.2f} |"
        )
    lines += [
        f"| Cash | | {decision.cash_weight:.1%} | | | |",
        "",
        f"Estimated portfolio volatility: {decision.volatility:.1%}",
        "",
        "## Why other candidates were not selected",
        "",
    ]
    counts = decision.table["status"].value_counts()
    lines += [f"- {status}: {count}" for status, count in counts.items() if status != "selected"]
    if trades is not None:
        trades.to_csv(folder / "trades.csv")
        lines += ["", "## Suggested trades (not executed)", ""]
        if trades.empty:
            lines.append("No trades needed.")
        else:
            lines += ["| Side | Asset | Units | Price € | Value € |", "|---|---|---:|---:|---:|"]
            for ticker, row in trades.iterrows():
                lines.append(
                    f"| {row['side']} | {ticker} | {row['units']:g} | {row['price_eur']:.2f} "
                    f"| {row['value_eur']:,.2f} |"
                )
        lines += [
            "",
            f"Capital: €{trades.attrs.get('capital_eur', float('nan')):,.2f}; "
            f"estimated cash after trades: €{trades.attrs.get('cash_after_eur', float('nan')):,.2f}",
        ]
    (folder / "summary.md").write_text("\n".join(lines) + "\n")
    return folder
