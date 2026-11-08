"""Your real portfolio, derived only from recorded transactions plus market prices.

Transactions are never edited: a mistake is corrected by `reverse_transaction`, which
adds an opposite row. Reversed rows and their reversals are ignored everywhere below.

Transaction types and their effect on cash (EUR, after `fx_to_eur`):
    deposit / withdrawal   +/- amount   (external flows: money you add or take out)
    dividend / fee         +/- amount
    buy / sell             -/+ units x price, minus fees
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from trade_tools.db.models import Asset, Transaction

TYPES = ("buy", "sell", "deposit", "withdrawal", "fee", "dividend")
REVERSED_TYPE = {
    "buy": "sell", "sell": "buy", "deposit": "withdrawal",
    "withdrawal": "deposit", "fee": "dividend", "dividend": "fee",
}
COLUMNS = [
    "id", "date", "type", "ticker", "quantity", "price", "fees", "currency", "fx_to_eur",
    "notes", "reference", "cash_flow_eur", "external_flow_eur", "unit_change",
]


# --- writing -------------------------------------------------------------------------

def record_transaction(
    session: Session,
    *,
    kind: str,
    trade_date: date,
    quantity: float,
    ticker: str | None = None,
    price: float | None = None,
    fees: float = 0.0,
    currency: str = "EUR",
    fx_to_eur: float = 1.0,
    notes: str | None = None,
    reference: str | None = None,
) -> Transaction:
    """Validate and add one transaction. `quantity` is units for buy/sell, else an amount."""
    kind = kind.strip().lower()
    if kind not in TYPES:
        raise ValueError(f"type must be one of {', '.join(TYPES)}")
    if not np.isfinite(quantity) or quantity <= 0:
        raise ValueError("quantity must be positive")
    if not np.isfinite(fees) or fees < 0:
        raise ValueError("fees cannot be negative")
    if not np.isfinite(fx_to_eur) or fx_to_eur <= 0:
        raise ValueError("fx_to_eur must be positive")
    if currency.strip().upper() == "EUR" and not np.isclose(fx_to_eur, 1.0):
        raise ValueError("EUR transactions must use fx_to_eur = 1")
    asset_id = None
    if kind in ("buy", "sell"):
        if not ticker:
            raise ValueError(f"{kind} needs a ticker")
        if price is None or not np.isfinite(price) or price <= 0:
            raise ValueError(f"{kind} needs a positive price")
        asset_id = session.scalar(select(Asset.id).where(Asset.ticker == ticker.strip().upper()))
        if asset_id is None:
            raise ValueError(f"Unknown ticker: {ticker}")
        if kind == "sell":
            current = load_transactions(session)
            held = current.loc[current["ticker"] == ticker.strip().upper(), "unit_change"].sum()
            if quantity > held + 1e-9:
                raise ValueError(f"Cannot sell {quantity:g} {ticker}: only {held:g} held")
    elif ticker or price is not None:
        raise ValueError(f"{kind} is a cash entry; leave ticker and price empty")
    elif fees:
        raise ValueError("Record fees on cash entries as a separate 'fee' transaction")
    row = Transaction(
        transaction_type=kind, trade_date=trade_date, quantity=float(quantity),
        asset_id=asset_id, execution_price=price, fees=float(fees),
        currency=currency.strip().upper(), fx_to_eur=float(fx_to_eur),
        notes=notes, reference=reference,
    )
    session.add(row)
    session.flush()
    return row


def reverse_transaction(session: Session, transaction_id: int, notes: str | None = None) -> Transaction:
    """Cancel a transaction by adding its mirror image (the original is kept)."""
    original = session.get(Transaction, transaction_id)
    if original is None:
        raise ValueError(f"Transaction {transaction_id} not found")
    if original.reversal_of_id is not None:
        raise ValueError("A reversal cannot itself be reversed")
    if session.scalar(select(Transaction.id).where(Transaction.reversal_of_id == transaction_id)):
        raise ValueError(f"Transaction {transaction_id} is already reversed")
    reversal = Transaction(
        asset_id=original.asset_id, trade_date=original.trade_date,
        transaction_type=REVERSED_TYPE[original.transaction_type],
        quantity=original.quantity, execution_price=original.execution_price,
        fees=original.fees, currency=original.currency, fx_to_eur=original.fx_to_eur,
        notes=notes or f"Reversal of transaction {transaction_id}", reversal_of_id=original.id,
    )
    session.add(reversal)
    session.flush()
    return reversal


def import_csv(session: Session, path: Path) -> int:
    """Import rows with columns date,type,quantity[,ticker,price,fees,currency,fx_to_eur,notes,reference]."""
    frame = pd.read_csv(path, dtype=str).fillna("")
    missing = {"date", "type", "quantity"} - set(frame.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {', '.join(sorted(missing))}")
    for line, row in enumerate(frame.to_dict("records"), start=2):
        try:
            record_transaction(
                session,
                kind=row["type"],
                trade_date=pd.Timestamp(row["date"]).date(),
                quantity=float(row["quantity"]),
                ticker=row.get("ticker") or None,
                price=float(row["price"]) if row.get("price") else None,
                fees=float(row.get("fees") or 0),
                currency=row.get("currency") or "EUR",
                fx_to_eur=float(row.get("fx_to_eur") or 1),
                notes=row.get("notes") or None,
                reference=row.get("reference") or None,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Row {line}: {exc}") from exc
    return len(frame)


# --- reading -------------------------------------------------------------------------

def load_transactions(session: Session, include_reversed: bool = False) -> pd.DataFrame:
    """All transactions as a table, with their EUR effect on cash and units.

    By default reversed transactions and the reversal rows themselves are left out.
    """
    rows = session.execute(
        select(Transaction, Asset.ticker)
        .join(Asset, Asset.id == Transaction.asset_id, isouter=True)
        .order_by(Transaction.trade_date, Transaction.id)
    ).all()
    records = [{
        "id": t.id, "date": pd.Timestamp(t.trade_date), "type": t.transaction_type,
        "ticker": ticker, "quantity": t.quantity, "price": t.execution_price, "fees": t.fees,
        "currency": t.currency, "fx_to_eur": t.fx_to_eur, "notes": t.notes,
        "reference": t.reference, "reversal_of": t.reversal_of_id,
    } for t, ticker in rows]
    frame = pd.DataFrame(records, columns=[*COLUMNS[:11], "reversal_of"])
    if not include_reversed:
        cancelled = set(frame["reversal_of"].dropna())
        frame = frame.loc[frame["reversal_of"].isna() & ~frame["id"].isin(cancelled)]
    return add_flows(frame.drop(columns="reversal_of"))


def add_flows(transactions: pd.DataFrame) -> pd.DataFrame:
    """Add cash_flow_eur, external_flow_eur and unit_change columns."""
    t = transactions.copy()
    amount = t["quantity"] * t["fx_to_eur"]
    trade_value = t["quantity"] * t["price"].fillna(0) * t["fx_to_eur"]
    fees = t["fees"] * t["fx_to_eur"]
    sign = t["type"].map({"deposit": 1, "dividend": 1, "withdrawal": -1, "fee": -1})
    t["cash_flow_eur"] = np.select(
        [t["type"].eq("buy"), t["type"].eq("sell")],
        [-trade_value - fees, trade_value - fees],
        default=sign * amount,
    )
    t["external_flow_eur"] = np.select(
        [t["type"].eq("deposit"), t["type"].eq("withdrawal")], [amount, -amount], default=0.0
    )
    t["unit_change"] = np.select(
        [t["type"].eq("buy"), t["type"].eq("sell")], [t["quantity"], -t["quantity"]], default=0.0
    )
    return t.reset_index(drop=True)


def holdings(transactions: pd.DataFrame, latest_prices: pd.Series) -> pd.DataFrame:
    """Per asset ever traded: units, average cost, value, unrealised and realised P&L.

    Average-cost method: buys (incl. fees) raise the cost basis; a sale removes the
    average cost of the units sold, and proceeds minus that cost is realised P&L.
    """
    rows = []
    trades = transactions.loc[transactions["type"].isin(["buy", "sell"])]
    for ticker, group in trades.groupby("ticker", sort=True):
        units = cost = realised = 0.0
        for trade in group.itertuples():
            if trade.type == "buy":
                units += trade.quantity
                cost += -trade.cash_flow_eur
            else:
                average = cost / units if units else 0.0
                realised += trade.cash_flow_eur - average * trade.quantity
                cost -= average * trade.quantity
                units -= trade.quantity
        price = latest_prices.get(ticker, np.nan)
        value = units * price if units > 1e-9 else 0.0
        rows.append({
            "ticker": ticker,
            "units": units if units > 1e-9 else 0.0,
            "avg_cost_eur": cost / units if units > 1e-9 else np.nan,
            "cost_basis_eur": cost if units > 1e-9 else 0.0,
            "price_eur": price,
            "value_eur": value,
            "unrealised_pnl_eur": value - cost if units > 1e-9 else 0.0,
            "unrealised_return": (value - cost) / cost if units > 1e-9 and cost > 0 else np.nan,
            "realised_pnl_eur": realised,
            "first_bought": group["date"].min(),
        })
    return pd.DataFrame(rows, columns=[
        "ticker", "units", "avg_cost_eur", "cost_basis_eur", "price_eur", "value_eur",
        "unrealised_pnl_eur", "unrealised_return", "realised_pnl_eur", "first_bought",
    ]).set_index("ticker")


def portfolio_history(transactions: pd.DataFrame, close: pd.DataFrame) -> pd.DataFrame:
    """Daily value of the portfolio from the first transaction to the latest price.

    Columns: holdings_eur, cash_eur, value_eur, net_invested_eur (deposits - withdrawals),
    gain_eur, daily_return (time-weighted: flows don't count as performance), twr_index.
    Holdings without a market price yet are valued at their last trade price.
    """
    if transactions.empty:
        return pd.DataFrame()
    start = transactions["date"].min()
    end = max(close.index.max(), transactions["date"].max()) if not close.empty else transactions["date"].max()
    days = pd.bdate_range(start, end)

    daily = transactions.groupby("date")[["cash_flow_eur", "external_flow_eur"]].sum()
    daily = daily.reindex(days.union(daily.index), fill_value=0.0)
    units = (
        transactions.loc[transactions["unit_change"] != 0]
        .pivot_table(index="date", columns="ticker", values="unit_change", aggfunc="sum")
        .reindex(daily.index).fillna(0.0).cumsum()
    )
    trade_prices = (
        transactions.dropna(subset=["price"])
        .assign(price_eur=lambda t: t["price"] * t["fx_to_eur"])
        .pivot_table(index="date", columns="ticker", values="price_eur", aggfunc="last")
    )
    prices = (
        close.reindex(columns=units.columns)
        .combine_first(trade_prices.reindex(columns=units.columns))
        .reindex(daily.index.union(close.index)).ffill().reindex(daily.index)
    )
    history = pd.DataFrame(index=daily.index)
    history["holdings_eur"] = (units * prices).sum(axis=1, min_count=0)
    history["cash_eur"] = daily["cash_flow_eur"].cumsum()
    history["value_eur"] = history["holdings_eur"] + history["cash_eur"]
    history["net_invested_eur"] = daily["external_flow_eur"].cumsum()
    history["gain_eur"] = history["value_eur"] - history["net_invested_eur"]
    # Money deposited on a day counts as available from the start of that day, so the
    # first day (and any deposit day) is measured against what was put in.
    base = history["value_eur"].shift(1).fillna(0.0) + daily["external_flow_eur"]
    history["daily_return"] = (history["value_eur"] / base - 1).where(base > 0)
    history["twr_index"] = (1 + history["daily_return"].fillna(0)).cumprod()
    return history.loc[start:]
