"""Transaction-derived actual holdings, cash flows, and non-executing trade proposals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from trade_tools.db.models import Asset, Transaction


_TRANSACTION_TYPES = {"buy", "sell", "deposit", "withdrawal", "fee", "dividend"}
_REVERSED_TYPES = {
    "buy": "sell",
    "sell": "buy",
    "deposit": "withdrawal",
    "withdrawal": "deposit",
    "fee": "dividend",
    "dividend": "fee",
}


@dataclass(frozen=True)
class TradeProposalConfig:
    proportional_cost_bps: float = 10.0
    fixed_fee: float = 2.0
    minimum_trade_size: float = 0.0
    fractional_trading: bool = False

    def __post_init__(self) -> None:
        costs = (self.proportional_cost_bps, self.fixed_fee, self.minimum_trade_size)
        if not all(np.isfinite(value) and value >= 0 for value in costs):
            raise ValueError("Trade costs and minimum size must be non-negative")


def record_transaction(
    session: Session,
    *,
    transaction_type: str,
    quantity: float,
    trade_date: date,
    asset_id: int | None = None,
    execution_price: float | None = None,
    fees: float = 0.0,
    currency: str = "EUR",
    fx_to_eur: float = 1.0,
    notes: str | None = None,
    reference: str | None = None,
) -> Transaction:
    """Validate and stage one transaction; the caller controls commit."""
    kind = transaction_type.lower()
    if kind not in _TRANSACTION_TYPES:
        raise ValueError(f"Unsupported transaction type: {transaction_type}")
    if not np.isfinite(quantity) or quantity <= 0:
        raise ValueError("quantity must be finite and positive")
    if not np.isfinite(fees) or fees < 0:
        raise ValueError("fees must be finite and non-negative")
    if not np.isfinite(fx_to_eur) or fx_to_eur <= 0:
        raise ValueError("fx_to_eur must be finite and positive")
    currency = currency.strip()
    if not currency:
        raise ValueError("currency is required")
    if currency.upper() == "EUR" and not np.isclose(fx_to_eur, 1.0):
        raise ValueError("EUR transactions must use an FX rate of 1")
    if kind in {"buy", "sell"}:
        if asset_id is None:
            raise ValueError(f"{kind} transactions require an asset")
        if execution_price is None or not np.isfinite(execution_price) or execution_price <= 0:
            raise ValueError(f"{kind} transactions require a positive execution price")
    elif asset_id is not None or execution_price is not None:
        raise ValueError(f"{kind} transactions are cash-only; omit asset and execution_price")
    elif fees != 0:
        raise ValueError("Record cash transaction fees as separate fee entries")
    transaction = Transaction(
        transaction_type=kind,
        quantity=float(quantity),
        trade_date=trade_date,
        asset_id=asset_id,
        execution_price=float(execution_price) if execution_price is not None else None,
        fees=float(fees),
        currency=currency,
        fx_to_eur=float(fx_to_eur),
        notes=notes,
        reference=reference,
    )
    session.add(transaction)
    session.flush()
    return transaction


def reverse_transaction(session: Session, transaction_id: int, *, notes: str | None = None) -> Transaction:
    """Cancel an entry with an immutable reversal row; never edit the original."""
    original = session.get(Transaction, transaction_id)
    if original is None:
        raise ValueError(f"Transaction {transaction_id} was not found")
    if original.reversal_of_id is not None:
        raise ValueError("A reversal entry cannot itself be reversed")
    existing = session.scalar(
        select(Transaction.id).where(Transaction.reversal_of_id == transaction_id)
    )
    if existing is not None:
        raise ValueError(f"Transaction {transaction_id} already has a reversal")
    reversal = Transaction(
        asset_id=original.asset_id,
        trade_date=original.trade_date,
        transaction_type=_REVERSED_TYPES[original.transaction_type],
        quantity=original.quantity,
        execution_price=original.execution_price,
        fees=original.fees,
        currency=original.currency,
        fx_to_eur=original.fx_to_eur,
        notes=notes or f"Reversal of transaction {transaction_id}",
        reversal_of_id=original.id,
    )
    session.add(reversal)
    session.flush()
    return reversal


def _effective_transactions(session: Session, as_of: date | None = None) -> list[Transaction]:
    rows = list(
        session.scalars(
            select(Transaction).order_by(Transaction.trade_date, Transaction.id)
        )
    )
    reversed_ids = {row.reversal_of_id for row in rows if row.reversal_of_id is not None}
    return [
        row for row in rows
        if row.reversal_of_id is None
        and row.id not in reversed_ids
        and (as_of is None or row.trade_date <= as_of)
    ]


def derive_portfolio(
    session: Session,
    prices: dict[str, pd.DataFrame],
    *,
    as_of: date | None = None,
    metadata: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Replay the ledger using EUR average cost and mark holdings to latest known close."""
    transactions = _effective_transactions(session, as_of)
    assets = {
        asset.id: asset.ticker
        for asset in session.scalars(select(Asset))
    }
    positions: dict[int, dict[str, float]] = {}
    realized_by_asset: dict[int, float] = {}
    cash_eur = 0.0
    realized_pnl = 0.0
    external_flows: dict[date, float] = {}
    for entry in transactions:
        kind = entry.transaction_type
        fx = entry.fx_to_eur
        if kind == "deposit":
            cash_eur += entry.quantity * fx
            external_flows[entry.trade_date] = external_flows.get(entry.trade_date, 0.0) + entry.quantity * fx
            continue
        if kind == "withdrawal":
            amount = entry.quantity * fx
            if amount > cash_eur + 1e-9:
                raise ValueError("Withdrawal exceeds available cash; record a sale or deposit first")
            cash_eur -= amount
            external_flows[entry.trade_date] = external_flows.get(entry.trade_date, 0.0) - amount
            continue
        if kind == "fee":
            amount = entry.quantity * fx
            if amount > cash_eur + 1e-9:
                raise ValueError("Fee exceeds available cash; record cash funding first")
            cash_eur -= amount
            continue
        if kind == "dividend":
            cash_eur += entry.quantity * fx
            continue

        if entry.asset_id is None or entry.execution_price is None:
            raise ValueError(f"Ledger transaction {entry.id} is missing its asset or execution price")
        slot = positions.setdefault(entry.asset_id, {"quantity": 0.0, "cost_eur": 0.0})
        gross = entry.quantity * entry.execution_price * fx
        fee_eur = entry.fees * fx
        if kind == "buy":
            if gross + fee_eur > cash_eur + 1e-9:
                raise ValueError(f"Buy of {entry.quantity:g} units exceeds available cash")
            slot["quantity"] += entry.quantity
            slot["cost_eur"] += gross + fee_eur
            cash_eur -= gross + fee_eur
        else:
            if entry.quantity > slot["quantity"] + 1e-9:
                ticker = assets.get(entry.asset_id, str(entry.asset_id))
                raise ValueError(f"Sell of {entry.quantity:g} {ticker} exceeds the held quantity")
            average_cost = slot["cost_eur"] / slot["quantity"] if slot["quantity"] else 0.0
            realized = gross - fee_eur - average_cost * entry.quantity
            realized_pnl += realized
            realized_by_asset[entry.asset_id] = realized_by_asset.get(entry.asset_id, 0.0) + realized
            slot["quantity"] -= entry.quantity
            slot["cost_eur"] -= average_cost * entry.quantity
            cash_eur += gross - fee_eur

    holding_rows: list[dict[str, Any]] = []
    total_market_value = 0.0
    total_unrealized = 0.0
    unpriced_tickers: list[str] = []
    for asset_id, slot in positions.items():
        quantity = slot["quantity"]
        if quantity <= 1e-9:
            continue
        ticker = assets[asset_id]
        frame = prices.get(ticker)
        price = np.nan
        conversion_reason = (metadata or {}).get(ticker, {}).get("conversion_reason")
        if (
            frame is not None and not frame.empty and "close" in frame
            and conversion_reason not in {"missing_currency", "unsupported_currency"}
        ):
            eligible = frame.loc[:pd.Timestamp(as_of, tz="UTC")] if as_of else frame
            values = pd.to_numeric(eligible["close"], errors="coerce").dropna()
            if not values.empty:
                price = float(values.iloc[-1])
        market_value = quantity * price if np.isfinite(price) else np.nan
        average_cost = slot["cost_eur"] / quantity
        unrealized = market_value - slot["cost_eur"] if np.isfinite(market_value) else np.nan
        if np.isfinite(market_value):
            total_market_value += market_value
            total_unrealized += unrealized
        else:
            unpriced_tickers.append(ticker)
        holding_rows.append({
            "asset_id": asset_id,
            "ticker": ticker,
            "quantity": quantity,
            "average_cost_eur": average_cost,
            "price_eur": price,
            "market_value_eur": market_value,
            "unrealized_pnl_eur": unrealized,
            "unrealized_return": (
                unrealized / slot["cost_eur"] if np.isfinite(unrealized) and slot["cost_eur"] > 0 else np.nan
            ),
            "realized_pnl_eur": realized_by_asset.get(asset_id, 0.0),
        })
    holdings = pd.DataFrame(holding_rows, columns=[
        "asset_id", "ticker", "quantity", "average_cost_eur", "price_eur",
        "market_value_eur", "unrealized_pnl_eur", "unrealized_return", "realized_pnl_eur",
    ])
    valuation_complete = not unpriced_tickers
    nav = cash_eur + total_market_value if valuation_complete else np.nan
    if not holdings.empty and nav > 0:
        holdings["actual_weight"] = holdings["market_value_eur"] / nav
    else:
        holdings["actual_weight"] = pd.Series(dtype=float)
    return {
        "holdings": holdings,
        "cash_eur": cash_eur,
        "market_value_eur": total_market_value,
        "portfolio_value_eur": nav,
        "valuation_complete": valuation_complete,
        "unpriced_tickers": unpriced_tickers,
        "realized_pnl_eur": realized_pnl,
        "unrealized_pnl_eur": total_unrealized,
        "external_flows": external_flows,
        "transactions": transactions,
    }


def propose_trades(
    target_weights: pd.DataFrame,
    holdings: pd.DataFrame,
    *,
    capital_eur: float,
    cash_eur: float,
    config: TradeProposalConfig | None = None,
    market_prices: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compare targets with actual holdings; this function never records executions."""
    settings = config or TradeProposalConfig()
    if not np.isfinite(capital_eur) or capital_eur < 0:
        raise ValueError("capital_eur must be finite and non-negative")
    actual = {
        str(row.ticker): row
        for row in holdings.itertuples(index=False)
    } if not holdings.empty else {}
    targets = {
        str(row.ticker): float(row.weight)
        for row in target_weights.itertuples(index=False)
    } if not target_weights.empty else {}
    proposals: list[dict[str, Any]] = []
    total_costs = 0.0
    cash_after = cash_eur
    tickers = sorted(
        set(actual) | set(targets),
        key=lambda ticker: (
            targets.get(ticker, 0.0) * capital_eur
            - (
                float(getattr(actual[ticker], "market_value_eur", 0.0))
                if ticker in actual and np.isfinite(getattr(actual[ticker], "market_value_eur", np.nan))
                else 0.0
            ) > 0,
            ticker,
        ),
    )
    for ticker in tickers:
        current = actual.get(ticker)
        price = (
            float(getattr(current, "price_eur", np.nan))
            if current else float((market_prices or {}).get(ticker, np.nan))
        )
        current_value = (
            float(getattr(current, "market_value_eur", 0.0))
            if current and np.isfinite(getattr(current, "market_value_eur", np.nan))
            else 0.0
        )
        target_value = targets.get(ticker, 0.0) * capital_eur
        if not np.isfinite(price) or price <= 0:
            if target_value > current_value:
                proposals.append({
                    "ticker": ticker, "side": "blocked", "quantity": np.nan,
                    "price_eur": price, "notional_eur": np.nan, "estimated_cost_eur": np.nan,
                    "reason": "missing_current_price",
                })
            continue
        difference = target_value - current_value
        raw_quantity = difference / price
        if not settings.fractional_trading:
            quantity = math.floor(abs(raw_quantity) + 1e-12) * (1 if raw_quantity > 0 else -1)
        else:
            quantity = raw_quantity
        side = "buy" if quantity > 0 else "sell"
        notional = abs(quantity) * price
        if notional < settings.minimum_trade_size or abs(quantity) < 1e-12:
            continue
        fee = notional * settings.proportional_cost_bps / 10_000 + settings.fixed_fee
        if side == "sell":
            quantity = -min(abs(quantity), float(getattr(current, "quantity", 0.0)) if current else 0.0)
            notional = abs(quantity) * price
            fee = notional * settings.proportional_cost_bps / 10_000 + settings.fixed_fee
            cash_after += notional - fee
        else:
            if notional + fee > cash_after + 1e-8:
                proposals.append({
                    "ticker": ticker, "side": "blocked", "quantity": 0.0,
                    "price_eur": price, "notional_eur": 0.0, "estimated_cost_eur": 0.0,
                    "reason": "insufficient_cash_for_proposed_buy",
                })
                continue
            cash_after -= notional + fee
        total_costs += fee
        proposals.append({
            "ticker": ticker,
            "side": side,
            "quantity": abs(quantity),
            "price_eur": price,
            "notional_eur": notional,
            "estimated_cost_eur": fee,
            "reason": "rebalance_to_recommended_target",
        })
    return {
        "trades": pd.DataFrame(proposals, columns=[
            "ticker", "side", "quantity", "price_eur", "notional_eur",
            "estimated_cost_eur", "reason",
        ]),
        "estimated_costs_eur": total_costs,
        "cash_after_trades_eur": cash_after,
    }


def time_weighted_returns(
    nav: pd.Series,
    external_flows: pd.Series | dict[date, float] | None = None,
) -> pd.Series:
    """Daily TWR with external cash flows assumed to occur at each day's close."""
    values = pd.to_numeric(nav, errors="coerce").sort_index()
    values.index = pd.to_datetime(values.index, utc=True).normalize()
    flows = pd.Series(
        {} if external_flows is None else external_flows,
        dtype=float,
    )
    flows.index = pd.to_datetime(flows.index, utc=True).normalize()
    flows = flows.groupby(level=0).sum()
    previous_nav = values.shift(1)
    returns = (values - flows.reindex(values.index, fill_value=0.0)) / previous_nav - 1
    valid = values.notna() & (previous_nav > 0) & np.isfinite(returns)
    return returns.loc[valid]


def portfolio_nav_history(
    session: Session,
    prices: dict[str, pd.DataFrame],
    *,
    metadata: dict[str, dict[str, Any]] | None = None,
) -> tuple[pd.Series, dict[date, float]]:
    """Value ledger holdings each market day, treating deposits/withdrawals as external flows."""
    transactions = _effective_transactions(session)
    assets = {asset.id: asset.ticker for asset in session.scalars(select(Asset))}
    market_dates = {
        timestamp.date()
        for frame in prices.values()
        for timestamp in frame.index
    }
    transaction_dates = {entry.trade_date for entry in transactions}
    dates = sorted(market_dates | transaction_dates)
    if not dates:
        return pd.Series(dtype=float), {}

    price_by_date: dict[str, dict[date, float]] = {}
    for ticker, frame in prices.items():
        conversion_reason = (metadata or {}).get(ticker, {}).get("conversion_reason")
        if (
            "close" not in frame or frame.empty
            or conversion_reason in {"missing_currency", "unsupported_currency"}
        ):
            continue
        series = pd.to_numeric(frame["close"], errors="coerce").dropna()
        price_by_date[ticker] = {
            timestamp.date(): float(value)
            for timestamp, value in series.items()
        }
    tickers = set(price_by_date)
    last_prices: dict[str, float] = {}
    entries_by_date: dict[date, list[Transaction]] = {}
    for entry in transactions:
        entries_by_date.setdefault(entry.trade_date, []).append(entry)

    quantities: dict[int, float] = {}
    cash = 0.0
    flows: dict[date, float] = {}
    nav_values: dict[pd.Timestamp, float] = {}
    for market_date in dates:
        for ticker in tickers:
            observed = price_by_date[ticker].get(market_date)
            if observed is not None:
                last_prices[ticker] = observed
        for entry in entries_by_date.get(market_date, []):
            amount = entry.quantity * entry.fx_to_eur
            if entry.transaction_type == "deposit":
                cash += amount
                flows[market_date] = flows.get(market_date, 0.0) + amount
            elif entry.transaction_type == "withdrawal":
                cash -= amount
                flows[market_date] = flows.get(market_date, 0.0) - amount
            elif entry.transaction_type == "fee":
                cash -= amount
            elif entry.transaction_type == "dividend":
                cash += amount
            elif entry.transaction_type == "buy":
                quantity = entry.quantity
                cash -= quantity * float(entry.execution_price) * entry.fx_to_eur
                cash -= entry.fees * entry.fx_to_eur
                quantities[entry.asset_id] = quantities.get(entry.asset_id, 0.0) + quantity
            elif entry.transaction_type == "sell":
                quantity = entry.quantity
                cash += quantity * float(entry.execution_price) * entry.fx_to_eur
                cash -= entry.fees * entry.fx_to_eur
                quantities[entry.asset_id] = quantities.get(entry.asset_id, 0.0) - quantity
        held_tickers = [
            assets.get(asset_id, "")
            for asset_id, quantity in quantities.items()
            if quantity > 1e-9
        ]
        if any(ticker not in last_prices for ticker in held_tickers):
            nav_values[pd.Timestamp(market_date, tz="UTC")] = np.nan
            continue
        market_value = sum(
            quantity * last_prices.get(assets.get(asset_id, ""), 0.0)
            for asset_id, quantity in quantities.items()
        )
        nav_values[pd.Timestamp(market_date, tz="UTC")] = cash + market_value
    return pd.Series(nav_values, dtype=float), flows
