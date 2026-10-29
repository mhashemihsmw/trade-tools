"""Price-series conversion and read-only database access for the research layer."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from trade_tools.db.models import Asset, DailyPrice


def _asof_rate(
    index: pd.DatetimeIndex,
    rates: pd.Series,
    max_gap: pd.Timedelta,
) -> pd.Series:
    """Return the latest available rate at or before each timestamp."""
    left_index = pd.DatetimeIndex(pd.to_datetime(index, utc=True))
    right_rates = rates.copy()
    right_rates.index = pd.DatetimeIndex(pd.to_datetime(right_rates.index, utc=True))
    left = pd.DataFrame({"timestamp": left_index}).sort_values("timestamp")
    right = right_rates.rename("rate").dropna().to_frame().reset_index()
    right.columns = ["timestamp", "rate"]
    merged = pd.merge_asof(
        left,
        right.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=max_gap,
    )
    return pd.Series(merged["rate"].to_numpy(), index=left_index, dtype=float)


def convert_to_base_currency(
    prices: pd.DataFrame,
    currency: str | None,
    fx: dict[str, pd.Series],
    *,
    base_currency: str = "EUR",
    max_fx_gap_days: int = 5,
) -> pd.DataFrame:
    """Convert close and adjusted execution open using point-in-time FX as-of rates."""
    required = {"close", "open", "adjusted_close"}
    if not required.issubset(prices.columns):
        raise ValueError(f"prices must include {', '.join(sorted(required))}")
    if max_fx_gap_days < 0:
        raise ValueError("max_fx_gap_days cannot be negative")
    result = prices.copy().sort_index()
    result.index = pd.DatetimeIndex(pd.to_datetime(result.index, utc=True))
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result["open"] = pd.to_numeric(result["open"], errors="coerce")
    result["adjusted_close"] = pd.to_numeric(result["adjusted_close"], errors="coerce")
    result["adjusted_open"] = result["open"] * result["adjusted_close"] / result["close"]
    result.loc[
        ~np.isfinite(result["close"]) | (result["close"] <= 0)
        | ~np.isfinite(result["adjusted_close"]) | (result["adjusted_close"] <= 0),
        ["close", "adjusted_open", "adjusted_close"],
    ] = np.nan

    raw_currency = currency.strip() if currency else None
    is_pence = raw_currency in {"GBp", "GBX", "GBx", "GBPENCE"}
    currency = raw_currency.upper() if raw_currency else None
    if currency is None:
        result[["close", "adjusted_open"]] = np.nan
        result["conversion_reason"] = "missing_currency"
        return result
    if is_pence:
        currency = "GBP"
        result[["close", "adjusted_open", "adjusted_close"]] /= 100
    base_currency = base_currency.upper()
    supported_currencies = {"EUR", "USD", "GBP", "JPY", "CHF"}
    if base_currency not in supported_currencies:
        raise ValueError(f"Unsupported base currency: {base_currency}")
    if currency == base_currency:
        result["conversion_reason"] = None
        return result

    max_gap = pd.Timedelta(days=max_fx_gap_days)
    def usd_value_per_unit(code: str) -> pd.Series:
        """Convert one unit of the currency to its USD value."""
        if code == "USD":
            return pd.Series(1.0, index=result.index)
        if code == "EUR":
            series = fx.get("EURUSD=X")
            return _asof_rate(result.index, series, max_gap) if series is not None else pd.Series(np.nan, index=result.index)
        if code == "GBP":
            series = fx.get("GBPUSD=X")
            return _asof_rate(result.index, series, max_gap) if series is not None else pd.Series(np.nan, index=result.index)
        symbol = "JPY=X" if code == "JPY" else "CHF=X"
        units_per_usd = fx.get(symbol)
        return (
            1 / _asof_rate(result.index, units_per_usd, max_gap)
            if units_per_usd is not None else pd.Series(np.nan, index=result.index)
        )

    if currency not in supported_currencies:
        result[["close", "adjusted_open", "adjusted_close"]] = np.nan
        result["conversion_reason"] = "unsupported_currency"
        return result
    rate = usd_value_per_unit(currency) / usd_value_per_unit(base_currency)

    result["close"] *= rate
    result["adjusted_open"] *= rate
    result["adjusted_close"] *= rate
    result["conversion_reason"] = np.where(rate.notna(), None, "missing_currency")
    return result


def load_research_prices(
    session: Session,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    base_currency: str = "EUR",
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, Any]]]:
    """Load every asset's daily history without filtering on its current active flag."""
    assets = list(session.scalars(select(Asset).order_by(Asset.ticker)))
    asset_by_id = {asset.id: asset for asset in assets}
    statement = select(DailyPrice).order_by(DailyPrice.asset_id, DailyPrice.timestamp)
    if start is not None:
        statement = statement.where(DailyPrice.timestamp >= start)
    if end is not None:
        statement = statement.where(DailyPrice.timestamp <= end)
    rows = session.scalars(statement)
    collected: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        collected.setdefault(row.asset_id, []).append({
            "timestamp": row.timestamp,
            "open": row.open,
            "close": row.close,
            "adjusted_close": row.adjusted_close,
        })

    raw: dict[str, pd.DataFrame] = {}
    metadata: dict[str, dict[str, Any]] = {}
    for asset_id, asset in asset_by_id.items():
        frame = pd.DataFrame(collected.get(asset_id, []))
        if not frame.empty:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            frame = frame.set_index("timestamp").sort_index()
            raw[asset.ticker] = frame
        metadata[asset.ticker] = {
            "id": asset.id,
            "ticker": asset.ticker,
            "type": asset.type,
            "currency": asset.currency,
            "country": asset.country,
            "sector": asset.sector,
        }

    fx = {
        ticker: frame["adjusted_close"].dropna()
        for ticker, frame in raw.items()
        if ticker in {"EURUSD=X", "GBPUSD=X", "JPY=X", "CHF=X"}
    }
    converted: dict[str, pd.DataFrame] = {}
    for ticker, frame in raw.items():
        currency = metadata[ticker]["currency"]
        if ticker in fx or currency is None:
            converted[ticker] = frame.copy()
            if currency is None:
                metadata[ticker]["conversion_reason"] = "missing_currency"
            continue
        converted[ticker] = convert_to_base_currency(
            frame,
            currency,
            fx,
            base_currency=base_currency,
        )
        reasons = converted[ticker].get("conversion_reason")
        if reasons is not None:
            missing_reasons = reasons.dropna()
            if not missing_reasons.empty:
                metadata[ticker]["conversion_reason"] = missing_reasons.iloc[-1]
    return converted, metadata
