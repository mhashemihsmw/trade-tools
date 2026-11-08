"""Load daily prices from the database as wide EUR tables (rows = trading dates, columns = tickers).

Three tables are produced:
    close      unadjusted close, used to value holdings in the portfolio
    adj_close  dividend/split-adjusted close, used for signals
    adj_open   adjusted open (= open x adj_close / close), the execution price in backtests

Yahoo stores each daily bar at midnight exchange-local time expressed in UTC
(e.g. 23:00 UTC the previous day for Frankfurt, 05:00 UTC for New York). Adding
12 hours before taking the date maps every exchange to its real trading date.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from trade_tools.db.models import Asset, DailyPrice

SUPPORTED_CURRENCIES = {"EUR", "USD", "GBP", "JPY", "CHF"}
PENCE_CODES = {"GBp", "GBX", "GBx", "GBPENCE"}
MAX_FX_GAP_ROWS = 5
MAX_DAILY_JUMP = 3.0  # a fund moving more than 3x (or below 1/3) in a day is a data error


@dataclass
class Prices:
    close: pd.DataFrame
    adj_close: pd.DataFrame
    adj_open: pd.DataFrame
    skipped: dict[str, str] = field(default_factory=dict)  # ticker -> reason it has no EUR prices
    notes: list[str] = field(default_factory=list)         # data-quality fixes applied

    def until(self, date: pd.Timestamp) -> "Prices":
        """Only rows up to and including `date` (what was known on that day)."""
        return Prices(
            self.close.loc[:date], self.adj_close.loc[:date], self.adj_open.loc[:date],
            self.skipped, self.notes,
        )


def trading_dates(timestamps: pd.Series) -> pd.Series:
    """Map Yahoo's midnight-local-time UTC stamps to the exchange's trading date (tz-naive)."""
    utc = pd.to_datetime(timestamps, utc=True)
    return (utc + pd.Timedelta(hours=12)).dt.tz_localize(None).dt.normalize()


def _positive(frame: pd.DataFrame) -> pd.DataFrame:
    values = frame.apply(pd.to_numeric, errors="coerce")
    return values.where(np.isfinite(values) & (values > 0))


def level_breaks(adj_close: pd.DataFrame) -> dict[str, pd.Timestamp]:
    """Per ticker, the last day its price jumped by more than MAX_DAILY_JUMP x in one step.

    Such jumps come from unadjusted splits/consolidations or unit changes in the vendor
    history; the data before them is on a different scale. Indices (^...) are exempt.
    """
    breaks = {}
    for ticker in adj_close.columns:
        if ticker.startswith("^"):
            continue
        series = adj_close[ticker].dropna()
        step = series / series.shift(1)
        jumps = step.index[(step > MAX_DAILY_JUMP) | (step < 1 / MAX_DAILY_JUMP)]
        if len(jumps):
            breaks[ticker] = jumps[-1]
    return breaks


def _fx_to_base(fx: pd.DataFrame, base: str) -> pd.DataFrame:
    """Base-currency value of one unit of each supported currency, per date.

    `fx` holds the Yahoo FX closes: EURUSD=X and GBPUSD=X (USD per unit),
    JPY=X and CHF=X (units per USD).
    """
    def column(name: str) -> pd.Series:
        return fx[name] if name in fx else pd.Series(np.nan, index=fx.index)

    usd_per_unit = pd.DataFrame({
        "USD": 1.0,
        "EUR": column("EURUSD=X"),
        "GBP": column("GBPUSD=X"),
        "JPY": 1 / column("JPY=X"),
        "CHF": 1 / column("CHF=X"),
    }, index=fx.index)
    rates = usd_per_unit.div(usd_per_unit[base], axis=0)
    rates[base] = 1.0  # no conversion needed, even on days without FX quotes
    return rates


def prices_from_long(
    rows: pd.DataFrame,
    currencies: dict[str, str | None],
    *,
    base_currency: str = "EUR",
) -> Prices:
    """Build the wide EUR tables from long rows [ticker, timestamp, open, close, adjusted_close]."""
    base = base_currency.upper()
    if base not in SUPPORTED_CURRENCIES:
        raise ValueError(f"Unsupported base currency: {base}")
    rows = rows.assign(date=trading_dates(rows["timestamp"]))
    rows = rows.sort_values("timestamp").drop_duplicates(["ticker", "date"], keep="last")

    def wide(column: str) -> pd.DataFrame:
        return _positive(rows.pivot(index="date", columns="ticker", values=column)).sort_index()

    close, adj_close, open_ = wide("close"), wide("adjusted_close"), wide("open")
    adj_open = _positive(open_ * adj_close / close)
    notes = []
    for ticker, day in level_breaks(adj_close).items():
        for table in (close, adj_close, adj_open):
            table.loc[table.index < day, ticker] = np.nan
        notes.append(f"{ticker}: price history before {day.date()} dropped (one-day jump > {MAX_DAILY_JUMP:g}x)")
    fx = _fx_to_base(adj_close, base).ffill(limit=MAX_FX_GAP_ROWS)

    skipped: dict[str, str] = {}
    converted: dict[str, list[pd.Series]] = {"close": [], "adj_close": [], "adj_open": []}
    for ticker in close.columns:
        raw = (currencies.get(ticker) or "").strip()
        code = "GBP" if raw in PENCE_CODES else raw.upper()
        if ticker.endswith("=X"):
            skipped[ticker] = "fx_rate"
            continue
        if code not in SUPPORTED_CURRENCIES:
            skipped[ticker] = "missing_currency" if not code else "unsupported_currency"
            continue
        rate = fx[code] / (100 if raw in PENCE_CODES else 1)
        for name, table in (("close", close), ("adj_close", adj_close), ("adj_open", adj_open)):
            converted[name].append((table[ticker] * rate).rename(ticker))

    def assemble(name: str) -> pd.DataFrame:
        if not converted[name]:
            return pd.DataFrame(index=close.index)
        return pd.concat(converted[name], axis=1).dropna(how="all", axis=0)

    return Prices(assemble("close"), assemble("adj_close"), assemble("adj_open"), skipped, notes)


FX_TICKERS = ("EURUSD=X", "GBPUSD=X", "JPY=X", "CHF=X")


def load_prices(
    session: Session,
    *,
    base_currency: str = "EUR",
    tickers: list[str] | None = None,
    start: pd.Timestamp | None = None,
) -> Prices:
    """Read daily history from the database (active or not) and convert to EUR.

    By default everything is loaded. `tickers` and `start` limit the download (FX rates
    are always included); the dashboard uses this because it only needs recent prices
    of the funds you hold.
    """
    assets = session.execute(select(Asset.id, Asset.ticker, Asset.currency)).all()
    if tickers is not None:
        wanted = set(tickers) | set(FX_TICKERS)
        assets = [row for row in assets if row.ticker in wanted]
    ticker_by_id = {row.id: row.ticker for row in assets}
    statement = select(
        DailyPrice.asset_id, DailyPrice.timestamp, DailyPrice.open,
        DailyPrice.close, DailyPrice.adjusted_close,
    )
    if tickers is not None:
        statement = statement.where(DailyPrice.asset_id.in_(list(ticker_by_id)))
    if start is not None:
        statement = statement.where(DailyPrice.timestamp >= pd.Timestamp(start, tz="UTC"))
    rows = pd.DataFrame(session.execute(statement).all(), columns=[
        "asset_id", "timestamp", "open", "close", "adjusted_close",
    ])
    rows["ticker"] = rows["asset_id"].map(ticker_by_id)
    return prices_from_long(
        rows, {row.ticker: row.currency for row in assets}, base_currency=base_currency
    )


def month_end_dates(adj_close: pd.DataFrame, tickers: list[str] | pd.Index) -> pd.DatetimeIndex:
    """Last trading date of each month on which any of `tickers` traded (fund calendar)."""
    traded = adj_close.reindex(columns=tickers).dropna(how="all").index
    if traded.empty:
        return pd.DatetimeIndex([])
    return pd.DatetimeIndex(pd.Series(traded, index=traded).groupby(traded.to_period("M")).max())
