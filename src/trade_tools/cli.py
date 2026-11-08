"""Command-line interface. Each command is a thin wrapper around one module.

Data:       init-db, ingest, backfill, refresh-adjusted-history, asset ...
Strategy:   decide, backtest
Portfolio:  tx add/list/reverse/import, portfolio, dashboard
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from trade_tools.db import get_session, init_db
from trade_tools.db.models import Asset, VALID_ASSET_TYPES
from trade_tools.jobs.init_db import run_init_db
from trade_tools.market_data.ingestion import MarketDataIngestion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

settings_option = click.option(
    "--settings", "settings_path", default="config/strategy.toml", show_default=True,
    type=click.Path(exists=True, dir_okay=False), help="Strategy settings file.",
)


@click.group()
def main():
    """Trade Tools: market data, monthly strategy decisions and portfolio tracking."""


# --- data (unchanged) ----------------------------------------------------------------

@main.command("init-db")
def init_db_cmd():
    """Initialize database tables and seed initial asset universe."""
    run_init_db()


@main.command("ingest")
@click.option("--daily/--no-daily", default=True, help="Include daily price ingestion.")
@click.option("--hourly/--no-hourly", default=True, help="Include hourly price ingestion.")
def ingest_cmd(daily: bool, hourly: bool):
    """Run market data ingestion for all active assets."""
    init_db()
    ingestion = MarketDataIngestion()
    summary = ingestion.ingest_all_active(include_daily=daily, include_hourly=hourly)
    click.echo(
        f"Ingestion completed: {summary.successful_assets}/{summary.total_active_assets} active assets succeeded. "
        f"Daily records: {summary.total_daily_records}, Hourly records: {summary.total_hourly_records}."
    )


@main.command("backfill")
@click.option("--ticker", "tickers", multiple=True, required=True, help="Ticker to backfill; repeat for multiple.")
@click.option("--daily/--no-daily", default=True, help="Include daily price ingestion.")
@click.option("--hourly/--no-hourly", default=True, help="Include hourly price ingestion.")
def backfill_cmd(tickers: tuple[str, ...], daily: bool, hourly: bool):
    """Backfill prices for selected active assets without processing other assets."""
    init_db()
    ingestion = MarketDataIngestion()
    try:
        summary = ingestion.ingest_tickers(
            list(tickers),
            include_daily=daily,
            include_hourly=hourly,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(
        f"Backfill completed: {summary.successful_assets}/{summary.total_active_assets} assets succeeded. "
        f"Daily records: {summary.total_daily_records}, Hourly records: {summary.total_hourly_records}."
    )
    if summary.failed_assets:
        raise click.ClickException(f"{summary.failed_assets} asset(s) failed; see logs for details.")


@main.group("asset")
def asset_grp():
    """Manage assets in the database."""
    pass


@asset_grp.command("list")
@click.option("--active-only/--all", default=True, help="Filter by active status.")
def list_assets_cmd(active_only: bool):
    """List assets stored in the database."""
    init_db()
    with get_session() as session:
        stmt = select(Asset)
        if active_only:
            stmt = stmt.where(Asset.active == True)
        assets = session.scalars(stmt).all()
        if not assets:
            click.echo("No assets found.")
            return

        click.echo(f"{'ID':<5} {'Ticker':<12} {'Type':<20} {'Active':<8} {'Name'}")
        click.echo("-" * 70)
        for a in assets:
            click.echo(f"{a.id:<5} {a.ticker:<12} {a.type:<20} {str(a.active):<8} {a.name or 'N/A'}")


@asset_grp.command("add")
@click.option("--ticker", required=True, help="Asset ticker symbol (e.g. AAPL, BTC-USD).")
@click.option(
    "--type",
    "asset_type",
    required=True,
    type=click.Choice(sorted(list(VALID_ASSET_TYPES))),
    help="Asset type category.",
)
def add_asset_cmd(ticker: str, asset_type: str):
    """Add a new asset to the database."""
    init_db()
    ticker = ticker.upper()
    with get_session() as session:
        existing = session.scalar(select(Asset).where(Asset.ticker == ticker))
        if existing:
            click.echo(f"Asset with ticker {ticker} already exists (active={existing.active}).")
            return

        asset = Asset(ticker=ticker, type=asset_type, active=True, data_source="yahoo_finance")
        session.add(asset)
        session.commit()

        # Update metadata immediately
        ingestion = MarketDataIngestion()
        ingestion.update_asset_metadata(session, asset)
        session.commit()

        click.echo(f"Added new asset {ticker} ({asset_type}).")


@asset_grp.command("deactivate")
@click.option("--ticker", required=True, help="Asset ticker symbol to deactivate.")
def deactivate_asset_cmd(ticker: str):
    """Deactivate an asset (suspends regular ingestion)."""
    init_db()
    ticker = ticker.upper()
    with get_session() as session:
        asset = session.scalar(select(Asset).where(Asset.ticker == ticker))
        if not asset:
            click.echo(f"Asset with ticker {ticker} not found.")
            return

        asset.deactivate()
        session.commit()
        click.echo(f"Asset {ticker} deactivated.")


@asset_grp.command("reactivate")
@click.option("--ticker", required=True, help="Asset ticker symbol to reactivate.")
def reactivate_asset_cmd(ticker: str):
    """Reactivate a previously deactivated asset."""
    init_db()
    ticker = ticker.upper()
    with get_session() as session:
        asset = session.scalar(select(Asset).where(Asset.ticker == ticker))
        if not asset:
            click.echo(f"Asset with ticker {ticker} not found.")
            return

        asset.reactivate()
        session.commit()
        click.echo(f"Asset {ticker} reactivated.")


# --- strategy ------------------------------------------------------------------------

def _load(settings_path: str):
    from trade_tools.prices import load_prices
    from trade_tools.settings import load_settings, load_universe

    settings = load_settings(settings_path)
    universe = load_universe()
    init_db()
    with get_session() as session:
        prices = load_prices(session)
    return settings, universe, prices


@main.command("decide")
@click.option("--date", "as_of", help="Decide as of this date (YYYY-MM-DD). Default: end of last month.")
@click.option("--contribution", type=click.FloatRange(min=0), default=0.0, show_default=True,
              help="New money (EUR) to invest this month, included in suggested trades.")
@click.option("--output-dir", default="reports/decisions", show_default=True,
              type=click.Path(file_okay=False, path_type=Path))
@settings_option
def decide(as_of: str | None, contribution: float, output_dir: Path, settings_path: str):
    """Compute this month's target weights and suggested trades (nothing is executed)."""
    from trade_tools.decision import make_decision, save_decision, suggest_trades
    from trade_tools.ledger import holdings, load_transactions, portfolio_history

    settings, universe, prices = _load(settings_path)
    today = pd.Timestamp.now().normalize()
    as_of_date = pd.Timestamp(as_of) if as_of else today.replace(day=1) - pd.Timedelta(days=1)
    decision = make_decision(prices, universe, settings, as_of_date)
    if (as_of_date - decision.date).days > 7:
        raise click.ClickException(
            f"Latest fund prices are from {decision.date.date()}; run `trade-tools ingest` first."
        )

    with get_session() as session:
        transactions = load_transactions(session)
    latest = prices.close.ffill().iloc[-1]
    current = holdings(transactions, latest)["units"]
    history = portfolio_history(transactions, prices.close)
    cash = float(history["cash_eur"].iloc[-1]) if not history.empty else 0.0
    trades = suggest_trades(decision.weights, current, cash, latest, settings, contribution)

    folder = save_decision(decision, trades, output_dir / str(decision.date.date()))
    click.echo((folder / "summary.md").read_text())
    click.echo(f"Saved to {folder}/")


@main.command("backtest")
@click.option("--start", help="First decision date (YYYY-MM-DD); overrides the settings file.")
@click.option("--end", help="Last decision date (YYYY-MM-DD); overrides the settings file.")
@click.option("--output-dir", default="reports/backtests", show_default=True,
              type=click.Path(file_okay=False, path_type=Path))
@settings_option
def backtest(start: str | None, end: str | None, output_dir: Path, settings_path: str):
    """Run the monthly decision over history and compare with equal-weight and benchmark."""
    from dataclasses import replace

    from trade_tools.backtest import run_backtest, save_backtest

    settings, universe, prices = _load(settings_path)
    settings = replace(settings, start=start or settings.start, end=end or settings.end)
    try:
        result = run_backtest(prices, universe, settings)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    folder = save_backtest(result, output_dir / stamp)
    click.echo((folder / "summary.md").read_text())
    click.echo(f"Saved to {folder}/")


# --- portfolio -----------------------------------------------------------------------

@main.group("tx")
def tx_group():
    """Record your real trades and cash movements."""


@tx_group.command("add")
@click.option("--type", "kind", required=True,
              type=click.Choice(["buy", "sell", "deposit", "withdrawal", "fee", "dividend"]))
@click.option("--date", "trade_date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--quantity", required=True, type=float, help="Units for buy/sell, amount otherwise.")
@click.option("--ticker", help="Required for buy/sell.")
@click.option("--price", type=float, help="Price per unit, required for buy/sell.")
@click.option("--fees", type=float, default=0.0, show_default=True)
@click.option("--currency", default="EUR", show_default=True)
@click.option("--fx-to-eur", type=float, default=1.0, show_default=True,
              help="EUR per unit of --currency on the trade date.")
@click.option("--notes")
@click.option("--reference", help="Optional unique ID, e.g. the broker's order number.")
def tx_add(kind, trade_date, quantity, ticker, price, fees, currency, fx_to_eur, notes, reference):
    """Add one transaction."""
    from trade_tools.ledger import record_transaction

    init_db()
    try:
        with get_session() as session:
            row = record_transaction(
                session, kind=kind, trade_date=trade_date.date(), quantity=quantity,
                ticker=ticker, price=price, fees=fees, currency=currency,
                fx_to_eur=fx_to_eur, notes=notes, reference=reference,
            )
            transaction_id = row.id
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Recorded transaction {transaction_id}.")


@tx_group.command("list")
@click.option("--all", "include_reversed", is_flag=True, help="Include reversed rows and reversals.")
@click.option("--limit", type=int, default=50, show_default=True)
def tx_list(include_reversed: bool, limit: int):
    """Show the most recent transactions."""
    from trade_tools.ledger import load_transactions

    init_db()
    with get_session() as session:
        table = load_transactions(session, include_reversed=include_reversed)
    columns = ["id", "date", "type", "ticker", "quantity", "price", "fees", "cash_flow_eur", "notes"]
    click.echo(table[columns].tail(limit).to_string(index=False, na_rep=""))


@tx_group.command("reverse")
@click.argument("transaction_id", type=int)
@click.option("--notes")
def tx_reverse(transaction_id: int, notes: str | None):
    """Cancel a transaction by adding an opposite one (the original is kept)."""
    from trade_tools.ledger import reverse_transaction

    init_db()
    try:
        with get_session() as session:
            reversal_id = reverse_transaction(session, transaction_id, notes).id
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Added reversal {reversal_id} for transaction {transaction_id}.")


@tx_group.command("import")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def tx_import(path: Path):
    """Import a CSV: date,type,quantity[,ticker,price,fees,currency,fx_to_eur,notes,reference]."""
    from trade_tools.ledger import import_csv

    init_db()
    try:
        with get_session() as session:
            count = import_csv(session, path)
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Imported {count} transactions.")


@main.command("portfolio")
@settings_option
def portfolio(settings_path: str):
    """Print current holdings and performance."""
    from trade_tools.dashboard import portfolio_summary
    from trade_tools.ledger import load_transactions

    settings, _, prices = _load(settings_path)
    with get_session() as session:
        transactions = load_transactions(session)
    summary = portfolio_summary(transactions, prices, settings.benchmark)
    for key, value in summary.stats.items():
        click.echo(f"{key:>18}: {value:,.4f}")
    columns = ["units", "avg_cost_eur", "price_eur", "value_eur", "weight", "unrealised_pnl_eur", "unrealised_return"]
    click.echo(summary.holdings[columns].to_string(float_format=lambda v: f"{v:,.2f}"))
    for warning in summary.warnings:
        click.echo(f"Warning: {warning}")


@main.command("dashboard")
@click.option("--output", default="reports/dashboard/index.html", show_default=True,
              type=click.Path(dir_okay=False, path_type=Path))
@settings_option
def dashboard(output: Path, settings_path: str):
    """Write the portfolio dashboard as a static HTML file."""
    from trade_tools.dashboard import build_dashboard

    build_dashboard(output, settings_path=settings_path)
    click.echo(f"Dashboard written to {output}")


# --- data maintenance ----------------------------------------------------------------

@main.command("refresh-adjusted-history")
def refresh_adjusted_history():
    """Refresh full daily history for all active assets (corrects restated adjusted prices)."""
    init_db()
    summary = MarketDataIngestion().refresh_adjusted_history()
    click.echo(
        f"Adjusted history refresh completed: {summary.successful_assets}/{summary.total_active_assets} assets succeeded; "
        f"{summary.total_daily_records} daily records upserted."
    )
    if summary.failed_assets:
        raise click.ClickException(f"{summary.failed_assets} asset(s) failed; see logs for details.")


if __name__ == "__main__":
    main()
