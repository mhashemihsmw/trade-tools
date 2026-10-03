import logging
import click
from sqlalchemy import select
from datetime import datetime, timezone

from trade_tools.db import init_db, get_session
from trade_tools.db.models import Asset, VALID_ASSET_TYPES
from trade_tools.jobs.init_db import run_init_db
from trade_tools.market_data.ingestion import MarketDataIngestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


@click.group()
def main():
    """Trade Tools CLI - Market Data, Strategy & Portfolio Management."""
    pass


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


if __name__ == "__main__":
    main()
