import logging
import json
import hashlib
import html
import click
import numpy as np
import pandas as pd
from sqlalchemy import select
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from sqlalchemy.exc import IntegrityError

from trade_tools.db import init_db, get_session
from trade_tools.db.models import (
    Asset, DecisionItem, DecisionTrade, RiskAssessment, StrategyDecision, Transaction,
    VALID_ASSET_TYPES,
)
from trade_tools.jobs.init_db import run_init_db
from trade_tools.market_data.ingestion import MarketDataIngestion
from trade_tools.market_data.research_prices import load_research_prices
from trade_tools.market_data.research_universe import classification_for
from trade_tools.portfolio.construction import construct_target_weights, target_values
from trade_tools.portfolio.operations import (
    TradeProposalConfig,
    derive_portfolio,
    portfolio_nav_history,
    propose_trades,
    record_decision_transactions,
    record_transaction,
    reverse_decision_transactions,
    reverse_transaction,
    time_weighted_returns,
)
from trade_tools.portfolio.risk import historical_var_cvar, validate_target_weights, RiskLimits
from trade_tools.research_config import load_research_config
from trade_tools.strategies.adaptive_momentum import AdaptiveMomentumStrategy, StrategyConfig
from trade_tools.strategies.backtest import (
    BacktestConfig,
    monthly_decision_dates,
    run_backtest,
    write_backtest_report,
)
from trade_tools.features.research import aligned_daily_returns, feature_frame, sample_covariance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# Register the command-line interface.
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


def _read_research_state(config_path: str, decision_date: str | None = None):
    backtest_config, strategy_config, portfolio_config = load_research_config(config_path)
    if strategy_config.universe_mode != backtest_config.universe_mode:
        strategy_config = replace(strategy_config, universe_mode=backtest_config.universe_mode)
    with get_session() as session:
        prices, metadata = load_research_prices(
            session,
            base_currency=backtest_config.base_currency,
        )
    dates = monthly_decision_dates(prices)
    if not dates:
        raise click.ClickException("No daily market data is available for research.")
    if decision_date:
        requested = pd.Timestamp(decision_date)
        if requested.tzinfo is None:
            requested = requested.tz_localize("UTC")
        eligible_dates = [value for value in dates if value <= requested]
        if not eligible_dates:
            raise click.ClickException("No market data exists on or before the requested decision date.")
        decision = eligible_dates[-1]
    else:
        decision = dates[-1]
    observations: dict[str, pd.Series] = {}
    features: list[pd.Series] = []
    latest: dict[str, pd.Timestamp] = {}
    counts: dict[str, int] = {}
    for ticker, frame in prices.items():
        series = pd.to_numeric(frame.get("adjusted_close"), errors="coerce").dropna()
        observations[ticker] = series
        point = feature_frame(
            series,
            volatility_window=strategy_config.volatility_window,
            trend_months=strategy_config.trend_months,
            trend_days=strategy_config.trend_days,
        ).loc[lambda values: values.index <= decision]
        point_series = series.loc[series.index <= decision]
        if not point.empty:
            row = point.iloc[-1].copy()
            row["ticker"] = ticker
            features.append(row)
            if not point_series.empty:
                latest[ticker] = point_series.index[-1]
            counts[ticker] = int(len(point_series))
    features_at_date = (
        pd.DataFrame(features).set_index("ticker", drop=False) if features else pd.DataFrame()
    )
    strategy = AdaptiveMomentumStrategy(strategy_config)
    ranked = strategy.rank(
        features_at_date,
        metadata,
        decision,
        latest_observations=latest,
        history_counts=counts,
    )
    if not ranked.empty:
        ranked["universe_mode"] = strategy_config.universe_mode
    return (
        decision,
        ranked,
        observations,
        metadata,
        backtest_config,
        strategy_config,
        portfolio_config,
        prices,
    )


def _has_stale_tradable_fund(ranked: pd.DataFrame) -> bool:
    if ranked.empty:
        return False
    tradable_fund_tickers = []
    for ticker in ranked["ticker"]:
        classification = classification_for(ticker)
        if classification is not None and classification.is_fund and classification.tradable:
            tradable_fund_tickers.append(ticker)
    return ranked.loc[
        ranked["ticker"].isin(tradable_fund_tickers), "exclusion_reason"
    ].eq("stale_data").any()


def _risk_limits(config: Any) -> RiskLimits:
    return RiskLimits(
        max_asset_weight=config.max_asset_weight,
        max_exposure_group_weight=config.max_exposure_group_weight,
        volatility_target=config.volatility_target,
        volatility_window=config.covariance_window,
        var_confidence=config.var_confidence,
        var_horizons=config.var_horizons,
        var_lookback=config.var_lookback,
        max_var_1d=config.max_var_1d,
        max_cvar_21d=config.max_cvar_21d,
        max_stress_loss=config.max_stress_loss,
    )


def _actual_risk_positions(
    portfolio: dict[str, Any],
    metadata: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    holdings = portfolio["holdings"].copy()
    if holdings.empty:
        return pd.DataFrame(columns=["ticker", "weight", "exposure_group", "type", "country", "sector"])
    holdings["weight"] = holdings["market_value_eur"] / portfolio["portfolio_value_eur"]
    for field in ("type", "country", "sector"):
        holdings[field] = holdings["ticker"].map(lambda ticker: metadata.get(ticker, {}).get(field))
    holdings["exposure_group"] = holdings["ticker"].map(
        lambda ticker: getattr(classification_for(ticker), "exposure_group", ticker)
    )
    return holdings[["ticker", "weight", "exposure_group", "type", "country", "sector"]]


@main.group("strategy")
def strategy_group():
    """Run point-in-time strategy research and signals."""


@strategy_group.command("signals")
@click.option("--date", "decision_date", help="Decision date (YYYY-MM-DD); defaults to latest month end.")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--output", type=click.Path(dir_okay=False), help="Optional CSV path for rankings and exclusions.")
@click.option("--fail-on-stale/--allow-stale", default=False, help="Exit non-zero when any asset's data is stale.")
def strategy_signals(decision_date: str | None, config_path: str, output: str | None, fail_on_stale: bool):
    """Print scores, selections, exclusions and point-in-time data status."""
    try:
        decision, ranked, *_ = _read_research_state(config_path, decision_date)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    stale = _has_stale_tradable_fund(ranked)
    click.echo(f"Decision date: {decision.date()} | universe mode: {ranked['universe_mode'].iloc[0] if not ranked.empty else 'unknown'}")
    click.echo(ranked.to_string(index=False, na_rep=""))
    if output:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        ranked.to_csv(target, index=False)
        click.echo(f"CSV report: {target}")
    if fail_on_stale and stale:
        raise click.ClickException("Stale market data was found; refresh data before publishing signals.")


@strategy_group.command("monthly-report")
@click.option("--date", "decision_date", help="Decision date (YYYY-MM-DD); defaults to last completed month end.")
@click.option("--capital", type=click.FloatRange(min=0, min_open=True), help="Total portfolio capital in the configured base currency.")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--reports-dir", default="reports/monthly_signals", show_default=True, type=click.Path(file_okay=False))
@click.option("--force", is_flag=True, help="Recreate a report already present for this date/configuration.")
def monthly_signals_report(
    decision_date: str | None,
    capital: float | None,
    config_path: str,
    reports_dir: str,
    force: bool,
):
    """Create an idempotent month-end signals, exclusions, targets and risk report."""
    try:
        backtest, strategy_config, portfolio_config = load_research_config(config_path)
        if decision_date is None:
            previous_month_end = pd.Timestamp.now(tz="UTC").to_period("M").start_time.tz_localize("UTC") - pd.Timedelta(days=1)
            decision_date = previous_month_end.date().isoformat()
        decision, ranked, observations, _, _, strategy_config, portfolio_config, _ = _read_research_state(
            config_path, decision_date
        )
        report_config = json.dumps(
            {
                "strategy": strategy_config.__dict__,
                "portfolio": portfolio_config.__dict__,
                "backtest": backtest.__dict__,
                "capital": capital if capital is not None else backtest.backtest_capital,
            },
            sort_keys=True,
        )
        report_hash = hashlib.sha256(report_config.encode()).hexdigest()[:16]
        report_id = f"{decision.date()}-{report_hash}"
        report_dir = Path(reports_dir) / report_id
        if report_dir.exists() and not force:
            click.echo(f"Report already exists for {decision.date()} and configuration {strategy_config.config_hash}; skipped.")
            click.echo(f"Existing report: {report_dir}")
            return
        if _has_stale_tradable_fund(ranked):
            raise click.ClickException("Stale tradable-universe data detected; refresh data before publishing signals.")
        final_capital = capital if capital is not None else backtest.backtest_capital
        selected = ranked.loc[ranked["selected"]]
        historical_prices = {
            ticker: series.loc[series.index <= decision]
            for ticker, series in observations.items()
        }
        returns = aligned_daily_returns(pd.DataFrame(historical_prices))
        covariance = sample_covariance(
            pd.DataFrame(historical_prices).tail(portfolio_config.covariance_window + 1),
            window=portfolio_config.covariance_window,
        )
        targets = construct_target_weights(selected, portfolio_config, covariance)
        targets = target_values(targets, final_capital)
        risk = validate_target_weights(
            targets,
            _risk_limits(portfolio_config),
            asset_returns=returns,
            asset_prices=pd.DataFrame(historical_prices),
            stress_scenarios=portfolio_config.stress_scenarios,
        )
        report_dir.mkdir(parents=True, exist_ok=True)
        ranked.to_csv(report_dir / "rankings_and_exclusions.csv", index=False)
        targets.to_csv(report_dir / "target_weights.csv", index=False)
        (report_dir / "risk_validation.json").write_text(
            json.dumps(_json_safe(risk), indent=2, allow_nan=False) + "\n"
        )
        status = {
            "decision_date": decision.date().isoformat(),
            "universe_mode": strategy_config.universe_mode,
            "configuration_hash": report_hash,
            "data_freshness": "fresh",
            "capital": final_capital,
            "base_currency": backtest.base_currency,
        }
        (report_dir / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        click.echo(f"Monthly signals report: {report_dir}")
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@main.group("portfolio")
def portfolio_group():
    """Construct and validate portfolio targets."""


@portfolio_group.command("target")
@click.option("--date", "decision_date", help="Decision date (YYYY-MM-DD); defaults to latest month end.")
@click.option("--capital", type=click.FloatRange(min=0, min_open=True), required=True, help="Total portfolio capital in the configured base currency.")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--output", type=click.Path(dir_okay=False), help="Optional CSV path for target weights.")
def portfolio_target(decision_date: str | None, capital: float, config_path: str, output: str | None):
    """Print target weights, base-currency values and independent risk validation."""
    try:
        decision, ranked, observations, metadata, _, strategy_config, portfolio_config, _ = _read_research_state(
            config_path, decision_date
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    selected = ranked.loc[ranked["selected"]]
    historical_prices = {
        ticker: series.loc[series.index <= decision]
        for ticker, series in observations.items()
    }
    returns = aligned_daily_returns(pd.DataFrame(historical_prices))
    covariance = sample_covariance(
        pd.DataFrame({
            ticker: series
            for ticker, series in historical_prices.items()
        }).tail(portfolio_config.covariance_window + 1),
        window=portfolio_config.covariance_window,
    )
    targets = construct_target_weights(selected, portfolio_config, covariance)
    if not targets.empty:
        targets = target_values(targets, capital)
    assessment = validate_target_weights(
        targets,
        _risk_limits(portfolio_config),
        asset_returns=returns,
        asset_prices=pd.DataFrame(historical_prices),
        stress_scenarios=portfolio_config.stress_scenarios,
    )
    click.echo(f"Decision date: {decision.date()} | universe mode: {strategy_config.universe_mode}")
    click.echo(f"Base currency: {load_research_config(config_path)[0].base_currency}")
    click.echo(targets.to_string(index=False, na_rep=""))
    click.echo(json.dumps(_json_safe(assessment), indent=2, default=str, allow_nan=False))
    if output:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        targets.to_csv(target, index=False)
        click.echo(f"CSV report: {target}")
    if assessment["status"] != "valid":
        raise click.ClickException("Target weights violate configured risk limits.")


@main.group("transaction")
def transaction_group():
    """Record and inspect the immutable actual-portfolio ledger."""


@transaction_group.command("record")
@click.option("--type", "transaction_type", required=True, type=click.Choice(
    ["buy", "sell", "deposit", "withdrawal", "fee", "dividend"]
))
@click.option("--date", "trade_date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--quantity", required=True, type=click.FloatRange(min=0, min_open=True))
@click.option("--ticker", help="Required for buy/sell; omit for cash transactions.")
@click.option("--price", type=click.FloatRange(min=0, min_open=True), help="Required for buy/sell.")
@click.option("--fees", type=click.FloatRange(min=0), default=0.0, show_default=True)
@click.option("--currency", default="EUR", show_default=True)
@click.option("--fx-to-eur", type=click.FloatRange(min=0, min_open=True), default=1.0, show_default=True)
@click.option("--notes")
@click.option("--reference")
def transaction_record(
    transaction_type: str, trade_date: datetime, quantity: float,
    ticker: str | None, price: float | None, fees: float,
    currency: str, fx_to_eur: float, notes: str | None, reference: str | None,
):
    """Record one actual trade or cash movement; no recommendation is executed."""
    init_db()
    try:
        with get_session() as session:
            asset = session.scalar(select(Asset).where(Asset.ticker == ticker.upper())) if ticker else None
            if ticker and asset is None:
                raise ValueError(f"Unknown ticker: {ticker}")
            entry = record_transaction(
                session, transaction_type=transaction_type, quantity=quantity,
                trade_date=trade_date.date(), asset_id=asset.id if asset else None,
                execution_price=price, fees=fees, currency=currency,
                fx_to_eur=fx_to_eur, notes=notes, reference=reference,
            )
            transaction_id = entry.id
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Recorded transaction {transaction_id}; no orders were sent.")


@transaction_group.command("list")
@click.option("--limit", type=click.IntRange(min=1, max=1000), default=100, show_default=True)
def transaction_list(limit: int):
    """List the most recent ledger entries, including reversal rows."""
    init_db()
    with get_session() as session:
        entries = list(session.scalars(
            select(Transaction).order_by(Transaction.trade_date.desc(), Transaction.id.desc()).limit(limit)
        ))
        assets = {asset.id: asset.ticker for asset in session.scalars(select(Asset))}
    click.echo(f"{'ID':>5} {'Date':<10} {'Type':<12} {'Ticker':<12} {'Quantity':>12} {'Price':>12} {'Fees':>10} {'FX/EUR':>10} {'Reverses':>9}")
    for entry in entries:
        price = f"{entry.execution_price:.4f}" if entry.execution_price is not None else "-"
        click.echo(
            f"{entry.id:>5} {entry.trade_date.isoformat():<10} {entry.transaction_type:<12} "
            f"{assets.get(entry.asset_id, '-'):<12} {entry.quantity:>12.4f} {price:>12} "
            f"{entry.fees:>10.2f} {entry.fx_to_eur:>10.5f} "
            f"{entry.reversal_of_id if entry.reversal_of_id is not None else '-':>9}"
        )


@transaction_group.command("reverse")
@click.argument("transaction_id", type=int)
@click.option("--notes")
def transaction_reverse(transaction_id: int, notes: str | None):
    """Correct an entry by appending a reversal; ledger rows are never edited."""
    init_db()
    try:
        with get_session() as session:
            reversal = reverse_transaction(session, transaction_id, notes=notes)
            reversal_id = reversal.id
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Created reversal transaction {reversal_id} for {transaction_id}.")


@transaction_group.command("import-csv")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def transaction_import_csv(path: Path):
    """Import a CSV with date,type,quantity and optional ticker/price/fees/FX fields."""
    init_db()
    required = {"date", "type", "quantity"}
    try:
        frame = pd.read_csv(path)
        rows = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
        if len(frame.columns) == 0 or not required.issubset(frame.columns):
            raise ValueError(f"CSV must include columns: {', '.join(sorted(required))}")
        with get_session() as session:
            assets = {asset.ticker: asset for asset in session.scalars(select(Asset))}
            created: list[int] = []
            for row_number, row in enumerate(rows, start=2):
                ticker = str(row.get("ticker") or "").strip().upper()
                asset = assets.get(ticker) if ticker else None
                if ticker and asset is None:
                    raise ValueError(f"Row {row_number}: unknown ticker {ticker}")
                try:
                    entry = record_transaction(
                        session,
                        transaction_type=str(row["type"]).strip(),
                        quantity=float(row["quantity"]),
                        trade_date=pd.Timestamp(row["date"]).date(),
                        asset_id=asset.id if asset else None,
                        execution_price=float(row["price"]) if row.get("price") not in (None, "") else None,
                        fees=float(row.get("fees") or 0),
                        currency=str(row.get("currency") or "EUR"),
                        fx_to_eur=float(row.get("fx_to_eur") or 1.0),
                        notes=str(row["notes"]) if row.get("notes") not in (None, "") else None,
                        reference=str(row["reference"]) if row.get("reference") not in (None, "") else None,
                    )
                    created.append(entry.id)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Row {row_number}: {exc}") from exc
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Imported {len(created)} transactions.")


@portfolio_group.command("actual")
@click.option("--as-of", "as_of_date", help="Valuation date (YYYY-MM-DD), defaults to latest stored prices.")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
def portfolio_actual(as_of_date: str | None, config_path: str):
    """Show transaction-derived actual holdings, cash, P&L, and risk."""
    init_db()
    _, _, portfolio_config = load_research_config(config_path)
    date_value = pd.Timestamp(as_of_date).date() if as_of_date else None
    with get_session() as session:
        prices, metadata = load_research_prices(session, base_currency="EUR")
        actual = derive_portfolio(session, prices, as_of=date_value, metadata=metadata)
        returns = aligned_daily_returns(pd.DataFrame({
            ticker: frame["close"] for ticker, frame in prices.items() if "close" in frame
        }))
    weights = _actual_risk_positions(actual, metadata)
    if not weights.empty and actual["portfolio_value_eur"] > 0:
        weights["cash_weight"] = actual["cash_eur"] / actual["portfolio_value_eur"]
    assessment = validate_target_weights(
        weights, _risk_limits(portfolio_config), asset_returns=returns,
        stress_scenarios=portfolio_config.stress_scenarios,
    )
    click.echo(
        f"ACTUAL portfolio value EUR {actual['portfolio_value_eur']:.2f} | "
        f"cash EUR {actual['cash_eur']:.2f} | realized P&L EUR {actual['realized_pnl_eur']:.2f} | "
        f"unrealized P&L EUR {actual['unrealized_pnl_eur']:.2f}"
    )
    click.echo(actual["holdings"].to_string(index=False, na_rep=""))
    click.echo(json.dumps(_json_safe(assessment), indent=2, allow_nan=False))


@main.group("operations")
def operations_group():
    """Persist monthly decisions and produce monitoring reports."""


@operations_group.command("monthly-decision")
@click.option("--date", "decision_date", help="Decision date (YYYY-MM-DD), defaults to last completed month.")
@click.option("--new-contribution", type=click.FloatRange(min=0), default=0.0, show_default=True)
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--reports-dir", default="reports/monthly_decisions", show_default=True, type=click.Path(file_okay=False))
@click.option("--regenerate", is_flag=True, help="Supersede the prior decision and create a new generation.")
@click.option("--refresh-history/--no-refresh-history", default=True, help="Refresh adjusted prices before computing.")
@click.option(
    "--auto-record/--no-auto-record", default=True,
    help="Record the contribution and proposed trades in the ledger as assumed fills.",
)
def monthly_decision(
    decision_date: str | None,
    new_contribution: float,
    config_path: str,
    reports_dir: str,
    regenerate: bool,
    refresh_history: bool,
    auto_record: bool,
):
    """Compute, risk-check and persist a monthly decision; fills are modelled, not broker orders."""
    init_db()
    backtest, strategy_config, portfolio_config = load_research_config(config_path)
    if strategy_config.universe_mode != backtest.universe_mode:
        strategy_config = replace(strategy_config, universe_mode=backtest.universe_mode)
    if decision_date is None:
        month_end = pd.Timestamp.now(tz="UTC").to_period("M").start_time.tz_localize("UTC") - pd.Timedelta(days=1)
        decision_date = month_end.date().isoformat()
    strategy = AdaptiveMomentumStrategy(strategy_config)
    parameters = {
        "backtest": backtest.__dict__,
        "strategy": strategy_config.__dict__,
        "portfolio": portfolio_config.__dict__,
        "new_contribution_eur": new_contribution,
    }
    parameter_json = json.dumps(parameters, sort_keys=True, default=list)
    parameter_hash = hashlib.sha256(parameter_json.encode()).hexdigest()
    try:
        requested_day = pd.Timestamp(decision_date).date()
    except (TypeError, ValueError) as exc:
        raise click.ClickException("Decision date must be YYYY-MM-DD.") from exc
    month_start = requested_day.replace(day=1)
    if not regenerate:
        with get_session() as session:
            existing = session.scalar(select(StrategyDecision).where(
                StrategyDecision.strategy_name == strategy.name,
                StrategyDecision.strategy_version == strategy.version,
                StrategyDecision.parameter_hash == parameter_hash,
                StrategyDecision.decision_date >= month_start,
                StrategyDecision.decision_date <= requested_day,
                StrategyDecision.universe_mode == strategy_config.universe_mode,
                StrategyDecision.status == "proposed",
            ).order_by(StrategyDecision.generation.desc()).limit(1))
            if existing is not None:
                click.echo(f"Existing decision {existing.id} returned unchanged; report: {existing.report_path}")
                return
    if refresh_history:
        summary = MarketDataIngestion().refresh_adjusted_history()
        if summary.failed_assets:
            raise click.ClickException(
                f"Adjusted-history refresh failed for {summary.failed_assets} asset(s); no decision was created."
            )
    try:
        state = _read_research_state(config_path, decision_date)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    decision, ranked, observations, metadata, backtest, strategy_config, portfolio_config, prices = state
    if strategy_config.universe_mode != "tradable":
        raise click.ClickException("Monthly operating decisions require universe_mode='tradable'.")
    if _has_stale_tradable_fund(ranked):
        raise click.ClickException("Required tradable-universe data is stale; refresh before creating a decision.")

    decision_day = decision.date()
    historical_prices = {
        ticker: series.loc[series.index <= decision]
        for ticker, series in observations.items()
    }
    price_frame = pd.DataFrame(historical_prices)
    returns = aligned_daily_returns(price_frame)
    covariance = sample_covariance(
        price_frame.tail(portfolio_config.covariance_window + 1),
        window=portfolio_config.covariance_window,
    )
    reports_root = Path(reports_dir)
    reports_root.mkdir(parents=True, exist_ok=True)

    try:
        with get_session() as session:
            existing = list(session.scalars(select(StrategyDecision).where(
                StrategyDecision.strategy_name == strategy.name,
                StrategyDecision.strategy_version == strategy.version,
                StrategyDecision.parameter_hash == parameter_hash,
                StrategyDecision.decision_date == decision_day,
                StrategyDecision.universe_mode == strategy_config.universe_mode,
            ).order_by(StrategyDecision.generation)))
            current = next((item for item in reversed(existing) if item.status == "proposed"), None)
            if current is not None and not regenerate:
                click.echo(f"Existing decision {current.id} returned unchanged; report: {current.report_path}")
                return

            actual = derive_portfolio(session, prices, as_of=decision_day, metadata=metadata)
            capital = actual["portfolio_value_eur"] + new_contribution
            if not np.isfinite(capital) or capital <= 0:
                raise ValueError(
                    "Actual holdings must be fully valued and portfolio value plus contribution must be positive."
                )
            targets = construct_target_weights(ranked.loc[ranked["selected"]], portfolio_config, covariance)
            targets = target_values(targets, capital)
            proposed_risk = validate_target_weights(
                targets, _risk_limits(portfolio_config),
                asset_returns=returns, asset_prices=price_frame,
                stress_scenarios=portfolio_config.stress_scenarios,
            )
            actual_risk = validate_target_weights(
                _actual_risk_positions(actual, metadata),
                _risk_limits(portfolio_config),
                asset_returns=returns,
                stress_scenarios=portfolio_config.stress_scenarios,
            )
            trade_plan = propose_trades(
                targets, actual["holdings"], capital_eur=capital,
                cash_eur=actual["cash_eur"] + new_contribution,
                config=TradeProposalConfig(
                    proportional_cost_bps=backtest.proportional_cost_bps,
                    fixed_fee=backtest.fixed_fee,
                    minimum_trade_size=backtest.minimum_trade_size,
                    fractional_trading=portfolio_config.fractional_trading,
                ),
                market_prices={
                    ticker: float(frame.loc[frame.index <= decision, "close"].dropna().iloc[-1])
                    for ticker, frame in prices.items()
                    if "close" in frame and not frame.loc[frame.index <= decision, "close"].dropna().empty
                },
            ) if proposed_risk["status"] == "valid" else {
                "trades": pd.DataFrame(),
                "estimated_costs_eur": 0.0,
                "cash_after_trades_eur": actual["cash_eur"] + new_contribution,
            }
            generation = max((row.generation for row in existing), default=0) + 1
            for old in existing:
                if old.status == "proposed":
                    old.status = "superseded"
            report_dir = reports_root / f"{decision_day.isoformat()}-{parameter_hash[:16]}-g{generation}"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / "report.html"
            decision_row = StrategyDecision(
                strategy_name=strategy.name,
                strategy_version=strategy.version,
                parameter_hash=parameter_hash,
                decision_date=decision_day,
                universe_mode=strategy_config.universe_mode,
                generation=generation,
                data_cutoff_at=decision.to_pydatetime(),
                parameters=_json_safe(parameters),
                status="proposed",
                report_path=str(report_path),
            )
            session.add(decision_row)
            session.flush()
            target_map = dict(zip(targets["ticker"], targets["weight"])) if not targets.empty else {}
            target_values_by_ticker = (
                targets.set_index("ticker")["target_value"].to_dict()
                if not targets.empty and "target_value" in targets
                else {}
            )
            def optional_float(value: Any) -> float | None:
                return float(value) if pd.notna(value) and np.isfinite(value) else None

            for row in ranked.to_dict("records"):
                feature_values = _json_safe(row)
                if row["ticker"] in target_values_by_ticker:
                    feature_values["target_value_eur"] = float(
                        target_values_by_ticker[row["ticker"]]
                    )
                session.add(DecisionItem(
                    decision_id=decision_row.id,
                    asset_id=row.get("id"),
                    ticker=row["ticker"],
                    score=float(row["score"]) if pd.notna(row.get("score")) else None,
                    rank=int(row["rank"]) if pd.notna(row.get("rank")) else None,
                    selected=bool(row["selected"]),
                    target_weight=float(target_map.get(row["ticker"], 0.0)),
                    feature_values=feature_values,
                    exclusion_reason=row.get("exclusion_reason"),
                ))
            for trade in trade_plan["trades"].to_dict("records"):
                session.add(DecisionTrade(
                    decision_id=decision_row.id,
                    ticker=trade["ticker"],
                    side=trade["side"],
                    quantity=optional_float(trade["quantity"]),
                    price_eur=optional_float(trade["price_eur"]),
                    notional_eur=optional_float(trade["notional_eur"]),
                    estimated_cost_eur=optional_float(trade["estimated_cost_eur"]),
                    reason=trade["reason"],
                ))
            combined_risk = {
                "recommended": proposed_risk,
                "actual": actual_risk,
            }
            session.add(RiskAssessment(
                decision_id=decision_row.id,
                status=proposed_risk["status"],
                metrics=_json_safe(combined_risk),
                limits=_json_safe(proposed_risk["limits"]),
                violations=proposed_risk["violations"],
                warnings=proposed_risk["warnings"],
            ))
            recorded_count = 0
            if auto_record and proposed_risk["status"] == "valid":
                for old in existing:
                    if old.status == "superseded":
                        reverse_decision_transactions(session, old.id)
                recorded = record_decision_transactions(
                    session,
                    decision_id=decision_row.id,
                    trade_date=decision_day,
                    trades=trade_plan["trades"],
                    asset_ids={ticker: meta["id"] for ticker, meta in metadata.items()},
                    contribution_eur=new_contribution,
                )
                session.flush()
                # Replaying the ledger rejects oversells or cash shortfalls and rolls everything back.
                derive_portfolio(session, prices, as_of=decision_day, metadata=metadata)
                recorded_count = len(recorded)
            rankings_html = ranked.to_html(index=False, escape=True, na_rep="")
            holdings_html = actual["holdings"].to_html(index=False, escape=True, na_rep="")
            targets_html = targets.to_html(index=False, escape=True, na_rep="")
            trades_html = trade_plan["trades"].to_html(index=False, escape=True, na_rep="")
            report_path.write_text(f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Monthly decision {decision_day}</title>
<style>body{{font:15px system-ui;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#18202a}}
table{{border-collapse:collapse;font-size:13px}}th,td{{border:1px solid #ccd2d8;padding:.35rem}}
th{{background:#eef2f6}}.actual{{color:#125d3a}}.recommended{{color:#124d83}}</style></head>
<body><h1>Monthly operating proposal — {decision_day}</h1>
<p>Decision #{decision_row.id}, generation {generation}; data cutoff {decision.isoformat()}.
No orders are sent; {recorded_count} assumed-fill ledger entries were recorded. Next scheduled decision: {(decision + pd.offsets.MonthEnd(1)).date()}.</p>
<p>Actual portfolio value: EUR {actual['portfolio_value_eur']:.2f};
actual cash: EUR {actual['cash_eur']:.2f}; new contribution considered: EUR {new_contribution:.2f};
recommended capital: EUR {capital:.2f}.</p>
<h2 class="actual">Actual holdings</h2>{holdings_html}
<h2 class="recommended">Recommended targets</h2>{targets_html}
<h2 class="recommended">Proposed trades — assumed fills at proposal price{' (recorded in ledger)' if recorded_count else ' (not recorded)'}</h2>{trades_html}
<p>Estimated costs: EUR {trade_plan['estimated_costs_eur']:.2f};
projected cash after proposals: EUR {trade_plan['cash_after_trades_eur']:.2f}.
Recommended risk status: {html.escape(proposed_risk['status'])}; actual risk status: {html.escape(actual_risk['status'])}.</p>
<h2>Risk validation</h2><pre>{html.escape(json.dumps(_json_safe(combined_risk), indent=2))}</pre>
<h2>Persisted feature values, rankings, and exclusions</h2>{rankings_html}</body></html>""")
            targets.to_csv(report_dir / "recommended_targets.csv", index=False)
            trade_plan["trades"].to_csv(report_dir / "proposed_trades.csv", index=False)
            decision_id = decision_row.id
            risk_status = proposed_risk["status"]
            ledger_entries = recorded_count
    except (ValueError, IntegrityError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Persisted decision {decision_id}; report: {report_path}")
    click.echo(
        f"Risk status: {risk_status}; {ledger_entries} assumed-fill ledger entries recorded "
        "(modelled fills, not broker orders)."
    )
    if risk_status != "valid":
        raise click.ClickException("Recommended proposal rejected by configured risk validation.")


def generate_monitoring_dashboard(config_path: str, output: Path) -> Path:
    """Render the current monitoring dashboard and return its output path."""
    init_db()
    _, _, portfolio_config = load_research_config(config_path)
    with get_session() as session:
        prices, metadata = load_research_prices(session, base_currency="EUR")
        actual = derive_portfolio(session, prices, metadata=metadata)
        nav, flows = portfolio_nav_history(session, prices, metadata=metadata)
        twr = time_weighted_returns(nav, flows)
        returns = aligned_daily_returns(pd.DataFrame({
            ticker: frame["close"] for ticker, frame in prices.items() if "close" in frame
        }))
        actual_risk = validate_target_weights(
            _actual_risk_positions(actual, metadata),
            _risk_limits(portfolio_config),
            asset_returns=returns,
            stress_scenarios=portfolio_config.stress_scenarios,
        )
        decisions = list(session.scalars(select(StrategyDecision).order_by(
            StrategyDecision.decision_date.desc(), StrategyDecision.id.desc()
        ).limit(36)))
        latest = decisions[0] if decisions else None
        latest_items = list(session.scalars(select(DecisionItem).where(
            DecisionItem.decision_id == latest.id
        ).order_by(DecisionItem.rank))) if latest else []
        latest_risk = session.scalar(select(RiskAssessment).where(
            RiskAssessment.decision_id == latest.id
        )) if latest else None
        latest_trades = list(session.scalars(select(DecisionTrade).where(
            DecisionTrade.decision_id == latest.id
        ).order_by(DecisionTrade.ticker))) if latest else []
        risk_history = list(session.scalars(select(RiskAssessment).order_by(
            RiskAssessment.created_at.desc()
        ).limit(36)))

    ranking_frame = pd.DataFrame([{
        "ticker": item.ticker,
        "score": item.score,
        "rank": item.rank,
        "selected": item.selected,
        "recommended_target_weight": item.target_weight,
        "exclusion": item.exclusion_reason,
    } for item in latest_items])
    target_frame = pd.DataFrame([{
        "ticker": item.ticker,
        "weight": item.target_weight,
        "target_value": item.feature_values.get("target_value_eur"),
    } for item in latest_items if item.selected and item.target_weight > 0])
    trade_frame = pd.DataFrame([{
        "ticker": trade.ticker,
        "side": trade.side,
        "quantity": trade.quantity,
        "price_eur": trade.price_eur,
        "notional_eur": trade.notional_eur,
        "estimated_cost_eur": trade.estimated_cost_eur,
        "reason": trade.reason,
    } for trade in latest_trades])
    if latest and latest.report_path and (target_frame.empty or trade_frame.empty):
        report_directory = Path(latest.report_path).parent
        try:
            if target_frame.empty:
                target_frame = pd.read_csv(report_directory / "recommended_targets.csv")
            if trade_frame.empty:
                trade_frame = pd.read_csv(report_directory / "proposed_trades.csv")
        except FileNotFoundError:
            logger.warning("Persisted report artifacts are missing for decision %s", latest.id)

    backtest_runs = []
    backtest_monthly_tables = []
    ic_tables = []
    for summary_path in sorted(Path("reports/backtests").glob("*/summary.json")):
        try:
            summary = json.loads(summary_path.read_text())
            run_id = summary.get("run_id", summary_path.parent.name)
            for portfolio, metrics in summary.get("metrics", {}).items():
                if isinstance(metrics, dict) and "cumulative_return" in metrics:
                    backtest_runs.append({"run_id": run_id, "portfolio": portfolio, **metrics})
            monthly_path = summary_path.parent / "monthly_returns.csv"
            if monthly_path.exists():
                monthly = pd.read_csv(monthly_path)
                monthly.insert(0, "run_id", run_id)
                backtest_monthly_tables.append(monthly.tail(36))
            ic_path = summary_path.parent / "ic.csv"
            if ic_path.exists():
                ic = pd.read_csv(ic_path)
                ic.insert(0, "run_id", run_id)
                ic_tables.append(ic.tail(36))
        except (OSError, json.JSONDecodeError, ValueError):
            logger.warning("Skipping unreadable backtest report %s", summary_path)
    nav_frame = pd.DataFrame({"date": nav.index.astype(str), "actual_portfolio_value_eur": nav.values})
    twr_frame = pd.DataFrame({"date": twr.index.astype(str), "actual_daily_twr": twr.values})
    returns_frame = pd.concat(backtest_monthly_tables, ignore_index=True) if backtest_monthly_tables else pd.DataFrame()
    ic_frame = pd.concat(ic_tables, ignore_index=True) if ic_tables else pd.DataFrame()
    risk_frame = pd.DataFrame([{
        "created_at": assessment.created_at.isoformat(),
        "status": assessment.status,
        "assessment": json.dumps(_json_safe(assessment.metrics), sort_keys=True),
    } for assessment in risk_history])

    def render(frame: pd.DataFrame, empty_message: str) -> str:
        return frame.to_html(index=False, escape=True, na_rep="") if not frame.empty else f"<p>{html.escape(empty_message)}</p>"

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Trade Tools Monitoring</title>
<style>body{{font:15px system-ui;max-width:1280px;margin:2rem auto;padding:0 1rem;color:#17212b}}
table{{border-collapse:collapse;font-size:13px;margin:1rem 0 2rem;display:block;overflow:auto}}
th,td{{border:1px solid #ccd2d8;padding:.4rem;white-space:nowrap}}th{{background:#eef2f6}}
section{{margin:2rem 0;padding-top:1rem;border-top:2px solid #d5dbe2}}.actual{{color:#125d3a}}
.recommended{{color:#124d83}}small{{color:#52606d}}</style></head><body>
<h1>Trade Tools — monitoring snapshot</h1>
<p>Generated {datetime.now(timezone.utc).isoformat()}. All figures are labeled actual or recommended;
trades from monthly decisions are modelled fills, not broker orders.</p>
<section><h2>A. Strategy</h2>
<p>Latest persisted decision: {latest.decision_date.isoformat() if latest else 'none'};
{html.escape(latest.strategy_name) if latest else '—'} v{html.escape(latest.strategy_version) if latest else '—'};
universe {html.escape(latest.universe_mode) if latest else '—'}.</p>
<h3>Latest saved ranking and selection</h3>{render(ranking_frame, 'No persisted decision yet.')}
<h3>Backtest summary metrics (hypothetical)</h3>{render(pd.DataFrame(backtest_runs), 'No backtest reports found.')}
<h3>Historical monthly backtest returns</h3>{render(returns_frame, 'No monthly backtest returns found.')}
<h3>Backtest rank IC, quintile spread and labeled selection hit rate</h3>{render(ic_frame, 'No ranking-quality history found.')}
<small>Backtest metric panels disclose in-sample/holdout splits in each run summary; backtests are not actual returns.</small>
</section>
<section><h2>B. Current portfolio and next allocation</h2>
<h3 class="actual">Actual holdings and weights</h3>
{render(actual['holdings'], 'No actual holdings recorded.')}
<p class="actual">ACTUAL value EUR {actual['portfolio_value_eur']:.2f}; cash EUR {actual['cash_eur']:.2f};
realized P&amp;L EUR {actual['realized_pnl_eur']:.2f}; unrealized P&amp;L EUR {actual['unrealized_pnl_eur']:.2f}.</p>
<p>Valuation completeness: {actual['valuation_complete']}; unpriced actual holdings: {html.escape(', '.join(actual['unpriced_tickers']) or 'none')}.</p>
<h3 class="recommended">Recommended targets from latest persisted decision</h3>{render(target_frame, 'No saved recommendation.')}
<h3 class="recommended">Latest decision trades (assumed fills; recorded in ledger when auto-record was on)</h3>{render(trade_frame, 'No proposed trades.')}
<p>Next decision date: {(latest.decision_date + pd.offsets.MonthEnd(1)).date().isoformat() if latest else 'not scheduled'}.</p>
<p>Latest recommended risk status: {html.escape(latest_risk.status) if latest_risk else 'not assessed'}.
Actual risk status: {html.escape(actual_risk['status'])}.</p>
<pre>{html.escape(json.dumps(_json_safe({'latest_decision_risk': latest_risk.metrics if latest_risk else None, 'current_actual_risk': actual_risk}), indent=2))}</pre>
</section>
<section><h2>C. Overall actual portfolio</h2>
<p>Current actual value EUR {actual['portfolio_value_eur']:.2f}; cumulative net contributions EUR {sum(flows.values()):.2f};
realized P&amp;L EUR {actual['realized_pnl_eur']:.2f}; unrealized P&amp;L EUR {actual['unrealized_pnl_eur']:.2f};
daily time-weighted return cumulative {(float((1 + twr).prod() - 1) if not twr.empty else 0.0):.2%}.
Daily TWR assumes same-day deposits and withdrawals occur at close.</p>
<h3>Actual portfolio value history</h3>{render(nav_frame, 'No portfolio history.')}
<h3>Actual daily time-weighted return history</h3>{render(twr_frame, 'No TWR intervals yet.')}
<h3>Historical VaR/CVaR and risk assessment history</h3>{render(risk_frame, 'No saved risk assessments.')}
<p>VaR/CVaR use overlapping 1-day and 21-day historical return windows, positive-loss sign convention,
and empirical lower-order-statistic thresholds. These are historical estimates, not forecasts.</p>
</section></body></html>""")
    return output


@operations_group.command("monitor-refresh")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--output", default="reports/monitoring/index.html", show_default=True, type=click.Path(dir_okay=False, path_type=Path))
def monitor_refresh(config_path: str, output: Path):
    """Generate a local static HTML dashboard from decisions, ledger, and backtests."""
    generate_monitoring_dashboard(config_path, output)
    click.echo(f"Monitoring report written: {output}")


@operations_group.command("show-decision")
@click.argument("decision_id", type=int)
def show_decision(decision_id: int):
    """Re-display the stored decision snapshot without re-reading market prices."""
    init_db()
    with get_session() as session:
        decision = session.get(StrategyDecision, decision_id)
        if decision is None:
            raise click.ClickException(f"Decision {decision_id} was not found.")
        items = list(session.scalars(select(DecisionItem).where(
            DecisionItem.decision_id == decision_id
        ).order_by(DecisionItem.rank)))
        assessment = session.scalar(select(RiskAssessment).where(
            RiskAssessment.decision_id == decision_id
        ))
        snapshot = {
            "decision_id": decision.id,
            "strategy": decision.strategy_name,
            "version": decision.strategy_version,
            "parameter_hash": decision.parameter_hash,
            "decision_date": decision.decision_date.isoformat(),
            "data_cutoff_at": decision.data_cutoff_at.isoformat(),
            "universe_mode": decision.universe_mode,
            "generation": decision.generation,
            "status": decision.status,
            "parameters": decision.parameters,
            "items": [{
                "ticker": item.ticker,
                "score": item.score,
                "rank": item.rank,
                "selected": item.selected,
                "target_weight": item.target_weight,
                "feature_values": item.feature_values,
                "exclusion_reason": item.exclusion_reason,
            } for item in items],
            "risk": assessment.metrics if assessment else None,
        }
    click.echo(json.dumps(_json_safe(snapshot), indent=2, allow_nan=False))


@main.group("backtest")
def backtest_group():
    """Run and compare monthly historical simulations."""


@backtest_group.command("run")
@click.option("--config", "config_path", default="config/research.json", show_default=True, type=click.Path(exists=True))
@click.option("--start", help="First decision date (YYYY-MM-DD).")
@click.option("--end", help="Last decision date (YYYY-MM-DD).")
@click.option("--universe-mode", type=click.Choice(["tradable", "research"]))
@click.option("--include-holdout/--exclude-holdout", default=None, help="Include the reserved recent holdout in reports.")
@click.option("--ablation-off", is_flag=True, help="Disable all optional strategy and weighting components.")
def backtest_run(
    config_path: str,
    start: str | None,
    end: str | None,
    universe_mode: str | None,
    include_holdout: bool | None,
    ablation_off: bool,
):
    """Run the configured monthly backtest and write a reproducible report directory."""
    try:
        backtest_config, strategy_config, portfolio_config = load_research_config(config_path)
        changes: dict[str, object] = {}
        if start is not None:
            changes["start"] = start
        if end is not None:
            changes["end"] = end
        if universe_mode is not None:
            changes["universe_mode"] = universe_mode
        if include_holdout is not None:
            changes["include_holdout"] = include_holdout
        backtest_config = replace(backtest_config, **changes)
        strategy_config = replace(strategy_config, universe_mode=backtest_config.universe_mode)
        if ablation_off:
            strategy_config = replace(
                strategy_config,
                volatility_adjusted=False,
                absolute_momentum_filter=False,
                trend_filter=False,
            )
            portfolio_config = replace(
                portfolio_config,
                inverse_volatility=False,
                volatility_targeting=False,
            )
        with get_session() as session:
            prices, metadata = load_research_prices(
                session,
                base_currency=backtest_config.base_currency,
            )
        result = run_backtest(
            prices,
            metadata,
            strategy_config=strategy_config,
            portfolio_config=portfolio_config,
            config=backtest_config,
        )
        report_dir = write_backtest_report(result)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Backtest report: {report_dir}")
    click.echo(json.dumps(_json_safe(result["metrics"]), indent=2, allow_nan=False))


@backtest_group.command("compare")
@click.argument("run_ids", nargs=-1, required=True)
def backtest_compare(run_ids: tuple[str, ...]):
    """Compare summary metrics from one or more completed run directories."""
    summaries = []
    for run_id in run_ids:
        if Path(run_id).name != run_id or run_id in {".", ".."}:
            raise click.ClickException(f"Invalid run ID: {run_id}")
        summary_path = Path("reports/backtests") / run_id / "summary.json"
        try:
            summary = json.loads(summary_path.read_text())
        except FileNotFoundError as exc:
            raise click.ClickException(f"Backtest run not found: {run_id}") from exc
        summaries.append({"run_id": run_id, **summary["metrics"]})
    click.echo(pd.DataFrame(summaries).to_string(index=False))


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


def _json_safe(value):
    if isinstance(value, dict):
        safe = {}
        for key, item in value.items():
            safe_key = _json_safe(key)
            safe[safe_key if isinstance(safe_key, (str, int, float, bool)) else str(safe_key)] = _json_safe(item)
        return safe
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.str_):
        return str(value)
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    return value


if __name__ == "__main__":
    main()
