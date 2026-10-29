import logging
import json
import hashlib
import click
import numpy as np
import pandas as pd
from sqlalchemy import select
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from trade_tools.db import init_db, get_session
from trade_tools.db.models import Asset, VALID_ASSET_TYPES
from trade_tools.jobs.init_db import run_init_db
from trade_tools.market_data.ingestion import MarketDataIngestion
from trade_tools.market_data.research_prices import load_research_prices
from trade_tools.market_data.research_universe import classification_for
from trade_tools.portfolio.construction import construct_target_weights, target_values
from trade_tools.portfolio.risk import validate_target_weights, RiskLimits
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
            RiskLimits(
                max_asset_weight=portfolio_config.max_asset_weight,
                max_exposure_group_weight=portfolio_config.max_exposure_group_weight,
                volatility_target=portfolio_config.volatility_target,
                volatility_window=portfolio_config.covariance_window,
            ),
            asset_returns=returns,
            asset_prices=pd.DataFrame(historical_prices),
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
        RiskLimits(
            max_asset_weight=portfolio_config.max_asset_weight,
            max_exposure_group_weight=portfolio_config.max_exposure_group_weight,
            volatility_target=portfolio_config.volatility_target,
            volatility_window=portfolio_config.covariance_window,
        ),
        asset_returns=returns,
        asset_prices=pd.DataFrame(historical_prices),
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
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    return value


if __name__ == "__main__":
    main()
