import logging
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Any, Optional
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from trade_tools.config import config
from trade_tools.db.models import Asset, DailyPrice, HourlyPrice, VALID_ASSET_TYPES
from trade_tools.db.session import get_session, upsert_prices, get_sessionmaker
from trade_tools.market_data.clients.yfinance_client import YahooFinanceClient
from trade_tools.market_data.classification import bond_matrix_for, style_box_for
from trade_tools.market_data.initial_universe import INITIAL_ASSET_UNIVERSE
from trade_tools.market_data.validation import DataValidation

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    ticker: str
    daily_records: int = 0
    hourly_records: int = 0
    metadata_updated: bool = False
    status: str = "success"
    error: Optional[str] = None


@dataclass
class JobSummary:
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    total_active_assets: int = 0
    successful_assets: int = 0
    failed_assets: int = 0
    total_daily_records: int = 0
    total_hourly_records: int = 0
    results: List[IngestionResult] = field(default_factory=list)


class MarketDataIngestion:
    """Orchestrates asset universe maintenance and historical price ingestion."""

    def __init__(self, session_factory=None, client: Optional[YahooFinanceClient] = None):
        self.session_factory = session_factory or get_sessionmaker()
        self.client = client or YahooFinanceClient()

    def seed_initial_assets(self, session: Optional[Session] = None) -> int:
        """Seed initial asset universe into the database if not already present."""
        def _seed(s: Session) -> int:
            added_count = 0
            for item in INITIAL_ASSET_UNIVERSE:
                ticker = item["ticker"]
                asset_type = item["type"]
                if asset_type not in VALID_ASSET_TYPES:
                    logger.warning(f"Invalid asset type '{asset_type}' for ticker {ticker}, skipping.")
                    continue

                style_box = style_box_for(ticker)
                bond_matrix = bond_matrix_for(ticker)
                existing = s.scalar(select(Asset).where(Asset.ticker == ticker))
                if existing:
                    existing.style_box_category = style_box
                    existing.bond_matrix_category = bond_matrix
                else:
                    new_asset = Asset(
                        ticker=ticker,
                        type=asset_type,
                        active=True,
                        data_source="yahoo_finance",
                        style_box_category=style_box,
                        bond_matrix_category=bond_matrix,
                    )
                    s.add(new_asset)
                    added_count += 1
            s.flush()
            logger.info(f"Seeded {added_count} new assets into the database.")
            return added_count

        if session:
            return _seed(session)
        else:
            with get_session() as s:
                return _seed(s)

    def update_asset_metadata(self, session: Session, asset: Asset) -> bool:
        """Retrieve and update Yahoo Finance metadata for an asset."""
        try:
            metadata = self.client.get_asset_metadata(asset.ticker)
            if not metadata:
                return False

            updated = False
            for key, val in metadata.items():
                if val is not None and getattr(asset, key) != val:
                    setattr(asset, key, val)
                    updated = True

            asset.metadata_updated_at = datetime.now(timezone.utc)
            return updated
        except Exception as e:
            logger.error(f"Failed updating metadata for {asset.ticker}: {e}")
            return False

    def get_latest_timestamp(self, session: Session, model_class: Any, asset_id: int) -> Optional[datetime]:
        """Query the latest stored timestamp for a given asset and price model."""
        stmt = select(func.max(model_class.timestamp)).where(model_class.asset_id == asset_id)
        return session.scalar(stmt)

    def ingest_asset(
        self,
        session: Session,
        asset: Asset,
        include_daily: bool = True,
        include_hourly: bool = True,
    ) -> IngestionResult:
        """Ingest daily and/or hourly price data and metadata for a single asset."""
        result = IngestionResult(ticker=asset.ticker)

        try:
            # 1. Update metadata
            result.metadata_updated = self.update_asset_metadata(session, asset)
            session.flush()

            # 2. Daily price ingestion
            if include_daily:
                latest_daily = self.get_latest_timestamp(session, DailyPrice, asset.id)
                if latest_daily:
                    start_daily = (latest_daily - timedelta(days=config.OVERLAP_DAYS_DAILY)).strftime("%Y-%m-%d")
                else:
                    start_daily = config.HISTORICAL_START_DATE

                daily_df = self.client.get_prices(
                    ticker=asset.ticker,
                    interval="1d",
                    start=start_daily,
                )
                valid_daily_df = DataValidation.validate_price_df(daily_df)

                if not valid_daily_df.empty:
                    daily_records = []
                    for row in valid_daily_df.to_dict(orient="records"):
                        rec = {
                            "asset_id": asset.id,
                            "timestamp": row["timestamp"].to_pydatetime(),
                            "open": row["open"] if pd.notna(row["open"]) else None,
                            "high": row["high"] if pd.notna(row["high"]) else None,
                            "low": row["low"] if pd.notna(row["low"]) else None,
                            "close": row["close"] if pd.notna(row["close"]) else None,
                            "adjusted_close": row["adjusted_close"] if pd.notna(row["adjusted_close"]) else None,
                            "volume": row["volume"] if pd.notna(row["volume"]) else None,
                        }
                        daily_records.append(rec)

                    result.daily_records = upsert_prices(session, DailyPrice, daily_records)

            # 3. Hourly price ingestion
            if include_hourly:
                latest_hourly = self.get_latest_timestamp(session, HourlyPrice, asset.id)
                if latest_hourly:
                    start_hourly = (latest_hourly - timedelta(days=config.OVERLAP_DAYS_HOURLY)).strftime("%Y-%m-%d")
                else:
                    # yfinance supports up to 730 days of hourly data (~2 years)
                    start_hourly = (datetime.now(timezone.utc) - timedelta(days=720)).strftime("%Y-%m-%d")

                hourly_df = self.client.get_prices(
                    ticker=asset.ticker,
                    interval="1h",
                    start=start_hourly,
                )
                valid_hourly_df = DataValidation.validate_price_df(hourly_df)

                if not valid_hourly_df.empty:
                    hourly_records = []
                    for row in valid_hourly_df.to_dict(orient="records"):
                        rec = {
                            "asset_id": asset.id,
                            "timestamp": row["timestamp"].to_pydatetime(),
                            "open": row["open"] if pd.notna(row["open"]) else None,
                            "high": row["high"] if pd.notna(row["high"]) else None,
                            "low": row["low"] if pd.notna(row["low"]) else None,
                            "close": row["close"] if pd.notna(row["close"]) else None,
                            "adjusted_close": row["adjusted_close"] if pd.notna(row["adjusted_close"]) else None,
                            "volume": row["volume"] if pd.notna(row["volume"]) else None,
                        }
                        hourly_records.append(rec)

                    result.hourly_records = upsert_prices(session, HourlyPrice, hourly_records)

            logger.info(
                f"Ingested {asset.ticker}: {result.daily_records} daily, {result.hourly_records} hourly records."
            )

        except Exception as e:
            logger.error(f"Error ingesting market data for {asset.ticker}: {e}", exc_info=True)
            result.status = "failed"
            result.error = str(e)

        return result

    def ingest_all_active(
        self,
        include_daily: bool = True,
        include_hourly: bool = True,
    ) -> JobSummary:
        """Ingest market data for all active assets in the database."""
        summary = JobSummary()

        with get_session() as session:
            # Seed initial assets if DB is empty
            self.seed_initial_assets(session)

            active_assets = list(session.scalars(select(Asset).where(Asset.active == True)))
            return self._ingest_assets(session, active_assets, include_daily, include_hourly)

    def ingest_tickers(
        self,
        tickers: List[str],
        include_daily: bool = True,
        include_hourly: bool = True,
    ) -> JobSummary:
        """Ingest only the specified active asset tickers."""
        requested_tickers = list(dict.fromkeys(ticker.strip().upper() for ticker in tickers))
        if not requested_tickers:
            raise ValueError("At least one ticker must be provided.")

        summary = JobSummary()
        with self.session_factory.begin() as session:
            assets = list(
                session.scalars(
                    select(Asset).where(
                        Asset.ticker.in_(requested_tickers),
                        Asset.active.is_(True),
                    )
                )
            )
            found_tickers = {asset.ticker for asset in assets}
            missing_tickers = sorted(set(requested_tickers) - found_tickers)
            if missing_tickers:
                raise ValueError(
                    "Unknown or inactive tickers: " + ", ".join(missing_tickers)
                )
            return self._ingest_assets(session, assets, include_daily, include_hourly)

    def _ingest_assets(
        self,
        session: Session,
        assets: List[Asset],
        include_daily: bool,
        include_hourly: bool,
    ) -> JobSummary:
        summary = JobSummary(total_active_assets=len(assets))
        logger.info(f"Starting ingestion job for {len(assets)} selected active assets.")

        for asset in assets:
            result = self.ingest_asset(
                session=session,
                asset=asset,
                include_daily=include_daily,
                include_hourly=include_hourly,
            )
            summary.results.append(result)

            if result.status == "success":
                summary.successful_assets += 1
                summary.total_daily_records += result.daily_records
                summary.total_hourly_records += result.hourly_records
            else:
                summary.failed_assets += 1

        summary.end_time = datetime.now(timezone.utc)
        logger.info(
            f"Completed ingestion job: {summary.successful_assets}/{summary.total_active_assets} succeeded, "
            f"inserted/updated {summary.total_daily_records} daily & {summary.total_hourly_records} hourly records."
        )
        return summary
