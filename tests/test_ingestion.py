import pytest
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import select

from trade_tools.db.models import Asset, DailyPrice, HourlyPrice
from trade_tools.market_data.ingestion import MarketDataIngestion
from trade_tools.market_data.initial_universe import INITIAL_ASSET_UNIVERSE


def test_seed_initial_assets(db_session):
    ingestion = MarketDataIngestion()
    seeded = ingestion.seed_initial_assets(db_session)
    assert seeded == len(INITIAL_ASSET_UNIVERSE)

    # Re-running seed should add 0 new assets
    seeded_again = ingestion.seed_initial_assets(db_session)
    assert seeded_again == 0


def test_ingest_asset_with_mocked_client(db_session, mock_yfinance_client):
    asset = Asset(ticker="AAPL", type="equity", active=True)
    db_session.add(asset)
    db_session.commit()

    # Setup mock returns
    mock_yfinance_client.get_asset_metadata.return_value = {
        "name": "Apple Inc.",
        "currency": "USD",
        "exchange": "NASDAQ",
        "country": "United States",
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }

    mock_daily_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2025-01-01 00:00:00+00:00")],
            "open": [150.0],
            "high": [155.0],
            "low": [149.0],
            "close": [154.0],
            "adjusted_close": [154.0],
            "volume": [50000.0],
        }
    )

    mock_hourly_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2025-01-01 10:00:00+00:00")],
            "open": [151.0],
            "high": [153.0],
            "low": [150.0],
            "close": [152.0],
            "adjusted_close": [152.0],
            "volume": [5000.0],
        }
    )

    def mock_get_prices(ticker, interval, start=None, end=None):
        if interval == "1d":
            return mock_daily_df
        elif interval == "1h":
            return mock_hourly_df
        return pd.DataFrame()

    mock_yfinance_client.get_prices.side_effect = mock_get_prices

    ingestion = MarketDataIngestion(client=mock_yfinance_client)
    res = ingestion.ingest_asset(db_session, asset)

    assert res.status == "success"
    assert res.daily_records == 1
    assert res.hourly_records == 1
    assert res.metadata_updated is True

    # Verify stored asset metadata
    db_session.refresh(asset)
    assert asset.name == "Apple Inc."

    # Verify stored prices
    daily_count = db_session.query(DailyPrice).filter_by(asset_id=asset.id).count()
    hourly_count = db_session.query(HourlyPrice).filter_by(asset_id=asset.id).count()
    assert daily_count == 1
    assert hourly_count == 1


def test_ingest_asset_failure_resilience(db_session, mock_yfinance_client):
    asset = Asset(ticker="FAIL_TICKER", type="equity", active=True)
    db_session.add(asset)
    db_session.commit()

    mock_yfinance_client.get_asset_metadata.side_effect = Exception("Network timeout")
    mock_yfinance_client.get_prices.side_effect = Exception("API error")

    ingestion = MarketDataIngestion(client=mock_yfinance_client)
    res = ingestion.ingest_asset(db_session, asset)

    assert res.status == "failed"
    assert "API error" in res.error or "Network timeout" in res.error
