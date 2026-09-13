import pytest
from datetime import datetime, timezone
from sqlalchemy.exc import IntegrityError
from trade_tools.db.models import Asset, DailyPrice, HourlyPrice
from trade_tools.db.session import upsert_prices


def test_daily_price_uniqueness(db_session):
    asset = Asset(ticker="SPY", type="etf", active=True)
    db_session.add(asset)
    db_session.commit()

    ts = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    p1 = DailyPrice(asset_id=asset.id, timestamp=ts, close=500.0)
    db_session.add(p1)
    db_session.commit()

    p2 = DailyPrice(asset_id=asset.id, timestamp=ts, close=502.0)
    db_session.add(p2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_hourly_price_uniqueness(db_session):
    asset = Asset(ticker="QQQ", type="etf", active=True)
    db_session.add(asset)
    db_session.commit()

    ts = datetime(2025, 1, 1, 14, 0, tzinfo=timezone.utc)
    p1 = HourlyPrice(asset_id=asset.id, timestamp=ts, close=400.0)
    db_session.add(p1)
    db_session.commit()

    p2 = HourlyPrice(asset_id=asset.id, timestamp=ts, close=401.0)
    db_session.add(p2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_upsert_prices_behavior(db_session):
    asset = Asset(ticker="GLD", type="commodity", active=True)
    db_session.add(asset)
    db_session.commit()

    ts = datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc)
    records_initial = [
        {
            "asset_id": asset.id,
            "timestamp": ts,
            "open": 180.0,
            "high": 182.0,
            "low": 179.0,
            "close": 181.0,
            "adjusted_close": 181.0,
            "volume": 10000.0,
        }
    ]

    count1 = upsert_prices(db_session, DailyPrice, records_initial)
    db_session.commit()
    assert count1 == 1

    # Insert updated observation for same timestamp
    records_updated = [
        {
            "asset_id": asset.id,
            "timestamp": ts,
            "open": 180.0,
            "high": 183.0,
            "low": 179.0,
            "close": 182.5,
            "adjusted_close": 182.5,
            "volume": 12000.0,
        }
    ]

    count2 = upsert_prices(db_session, DailyPrice, records_updated)
    db_session.commit()
    assert count2 == 1

    stored = db_session.query(DailyPrice).filter_by(asset_id=asset.id, timestamp=ts).first()
    assert stored is not None
    assert stored.close == 182.5
    assert stored.volume == 12000.0
