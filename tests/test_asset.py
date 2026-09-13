import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from trade_tools.db.models import Asset


def test_asset_creation(db_session):
    asset = Asset(
        ticker="AAPL",
        type="equity",
        name="Apple Inc.",
        currency="USD",
        exchange="NASDAQ",
        country="United States",
        active=True,
    )
    db_session.add(asset)
    db_session.commit()

    retrieved = db_session.scalar(select(Asset).where(Asset.ticker == "AAPL"))
    assert retrieved is not None
    assert retrieved.ticker == "AAPL"
    assert retrieved.type == "equity"
    assert retrieved.active is True
    assert retrieved.created_at is not None


def test_duplicate_ticker_prevention(db_session):
    asset1 = Asset(ticker="MSFT", type="equity", active=True)
    db_session.add(asset1)
    db_session.commit()

    asset2 = Asset(ticker="MSFT", type="equity", active=True)
    db_session.add(asset2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_deactivate_and_reactivate_asset(db_session):
    asset = Asset(ticker="NVDA", type="equity", active=True)
    db_session.add(asset)
    db_session.commit()

    # Deactivate
    asset.deactivate()
    db_session.commit()

    retrieved = db_session.scalar(select(Asset).where(Asset.ticker == "NVDA"))
    assert retrieved.active is False
    assert retrieved.deactivated_at is not None

    # Reactivate
    retrieved.reactivate()
    db_session.commit()

    retrieved_again = db_session.scalar(select(Asset).where(Asset.ticker == "NVDA"))
    assert retrieved_again.active is True
    assert retrieved_again.deactivated_at is None
