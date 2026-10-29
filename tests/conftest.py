import os
from pathlib import Path

import pytest
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_repo_root = Path(__file__).resolve().parents[1]
_production_database_url = dotenv_values(_repo_root / ".env").get("DATABASE_URL")
_configured_database_url = os.environ.get("DATABASE_URL")
_test_database_url = os.environ.get("TEST_DATABASE_URL", "sqlite:///:memory:")


def ensure_isolated_test_database(
    configured_url: str | None,
    test_url: str | None,
    production_url: str | None,
) -> None:
    if production_url and configured_url == production_url:
        raise pytest.UsageError("Tests refuse to run with the production database configured.")
    if production_url and test_url == production_url:
        raise pytest.UsageError("TEST_DATABASE_URL must not point to the production database.")


ensure_isolated_test_database(
    _configured_database_url,
    _test_database_url,
    _production_database_url,
)
os.environ["DATABASE_URL"] = _test_database_url

from trade_tools.db import Base
from trade_tools.market_data.clients.yfinance_client import YahooFinanceClient


@pytest.fixture
def db_engine():
    """Create an in-memory SQLite database engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture
def db_session(db_engine):
    """Provide a transactional SQLAlchemy session for testing."""
    Session = sessionmaker(bind=db_engine, autoflush=False, expire_on_commit=False)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def mock_yfinance_client(mocker):
    """Provide a mocked YahooFinanceClient."""
    mock_client = mocker.MagicMock(spec=YahooFinanceClient)
    return mock_client
