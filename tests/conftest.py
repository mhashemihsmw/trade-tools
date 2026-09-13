import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
