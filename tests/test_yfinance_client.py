import pytest
import pandas as pd
from datetime import datetime, timezone
from trade_tools.market_data.clients.yfinance_client import YahooFinanceClient


def test_get_asset_metadata_success(mocker):
    mock_yf_ticker = mocker.MagicMock()
    mock_yf_ticker.info = {
        "longName": "Apple Inc.",
        "currency": "USD",
        "exchange": "NASDAQ",
        "country": "United States",
        "sector": "Technology",
    }
    mocker.patch("yfinance.Ticker", return_value=mock_yf_ticker)

    client = YahooFinanceClient()
    metadata = client.get_asset_metadata("AAPL")

    assert metadata["name"] == "Apple Inc."
    assert metadata["currency"] == "USD"
    assert metadata["sector"] == "Technology"


def test_get_asset_metadata_error_handling(mocker):
    mocker.patch("yfinance.Ticker", side_effect=Exception("API Error"))

    client = YahooFinanceClient()
    metadata = client.get_asset_metadata("INVALID")

    assert metadata["name"] is None
    assert metadata["currency"] is None


def test_get_prices_success(mocker):
    mock_df = pd.DataFrame(
        {
            "Open": [150.0],
            "High": [155.0],
            "Low": [149.0],
            "Close": [154.0],
            "Adj Close": [154.0],
            "Volume": [100000],
        },
        index=pd.DatetimeIndex(["2025-01-01 00:00:00+00:00"], name="Date"),
    )

    mock_yf_ticker = mocker.MagicMock()
    mock_yf_ticker.history.return_value = mock_df
    mocker.patch("yfinance.Ticker", return_value=mock_yf_ticker)

    client = YahooFinanceClient()
    prices_df = client.get_prices("AAPL", interval="1d", start="2025-01-01")

    assert not prices_df.empty
    assert "timestamp" in prices_df.columns
    assert prices_df["close"].iloc[0] == 154.0
    assert prices_df["volume"].iloc[0] == 100000


def test_get_prices_empty_response(mocker):
    mock_yf_ticker = mocker.MagicMock()
    mock_yf_ticker.history.return_value = pd.DataFrame()
    mocker.patch("yfinance.Ticker", return_value=mock_yf_ticker)

    client = YahooFinanceClient()
    prices_df = client.get_prices("EMPTY_TICKER", interval="1d")

    assert prices_df.empty
    assert "timestamp" in prices_df.columns
