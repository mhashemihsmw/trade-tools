import logging
from datetime import datetime, date, timezone
from typing import Dict, Any, Optional, Union
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class YahooFinanceClient:
    """Client for interacting with Yahoo Finance via yfinance."""

    def get_asset_metadata(self, ticker: str) -> Dict[str, Any]:
        """Retrieve asset metadata for a given ticker.
        
        Returns a dict containing name, currency, exchange, country, sector, etc.
        Missing attributes will be set to None.
        """
        metadata: Dict[str, Any] = {
            "name": None,
            "currency": None,
            "exchange": None,
            "country": None,
            "sector": None,
        }

        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info or {}

            if not info or not isinstance(info, dict):
                logger.warning(f"No metadata returned for ticker {ticker}")
                return metadata

            metadata["name"] = info.get("longName") or info.get("shortName") or info.get("name")
            metadata["currency"] = info.get("currency")
            metadata["exchange"] = info.get("exchange") or info.get("fullExchangeName")
            metadata["country"] = info.get("country")
            metadata["sector"] = info.get("sector")

        except Exception as e:
            logger.error(f"Error fetching metadata for ticker {ticker}: {e}")

        return metadata

    def get_prices(
        self,
        ticker: str,
        interval: str = "1d",
        start: Optional[Union[datetime, date, str]] = None,
        end: Optional[Union[datetime, date, str]] = None,
    ) -> pd.DataFrame:
        """Download historical price observations for a given ticker and interval.
        
        Returns a standardized pandas DataFrame with UTC timezone-aware 'timestamp' column
        and lower-case price columns: ['timestamp', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'].
        """
        empty_df = pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "adjusted_close", "volume"]
        )

        try:
            yf_ticker = yf.Ticker(ticker)
            
            # Convert start and end to standard format if needed
            start_str = start.strftime("%Y-%m-%d") if isinstance(start, (datetime, date)) else start
            end_str = end.strftime("%Y-%m-%d") if isinstance(end, (datetime, date)) else end

            # Download history
            # Note: For hourly interval ('1h'), yfinance supports period up to 730d or start/end
            df = yf_ticker.history(
                interval=interval,
                start=start_str,
                end=end_str,
                auto_adjust=False,
            )

            if df is None or df.empty:
                logger.info(f"No price data returned for ticker {ticker} ({interval})")
                return empty_df

            # Handle MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()

            # Identify timestamp column
            date_col = None
            for col in ["Date", "Datetime", "date", "datetime", "index"]:
                if col in df.columns:
                    date_col = col
                    break

            if date_col is None:
                logger.warning(f"No date column found in response for {ticker}")
                return empty_df

            # Convert timestamp to UTC datetime
            df["timestamp"] = pd.to_datetime(df[date_col], utc=True)

            # Map column names
            col_map = {
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adjusted_close",
                "Volume": "volume",
            }
            df = df.rename(columns=col_map)

            # If 'adjusted_close' is missing, fallback to 'close'
            if "adjusted_close" not in df.columns and "close" in df.columns:
                df["adjusted_close"] = df["close"]

            required_cols = ["timestamp", "open", "high", "low", "close", "adjusted_close", "volume"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None

            return df[required_cols]

        except Exception as e:
            logger.error(f"Failed to fetch prices for ticker {ticker} ({interval}): {e}")
            return empty_df
