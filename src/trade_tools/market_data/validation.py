import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "adjusted_close", "volume"]
NUMERIC_PRICE_COLUMNS = ["open", "high", "low", "close", "adjusted_close", "volume"]


class DataValidation:
    """Utilities for validating downloaded price data prior to storage."""

    @staticmethod
    def validate_price_df(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate a DataFrame of price observations.
        
        - Ensures required columns exist.
        - Validates timezone-aware timestamps and drops invalid timestamps.
        - Coerces numeric price/volume columns to float/numeric.
        - Drops completely empty price rows.
        - Removes duplicate timestamps.
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)

        clean_df = df.copy()

        # Ensure required columns exist
        for col in REQUIRED_COLUMNS:
            if col not in clean_df.columns:
                clean_df[col] = np.nan

        # Convert and validate timestamps
        clean_df["timestamp"] = pd.to_datetime(clean_df["timestamp"], utc=True, errors="coerce")
        clean_df = clean_df.dropna(subset=["timestamp"])

        # Coerce numeric columns
        for col in NUMERIC_PRICE_COLUMNS:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

        # Drop rows where all price fields (open, high, low, close) are missing
        price_cols = ["open", "high", "low", "close"]
        clean_df = clean_df.dropna(subset=price_cols, how="all")

        # Drop duplicate timestamps, keeping the last observation
        clean_df = clean_df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")

        return clean_df[REQUIRED_COLUMNS]
