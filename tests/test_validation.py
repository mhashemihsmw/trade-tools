import pandas as pd
import numpy as np
from datetime import datetime, timezone
from trade_tools.market_data.validation import DataValidation


def test_validate_price_df_valid():
    data = {
        "timestamp": [
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
        ],
        "open": ["100.5", 102.0],
        "high": [105.0, 103.5],
        "low": [99.0, 101.0],
        "close": [104.0, 103.0],
        "adjusted_close": [104.0, 103.0],
        "volume": [1000, 1500],
    }
    df = pd.DataFrame(data)
    validated = DataValidation.validate_price_df(df)

    assert len(validated) == 2
    assert validated["open"].iloc[0] == 100.5
    assert pd.api.types.is_numeric_dtype(validated["open"])


def test_validate_price_df_duplicates_and_empty():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    data = {
        "timestamp": [ts, ts, None],
        "open": [100.0, 101.0, 102.0],
        "high": [105.0, 106.0, 107.0],
        "low": [99.0, 99.5, 98.0],
        "close": [104.0, 105.0, 106.0],
        "adjusted_close": [104.0, 105.0, 106.0],
        "volume": [1000, 2000, 3000],
    }
    df = pd.DataFrame(data)
    validated = DataValidation.validate_price_df(df)

    assert len(validated) == 1
    assert validated["open"].iloc[0] == 101.0


def test_validate_price_df_empty_dataframe():
    validated = DataValidation.validate_price_df(pd.DataFrame())
    assert validated.empty
    assert list(validated.columns) == [
        "timestamp", "open", "high", "low", "close", "adjusted_close", "volume"
    ]
