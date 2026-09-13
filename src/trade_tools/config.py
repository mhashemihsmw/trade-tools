import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


class Config:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/trade_tools"
    )

    # Historical start date default (~20 years of daily history)
    _default_historical_start = (datetime.now(timezone.utc) - timedelta(days=365 * 20)).strftime("%Y-%m-%d")
    HISTORICAL_START_DATE: str = os.getenv("HISTORICAL_START_DATE", "2005-01-01")

    OVERLAP_DAYS_DAILY: int = int(os.getenv("OVERLAP_DAYS_DAILY", "5"))
    OVERLAP_DAYS_HOURLY: int = int(os.getenv("OVERLAP_DAYS_HOURLY", "2"))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
