import logging
import sys
from trade_tools.db import init_db
from trade_tools.market_data.ingestion import MarketDataIngestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def run_ingestion(include_daily: bool = True, include_hourly: bool = True):
    logger.info("Starting scheduled market data ingestion job...")
    init_db()
    ingestion = MarketDataIngestion()
    summary = ingestion.ingest_all_active(
        include_daily=include_daily,
        include_hourly=include_hourly
    )

    if summary.failed_assets > 0:
        logger.warning(
            f"Job finished with failures: {summary.failed_assets} failed out of {summary.total_active_assets} active assets."
        )
        sys.exit(1)
    else:
        logger.info("Job finished successfully for all active assets.")
        sys.exit(0)


if __name__ == "__main__":
    run_ingestion()
