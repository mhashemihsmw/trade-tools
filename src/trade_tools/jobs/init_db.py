import logging
from trade_tools.db import init_db
from trade_tools.market_data.ingestion import MarketDataIngestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_init_db():
    logger.info("Initializing database tables...")
    init_db()
    ingestion = MarketDataIngestion()
    count = ingestion.seed_initial_assets()
    logger.info(f"Database initialized. Seeded {count} initial assets.")


if __name__ == "__main__":
    run_init_db()
