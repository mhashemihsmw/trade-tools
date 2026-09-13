import logging
from contextlib import contextmanager
from typing import Generator, List, Dict, Any, Type, Optional
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_tools.config import config
from trade_tools.db.base import Base

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None
_SessionFactory: Optional[sessionmaker] = None


def get_engine(db_url: Optional[str] = None) -> Engine:
    global _engine
    url = db_url or config.DATABASE_URL
    if _engine is None or db_url is not None:
        # Enable connection pooling options if PostgreSQL
        connect_args = {}
        if url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
        if db_url is None:
            _engine = engine
        return engine
    return _engine


def get_sessionmaker(engine: Optional[Engine] = None) -> sessionmaker:
    global _SessionFactory
    eng = engine or get_engine()
    if _SessionFactory is None or engine is not None:
        factory = sessionmaker(bind=eng, autoflush=False, expire_on_commit=False)
        if engine is None:
            _SessionFactory = factory
        return factory
    return _SessionFactory


def init_db(engine: Optional[Engine] = None) -> None:
    eng = engine or get_engine()
    Base.metadata.create_all(bind=eng)
    logger.info("Database tables initialized successfully.")


@contextmanager
def get_session(engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    factory = get_sessionmaker(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_prices(session: Session, model_class: Any, records: List[Dict[str, Any]]) -> int:
    """Upsert price records into daily_prices or hourly_prices table idempotently.
    
    Returns the number of records upserted.
    """
    if not records:
        return 0

    bind = session.get_bind()
    dialect_name = bind.dialect.name

    update_cols = ["open", "high", "low", "close", "adjusted_close", "volume"]

    if dialect_name == "postgresql":
        stmt = pg_insert(model_class).values(records)
        set_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
        stmt = stmt.on_conflict_do_update(
            index_elements=["asset_id", "timestamp"],
            set_=set_dict
        )
        session.execute(stmt)
    elif dialect_name == "sqlite":
        stmt = sqlite_insert(model_class).values(records)
        set_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
        stmt = stmt.on_conflict_do_update(
            index_elements=["asset_id", "timestamp"],
            set_=set_dict
        )
        session.execute(stmt)
    else:
        # Generic fallback for other SQL dialects
        for rec in records:
            existing = (
                session.query(model_class)
                .filter_by(asset_id=rec["asset_id"], timestamp=rec["timestamp"])
                .first()
            )
            if existing:
                for col in update_cols:
                    setattr(existing, col, rec.get(col))
            else:
                session.add(model_class(**rec))

    return len(records)
