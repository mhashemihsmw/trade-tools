from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trade_tools.db.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DailyPrice(Base):
    __tablename__ = "daily_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    open: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    close: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    adjusted_close: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    asset: Mapped["Asset"] = relationship("Asset", back_populates="daily_prices")

    __table_args__ = (
        UniqueConstraint("asset_id", "timestamp", name="uq_daily_prices_asset_timestamp"),
        Index("ix_daily_prices_asset_timestamp", "asset_id", "timestamp"),
    )

    def __repr__(self) -> str:
        return f"<DailyPrice(asset_id={self.asset_id}, timestamp='{self.timestamp}', close={self.close})>"


class HourlyPrice(Base):
    __tablename__ = "hourly_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    open: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    close: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    adjusted_close: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    asset: Mapped["Asset"] = relationship("Asset", back_populates="hourly_prices")

    __table_args__ = (
        UniqueConstraint("asset_id", "timestamp", name="uq_hourly_prices_asset_timestamp"),
        Index("ix_hourly_prices_asset_timestamp", "asset_id", "timestamp"),
    )

    def __repr__(self) -> str:
        return f"<HourlyPrice(asset_id={self.asset_id}, timestamp='{self.timestamp}', close={self.close})>"
