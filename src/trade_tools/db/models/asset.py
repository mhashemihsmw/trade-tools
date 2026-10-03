from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trade_tools.db.base import Base

VALID_ASSET_TYPES = {
    "equity",
    "etf",
    "reit",
    "fixed_income_proxy",
    "commodity",
    "crypto",
    "index",
    "fx",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    style_box_category: Mapped[str] = mapped_column(
        String(32), default="N/A", server_default="N/A", nullable=False
    )
    bond_matrix_category: Mapped[str] = mapped_column(
        String(32), default="N/A", server_default="N/A", nullable=False
    )
    data_source: Mapped[Optional[str]] = mapped_column(String(64), default="yahoo_finance", nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    metadata_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    daily_prices: Mapped[List["DailyPrice"]] = relationship(
        "DailyPrice", back_populates="asset", cascade="all, delete-orphan"
    )
    hourly_prices: Mapped[List["HourlyPrice"]] = relationship(
        "HourlyPrice", back_populates="asset", cascade="all, delete-orphan"
    )

    def deactivate(self) -> None:
        self.active = False
        self.deactivated_at = utc_now()
        self.updated_at = utc_now()

    def reactivate(self) -> None:
        self.active = True
        self.deactivated_at = None
        self.updated_at = utc_now()

    def __repr__(self) -> str:
        return f"<Asset(id={self.id}, ticker='{self.ticker}', type='{self.type}', active={self.active})>"
