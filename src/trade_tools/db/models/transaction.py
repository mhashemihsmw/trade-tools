"""Ledger of real trades and cash movements. Rows are never edited; corrections are reversal rows."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import CheckConstraint, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from trade_tools.db.base import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Transaction(Base):
    """Immutable cash/security ledger entry; reversals are separate entries."""

    __tablename__ = "transactions"
    __table_args__ = (
        CheckConstraint(
            "transaction_type IN ('buy', 'sell', 'deposit', 'withdrawal', 'fee', 'dividend')",
            name="ck_transactions_type",
        ),
        CheckConstraint("quantity > 0", name="ck_transactions_quantity_positive"),
        CheckConstraint("fees >= 0", name="ck_transactions_fees_nonnegative"),
        CheckConstraint("fx_to_eur > 0", name="ck_transactions_fx_positive"),
        UniqueConstraint("reversal_of_id", name="uq_transactions_reversal"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[int | None] = mapped_column(
        ForeignKey("assets.id", ondelete="RESTRICT"), nullable=True, index=True
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    transaction_type: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    execution_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    fees: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="EUR")
    fx_to_eur: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    reversal_of_id: Mapped[int | None] = mapped_column(
        ForeignKey("transactions.id", ondelete="RESTRICT"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
