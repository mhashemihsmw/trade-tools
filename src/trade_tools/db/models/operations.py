"""Additive persistence models for portfolio operations and decisions."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
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


class StrategyDecision(Base):
    """Immutable decision snapshot identified by strategy/configuration/date."""

    __tablename__ = "strategy_decisions"
    __table_args__ = (
        UniqueConstraint(
            "strategy_name",
            "strategy_version",
            "parameter_hash",
            "decision_date",
            "universe_mode",
            "generation",
            name="uq_strategy_decision_generation",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    parameter_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    decision_date: Mapped[date] = mapped_column(Date, nullable=False)
    universe_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    data_cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="proposed")
    report_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )


class DecisionItem(Base):
    """Feature, ranking, selection and target snapshot for one decision asset."""

    __tablename__ = "decision_items"
    __table_args__ = (
        UniqueConstraint("decision_id", "ticker", name="uq_decision_items_ticker"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_decisions.id", ondelete="CASCADE"), nullable=False, index=True
    )
    asset_id: Mapped[int | None] = mapped_column(
        ForeignKey("assets.id", ondelete="SET NULL"), nullable=True
    )
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    selected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    target_weight: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    feature_values: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    exclusion_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)


class RiskAssessment(Base):
    """Persisted risk outputs for a decision, including assessed portfolios."""

    __tablename__ = "risk_assessments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_decisions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    limits: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    violations: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    warnings: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
