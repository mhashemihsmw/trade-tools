from datetime import date

import numpy as np
import pandas as pd

from trade_tools.db.models import Asset
from trade_tools.portfolio.operations import (
    TradeProposalConfig,
    derive_portfolio,
    propose_trades,
    record_transaction,
    reverse_transaction,
    time_weighted_returns,
)
from trade_tools.portfolio.risk import (
    RiskLimits,
    historical_var_cvar,
    stress_scenario_returns,
    validate_target_weights,
)


def test_ledger_uses_eur_average_cost_partial_sales_and_transaction_fx(db_session):
    asset = Asset(ticker="VWCE.DE", type="etf", currency="EUR")
    db_session.add(asset)
    db_session.flush()
    record_transaction(
        db_session, transaction_type="deposit", quantity=5000, trade_date=date(2024, 1, 1)
    )
    record_transaction(
        db_session, transaction_type="buy", quantity=10, trade_date=date(2024, 1, 2),
        asset_id=asset.id, execution_price=100, fees=10,
    )
    record_transaction(
        db_session, transaction_type="buy", quantity=10, trade_date=date(2024, 1, 3),
        asset_id=asset.id, execution_price=120,
    )
    record_transaction(
        db_session, transaction_type="sell", quantity=5, trade_date=date(2024, 1, 4),
        asset_id=asset.id, execution_price=130, fees=5,
    )
    dates = pd.DatetimeIndex(["2024-01-04"], tz="UTC")
    result = derive_portfolio(
        db_session,
        {"VWCE.DE": pd.DataFrame({"close": [125.0]}, index=dates)},
    )
    holding = result["holdings"].iloc[0]
    assert holding["quantity"] == 15
    assert np.isclose(holding["average_cost_eur"], 110.5)
    assert np.isclose(holding["realized_pnl_eur"], 92.5)
    assert np.isclose(holding["unrealized_pnl_eur"], 217.5)
    assert np.isclose(result["cash_eur"], 3435.0)
    assert np.isclose(result["portfolio_value_eur"], 5310.0)


def test_reversal_appends_entry_and_cancels_original_from_derived_holdings(db_session):
    asset = Asset(ticker="VWCE.DE", type="etf", currency="EUR")
    db_session.add(asset)
    db_session.flush()
    original = record_transaction(
        db_session, transaction_type="buy", quantity=2, trade_date=date(2024, 1, 1),
        asset_id=asset.id, execution_price=100,
    )
    original_id = original.id
    reversal = reverse_transaction(db_session, original_id)
    assert reversal.reversal_of_id == original_id
    assert db_session.get(type(original), original_id).id == original_id
    result = derive_portfolio(db_session, {})
    assert result["holdings"].empty
    assert result["cash_eur"] == 0


def test_execution_fx_is_applied_to_cost_basis_and_cash():
    from sqlalchemy.orm import Session
    from trade_tools.db import Base
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        asset = Asset(ticker="US_TEST", type="etf", currency="USD")
        session.add(asset)
        session.flush()
        record_transaction(
            session, transaction_type="deposit", quantity=100, trade_date=date(2024, 1, 1)
        )
        record_transaction(
            session, transaction_type="buy", quantity=1, trade_date=date(2024, 1, 2),
            asset_id=asset.id, execution_price=50, currency="USD", fx_to_eur=0.9, fees=1,
        )
        result = derive_portfolio(session, {})
    assert result["cash_eur"] == 54.1
    assert result["holdings"].iloc[0]["average_cost_eur"] == 45.9
    engine.dispose()


def test_time_weighted_return_excludes_external_deposits_and_withdrawals():
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True)
    nav = pd.Series([100.0, 120.0, 99.0], index=dates)
    flows = {dates[1].date(): 20.0, dates[2].date(): -33.0}
    result = time_weighted_returns(nav, flows)
    assert np.allclose(result.to_numpy(), [0.0, 0.1])
    assert np.isclose((1 + result).prod() - 1, 0.1)


def test_historical_var_cvar_uses_overlapping_windows_and_includes_threshold():
    returns = pd.Series([-0.10, -0.05, 0.0, 0.05, 0.10])
    result = historical_var_cvar(returns, confidence=0.6, horizons=(1,), lookback=5)["1d"]
    assert result["observations"] == 5
    assert np.isclose(result["threshold_return"], -0.05)
    assert np.isclose(result["var_loss"], 0.05)
    assert np.isclose(result["cvar_loss"], 0.075)
    two_day = historical_var_cvar(returns, confidence=0.6, horizons=(2,), lookback=5)["2d"]
    assert two_day["observations"] == 4


def test_stress_scenarios_apply_group_before_type_and_label_losses():
    weights = pd.DataFrame([
        {"ticker": "A", "weight": 0.6, "exposure_group": "equity", "type": "etf"},
        {"ticker": "B", "weight": 0.3, "exposure_group": "gold", "type": "etf"},
    ])
    results = stress_scenario_returns(weights, {
        "selloff": {
            "exposure_groups": {"equity": -0.3, "gold": 0.1},
            "types": {"etf": -0.2},
            "default": 0.0,
        },
    })
    assert np.isclose(results["selloff"]["portfolio_return"], -0.15)
    assert np.isclose(results["selloff"]["loss"], 0.15)


def test_risk_validation_applies_historical_limits_and_scenario_results():
    positions = pd.DataFrame([
        {"ticker": "A", "weight": 0.8, "cash_weight": 0.2, "exposure_group": "equity", "type": "etf"},
    ])
    history = pd.DataFrame({"A": np.full(40, -0.02)})
    result = validate_target_weights(
        positions,
        RiskLimits(
            max_asset_weight=1.0, max_exposure_group_weight=1.0,
            var_confidence=0.95, var_horizons=(1, 21), var_lookback=30,
            max_var_1d=0.01,
        ),
        asset_returns=history,
        stress_scenarios={"shock": {"exposure_groups": {"equity": -0.3}}},
    )
    assert result["status"] == "invalid"
    assert "Historical 1-day VaR exceeds the configured limit." in result["violations"]
    assert np.isclose(result["metrics"]["stress_scenarios"]["shock"]["loss"], 0.24)


def test_proposed_trades_round_whole_shares_and_apply_minimum_order_size():
    targets = pd.DataFrame([
        {"ticker": "A", "weight": 0.5},
        {"ticker": "B", "weight": 0.5},
    ])
    holdings = pd.DataFrame(columns=["ticker", "quantity", "market_value_eur", "price_eur"])
    result = propose_trades(
        targets, holdings, capital_eur=1000, cash_eur=1000,
        config=TradeProposalConfig(
            proportional_cost_bps=0, fixed_fee=0, minimum_trade_size=200,
            fractional_trading=False,
        ),
        market_prices={"A": 100, "B": 30},
    )
    trades = result["trades"].set_index("ticker")
    assert trades.loc["A", "quantity"] == 5
    assert trades.loc["B", "quantity"] == 16
    assert result["cash_after_trades_eur"] == 20


def test_proposed_buys_are_blocked_when_cash_is_insufficient():
    targets = pd.DataFrame([{"ticker": "A", "weight": 0.5}])
    result = propose_trades(
        targets, pd.DataFrame(), capital_eur=1000, cash_eur=20,
        config=TradeProposalConfig(proportional_cost_bps=0, fixed_fee=2),
        market_prices={"A": 100},
    )
    assert result["trades"].iloc[0]["side"] == "blocked"
    assert result["trades"].iloc[0]["reason"] == "insufficient_cash_for_proposed_buy"
    assert result["cash_after_trades_eur"] == 20


def test_decision_transactions_sell_before_buy_and_reverse(db_session):
    from trade_tools.portfolio.operations import (
        record_decision_transactions,
        reverse_decision_transactions,
    )

    old, new = Asset(ticker="OLD", type="etf", currency="EUR"), Asset(ticker="NEW", type="etf", currency="EUR")
    db_session.add_all([old, new])
    db_session.flush()
    record_transaction(db_session, transaction_type="deposit", quantity=1000, trade_date=date(2024, 1, 1))
    record_transaction(
        db_session, transaction_type="buy", quantity=10, trade_date=date(2024, 1, 2),
        asset_id=old.id, execution_price=100,
    )
    trades = pd.DataFrame([
        {"ticker": "NEW", "side": "buy", "quantity": 10.0, "price_eur": 100.0, "estimated_cost_eur": 2.0},
        {"ticker": "OLD", "side": "sell", "quantity": 10.0, "price_eur": 100.0, "estimated_cost_eur": 2.0},
        {"ticker": "NEW", "side": "blocked", "quantity": 0.0, "price_eur": 100.0, "estimated_cost_eur": 0.0},
    ])
    entries = record_decision_transactions(
        db_session, decision_id=7, trade_date=date(2024, 2, 1), trades=trades,
        asset_ids={"OLD": old.id, "NEW": new.id}, contribution_eur=50,
    )
    assert [entry.transaction_type for entry in entries] == ["deposit", "sell", "buy"]
    assert reverse_decision_transactions(db_session, 7) == 3
    assert reverse_decision_transactions(db_session, 7) == 0
