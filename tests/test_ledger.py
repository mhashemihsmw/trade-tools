"""Ledger accounting and the dashboard, using an in-memory database."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_tools.dashboard import portfolio_summary, render_html
from trade_tools.db.models import Asset
from trade_tools.ledger import (
    holdings, import_csv, load_transactions, portfolio_history, record_transaction, reverse_transaction,
)
from trade_tools.prices import Prices


@pytest.fixture
def session(db_session):
    db_session.add_all([Asset(ticker="AAA", type="etf"), Asset(ticker="BBB", type="etf")])
    db_session.flush()
    return db_session


def add(session, kind, day, quantity, **kwargs):
    return record_transaction(session, kind=kind, trade_date=date(2024, 1, day), quantity=quantity, **kwargs)


def test_cash_and_units_follow_transactions(session):
    add(session, "deposit", 2, 1000)
    add(session, "buy", 2, 10, ticker="AAA", price=50, fees=2)
    add(session, "dividend", 3, 5)
    add(session, "sell", 4, 4, ticker="AAA", price=60, fees=1)
    t = load_transactions(session)
    assert list(t["cash_flow_eur"]) == [1000, -502, 5, 239]
    assert t["unit_change"].sum() == 6
    assert t["external_flow_eur"].sum() == 1000


def test_invalid_transactions_are_rejected(session):
    with pytest.raises(ValueError, match="only 0 held"):
        add(session, "sell", 2, 1, ticker="AAA", price=10)
    with pytest.raises(ValueError, match="Unknown ticker"):
        add(session, "buy", 2, 1, ticker="ZZZ", price=10)
    with pytest.raises(ValueError, match="cash entry"):
        add(session, "deposit", 2, 100, ticker="AAA")


def test_reversed_transactions_are_ignored(session):
    add(session, "deposit", 2, 1000)
    wrong = add(session, "buy", 2, 10, ticker="AAA", price=50)
    reverse_transaction(session, wrong.id)
    assert list(load_transactions(session)["type"]) == ["deposit"]
    assert len(load_transactions(session, include_reversed=True)) == 3
    with pytest.raises(ValueError, match="already reversed"):
        reverse_transaction(session, wrong.id)


def test_holdings_use_average_cost(session):
    add(session, "deposit", 2, 5000)
    add(session, "buy", 2, 10, ticker="AAA", price=100)
    add(session, "buy", 3, 10, ticker="AAA", price=200)
    add(session, "sell", 4, 5, ticker="AAA", price=300)
    table = holdings(load_transactions(session), pd.Series({"AAA": 250.0}))
    row = table.loc["AAA"]
    assert row["units"] == 15
    assert row["avg_cost_eur"] == pytest.approx(150)
    assert row["realised_pnl_eur"] == pytest.approx(5 * (300 - 150))
    assert row["unrealised_pnl_eur"] == pytest.approx(15 * (250 - 150))


def test_time_weighted_return_ignores_deposits(session):
    add(session, "deposit", 1, 1000)
    add(session, "buy", 1, 10, ticker="AAA", price=100)
    add(session, "deposit", 3, 1000)  # new money is not performance
    close = pd.DataFrame({"AAA": [100.0, 110.0, 110.0]}, index=pd.bdate_range("2024-01-01", periods=3))
    history = portfolio_history(load_transactions(session), close)
    assert history["value_eur"].iloc[-1] == pytest.approx(2100)
    assert history["net_invested_eur"].iloc[-1] == 1000 + 1000
    assert history["twr_index"].iloc[-1] == pytest.approx(1.10)


def test_csv_import(session, tmp_path):
    path = tmp_path / "tx.csv"
    path.write_text(
        "date,type,quantity,ticker,price,fees\n"
        "2024-01-02,deposit,500,,,\n"
        "2024-01-02,buy,2,BBB,100,1\n"
    )
    assert import_csv(session, path) == 2
    tickers = load_transactions(session)["ticker"]
    assert tickers.isna().iloc[0] and tickers.iloc[1] == "BBB"


def test_dashboard_renders_summary_without_scripts(session):
    add(session, "deposit", 2, 1000)
    add(session, "buy", 2, 5, ticker="AAA", price=100)
    add(session, "buy", 2, 2, ticker="BBB", price=50)
    add(session, "sell", 3, 2, ticker="BBB", price=60)
    days = pd.bdate_range("2024-01-01", periods=5)
    close = pd.DataFrame({"AAA": np.linspace(100, 120, 5), "BBB": 60.0, "VWCE.DE": np.linspace(50, 55, 5)}, index=days)
    prices = Prices(close=close, adj_close=close, adj_open=close)

    summary = portfolio_summary(load_transactions(session), prices, ("VWCE.DE",))
    assert list(summary.holdings.index) == ["AAA"]
    assert list(summary.closed.index) == ["BBB"]
    assert summary.stats["value"] == pytest.approx(1000 - 500 - 100 + 120 + 5 * 120)
    assert summary.stats["benchmark_return"] == pytest.approx(55 / 51.25 - 1)

    page = render_html(summary, target_weights=pd.Series({"AAA": 0.4, "CCC": 0.2}))
    assert "<script" not in page
    assert "AAA" in page and "target only" in page and "€1,120.00" in page


def test_first_day_loss_from_fees_counts_in_time_weighted_return(session):
    add(session, "deposit", 1, 1000)
    add(session, "buy", 1, 10, ticker="AAA", price=100, fees=10)  # pay 1010, own 1000 worth
    close = pd.DataFrame({"AAA": [100.0]}, index=pd.bdate_range("2024-01-01", periods=1))
    history = portfolio_history(load_transactions(session), close)
    assert history["twr_index"].iloc[-1] == pytest.approx(990 / 1000)
