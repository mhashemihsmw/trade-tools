from contextlib import contextmanager
import json

import numpy as np
import pandas as pd
from click.testing import CliRunner

from trade_tools import cli
from trade_tools.db.models import Asset, DecisionItem, StrategyDecision, Transaction
from trade_tools.portfolio.construction import PortfolioConfig
from trade_tools.portfolio.operations import derive_portfolio
from trade_tools.strategies.adaptive_momentum import StrategyConfig
from trade_tools.strategies.backtest import BacktestConfig


def test_monthly_decision_is_idempotent_regenerates_and_replays_saved_features(
    db_session, mocker, tmp_path, monkeypatch
):
    asset = Asset(ticker="VWCE.DE", type="etf", currency="EUR", country="Germany")
    db_session.add(asset)
    db_session.flush()
    dates = pd.bdate_range("2023-10-01", "2024-01-31", tz="UTC")
    close = pd.Series(100 * np.cumprod(np.full(len(dates), 1.001)), index=dates)
    frame = pd.DataFrame({"close": close, "adjusted_close": close, "adjusted_open": close}, index=dates)
    metadata = {
        "VWCE.DE": {
            "id": asset.id, "ticker": "VWCE.DE", "type": "etf", "currency": "EUR",
            "country": "Germany", "sector": None,
        },
    }
    backtest_config = BacktestConfig(universe_mode="tradable")
    strategy_config = StrategyConfig(
        top_n=1, universe_mode="tradable", minimum_history=1,
        volatility_adjusted=False, absolute_momentum_filter=False, trend_filter=False,
    )
    portfolio_config = PortfolioConfig(
        top_n=1, inverse_volatility=False, max_asset_weight=1.0,
        max_exposure_group_weight=1.0, volatility_target=0.0,
        volatility_targeting=False,
    )
    config_path = tmp_path / "research.json"
    config_path.write_text("{}")
    mocker.patch.object(cli, "init_db")
    mocker.patch.object(
        cli,
        "load_research_config",
        return_value=(backtest_config, strategy_config, portfolio_config),
    )
    calls = {"count": 0}
    score = {"value": 0.25}

    def research_state(*_args, **_kwargs):
        calls["count"] += 1
        ranked = pd.DataFrame([{
            **metadata["VWCE.DE"],
            "ticker": "VWCE.DE",
            "score": score["value"],
            "rank": 1,
            "selected": True,
            "eligible": True,
            "exclusion_reason": None,
            "exposure_group": "global_equity_all_world",
            "preferred": True,
            "history_count": len(close),
            "universe_mode": "tradable",
            "r1": 0.01,
            "r3": 0.02,
            "r6": 0.03,
            "r12": 0.04,
            "realized_vol": 0.1,
            "trend_distance": 0.1,
        }])
        return (
            pd.Timestamp("2024-01-31", tz="UTC"),
            ranked,
            {"VWCE.DE": close},
            metadata,
            backtest_config,
            strategy_config,
            portfolio_config,
            {"VWCE.DE": frame},
        )

    mocker.patch.object(cli, "_read_research_state", side_effect=research_state)

    @contextmanager
    def test_session():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    mocker.patch.object(cli, "get_session", test_session)
    runner = CliRunner()
    common = [
        "operations", "monthly-decision", "--date", "2024-01-31",
        "--new-contribution", "1000", "--no-refresh-history",
        "--config", str(config_path), "--reports-dir", str(tmp_path / "reports"),
    ]
    first = runner.invoke(cli.main, common)
    assert first.exit_code == 0, first.output
    assert "Persisted decision" in first.output, first.output
    assert db_session.query(StrategyDecision).count() == 1, first.output
    original_decision = db_session.query(StrategyDecision).one()
    original_item = db_session.query(DecisionItem).filter_by(
        decision_id=original_decision.id
    ).one()
    assert original_item.feature_values["score"] == 0.25
    assert calls["count"] == 1

    score["value"] = 0.99
    repeated = runner.invoke(cli.main, common)
    assert repeated.exit_code == 0, repeated.output
    assert "returned unchanged" in repeated.output
    assert calls["count"] == 1
    assert db_session.query(StrategyDecision).count() == 1
    replay = runner.invoke(cli.main, ["operations", "show-decision", str(original_decision.id)])
    assert replay.exit_code == 0, replay.output
    saved = json.loads(replay.output)
    assert saved["items"][0]["feature_values"]["score"] == 0.25

    regenerated = runner.invoke(cli.main, [*common, "--regenerate"])
    assert regenerated.exit_code == 0, regenerated.output
    db_session.expire_all()
    decisions = db_session.query(StrategyDecision).order_by(StrategyDecision.generation).all()
    assert [decision.status for decision in decisions] == ["superseded", "proposed"]
    assert [decision.generation for decision in decisions] == [1, 2]
    new_item = db_session.query(DecisionItem).filter_by(
        decision_id=decisions[1].id
    ).one()
    assert new_item.feature_values["score"] == 0.99
    mocker.patch.object(cli, "load_research_prices", return_value=({}, metadata))
    monkeypatch.chdir(tmp_path)
    dashboard = runner.invoke(cli.main, [
        "operations", "monitor-refresh", "--config", str(config_path),
        "--output", str(tmp_path / "monitoring" / "index.html"),
    ])
    assert dashboard.exit_code == 0, dashboard.output
    dashboard_html = (tmp_path / "monitoring" / "index.html").read_text()
    assert "A. Strategy" in dashboard_html
    assert "B. Current portfolio and next allocation" in dashboard_html
    assert "C. Overall actual portfolio" in dashboard_html


def test_transaction_csv_import_persists_cash_and_trade_entries(db_session, mocker, tmp_path):
    asset = Asset(ticker="VWCE.DE", type="etf", currency="EUR")
    db_session.add(asset)
    db_session.flush()
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(
        "date,type,ticker,quantity,price,fees,currency,fx_to_eur,reference\n"
        "2024-01-01,deposit,,1000,,,EUR,1,deposit-1\n"
        "2024-01-02,buy,VWCE.DE,2,100,2,EUR,1,buy-1\n"
    )
    mocker.patch.object(cli, "init_db")

    @contextmanager
    def test_session():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    mocker.patch.object(cli, "get_session", test_session)
    result = CliRunner().invoke(cli.main, ["transaction", "import-csv", str(csv_path)])
    assert result.exit_code == 0, result.output
    assert "Imported 2 transactions" in result.output
    assert db_session.query(Transaction).count() == 2
    prices = {"VWCE.DE": pd.DataFrame(
        {"close": [110.0]}, index=pd.DatetimeIndex(["2024-01-03"], tz="UTC")
    )}
    portfolio = derive_portfolio(db_session, prices)
    assert portfolio["holdings"].iloc[0]["quantity"] == 2
    assert portfolio["cash_eur"] == 798
