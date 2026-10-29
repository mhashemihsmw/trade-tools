import numpy as np
import pandas as pd

from trade_tools.features.research import (
    aligned_daily_returns,
    build_feature_panel,
    feature_frame,
    historical_returns,
    normalize_cross_section,
    realized_volatility,
    rolling_max_drawdown,
)
from trade_tools.market_data.research_prices import convert_to_base_currency
from trade_tools.market_data.research_universe import all_classifications
from trade_tools.market_data.initial_universe import INITIAL_ASSET_UNIVERSE
from trade_tools.portfolio.construction import PortfolioConfig, construct_target_weights
from trade_tools.portfolio.risk import validate_target_weights
from trade_tools.strategies.adaptive_momentum import AdaptiveMomentumStrategy, StrategyConfig
from trade_tools.strategies.backtest import (
    BacktestConfig,
    forward_holding_returns,
    monthly_decision_dates,
    portfolio_metrics,
    run_backtest,
)


def test_price_features_use_exact_trading_observation_lags_and_warmups():
    index = pd.bdate_range("2020-01-01", periods=300, tz="UTC")
    prices = pd.Series(np.arange(1.0, 301.0), index=index)
    returns = historical_returns(prices)
    assert pd.isna(returns.iloc[20]["r1"])
    assert returns.iloc[21]["r1"] == 22 / 1 - 1
    vol = realized_volatility(prices, window=3)
    assert pd.isna(vol.iloc[2])
    assert vol.iloc[3] > 0
    assert pd.isna(feature_frame(prices).iloc[251]["r12"])


def test_invalid_prices_missing_data_and_point_in_time_panel():
    index = pd.date_range("2024-01-01", periods=300, freq="D", tz="UTC")
    prices = pd.Series(np.arange(1.0, 301.0), index=index)
    prices.iloc[270] = 0
    features = feature_frame(prices)
    assert pd.isna(features.iloc[270]["price"])
    assert pd.isna(features.iloc[270]["r1"])
    decision = index[200]
    panel = build_feature_panel({"A": prices}, [decision])
    assert panel.iloc[0]["latest_observation"] == index[200]
    assert panel.iloc[0]["observations"] == 201


def test_future_price_changes_cannot_change_earlier_features():
    index = pd.bdate_range("2020-01-01", periods=400, tz="UTC")
    original = pd.Series(100 * np.cumprod(np.full(400, 1.001)), index=index)
    changed_future = original.copy()
    changed_future.loc[index[300]:] *= 3
    before_a = feature_frame(original).loc[:index[299]]
    before_b = feature_frame(changed_future).loc[:index[299]]
    pd.testing.assert_frame_equal(before_a, before_b)


def test_covariance_aligns_observed_calendar_dates_without_forward_fill():
    first = pd.DatetimeIndex(["2024-01-01 15:00Z", "2024-01-02 15:00Z", "2024-01-03 15:00Z"])
    second = pd.DatetimeIndex(["2024-01-01 21:00Z", "2024-01-03 21:00Z"])
    prices = pd.DataFrame({
        "A": pd.Series([100.0, 101.0, 102.0], index=first),
        "B": pd.Series([50.0, 52.0], index=second),
    })
    aligned = aligned_daily_returns(prices)
    assert list(aligned.index) == [pd.Timestamp("2024-01-03", tz="UTC")]
    assert aligned.shape == (1, 2)


def test_drawdown_cross_section_normalization_and_feature_panel():
    index = pd.bdate_range("2020-01-01", periods=260, tz="UTC")
    values = np.ones(260) * 100
    values[254:257] = [80, 90, 95]
    prices = pd.Series(values, index=index)
    dd = rolling_max_drawdown(prices, 5)
    assert np.isclose(dd.iloc[255], -0.2)
    panel = pd.DataFrame({
        "decision_date": [1, 1, 1],
        "score": [1.0, 2.0, 3.0],
    })
    normalized = normalize_cross_section(panel, "score")
    assert normalized["rank_01"].tolist() == [0.0, 0.5, 1.0]
    assert np.isclose(normalized["winsorized_zscore"].mean(), 0)


def test_base_currency_conversion_adjusted_open_gbp_pence_and_fx_gap():
    dates = pd.DatetimeIndex(["2024-01-02", "2024-01-20"], tz="UTC")
    source = pd.DataFrame({
        "open": [110.0, 110.0],
        "close": [120.0, 120.0],
        "adjusted_close": [120.0, 120.0],
    }, index=dates)
    fx_date = pd.DatetimeIndex(["2024-01-01"], tz="UTC")
    eur_usd = pd.Series([1.2], index=fx_date)
    usd = convert_to_base_currency(source, "USD", {"EURUSD=X": eur_usd}, max_fx_gap_days=5)
    assert np.isclose(usd.iloc[0]["adjusted_open"], 110 / 1.2)
    assert usd.iloc[1]["adjusted_close"] != usd.iloc[1]["adjusted_close"]

    pence = convert_to_base_currency(
        source.iloc[:1],
        "GBp",
        {
            "EURUSD=X": pd.Series([1.2], index=fx_date),
            "GBPUSD=X": pd.Series([1.2], index=fx_date),
        },
    )
    assert np.isclose(pence.iloc[0]["adjusted_close"], 1.2)
    assert np.isclose(pence.iloc[0]["adjusted_open"], 1.1)

    jpy = convert_to_base_currency(
        source.iloc[:1],
        "JPY",
        {
            "EURUSD=X": pd.Series([1.2], index=fx_date),
            "JPY=X": pd.Series([150.0], index=fx_date),
        },
    )
    assert np.isclose(jpy.iloc[0]["adjusted_close"], 120 / (150 * 1.2))


def test_classification_covers_seeded_assets_and_marks_uncertainty():
    classifications = all_classifications()
    seeded = {asset["ticker"] for asset in INITIAL_ASSET_UNIVERSE}
    assert set(classifications) == seeded
    assert classifications["O"].is_fund is False
    assert classifications["VWCE.DE"].is_fund is True
    assert classifications["VWCE.DE"].tradable is True
    assert classifications["SPY"].tradable is False
    assert classifications["ASHR.L"].uncertain is True


def test_strategy_score_deduplicates_and_uses_configurable_filters():
    config = StrategyConfig(minimum_history=1, volatility_adjusted=False, top_n=2)
    strategy = AdaptiveMomentumStrategy(config)
    features = pd.DataFrame([
        {"ticker": "SXR8.DE", "r1": 0.04, "r3": 0.10, "r6": 0.20, "r12": 0.40, "realized_vol": 0.2, "trend_distance": 0.1},
        {"ticker": "VUAA.DE", "r1": 0.03, "r3": 0.09, "r6": 0.19, "r12": 0.39, "realized_vol": 0.2, "trend_distance": 0.1},
        {"ticker": "SPY", "r1": 0.05, "r3": 0.11, "r6": 0.21, "r12": 0.41, "realized_vol": 0.2, "trend_distance": 0.1},
    ]).set_index("ticker", drop=False)
    metadata = {
        ticker: {"id": i, "ticker": ticker, "type": "etf", "currency": "EUR"}
        for i, ticker in enumerate(("SXR8.DE", "VUAA.DE", "SPY"))
    }
    report = strategy.rank(
        features,
        metadata,
        pd.Timestamp("2024-01-31", tz="UTC"),
        history_counts={ticker: 300 for ticker in metadata},
    )
    assert report.loc[report["ticker"] == "SPY", "exclusion_reason"].iloc[0] == "not_tradable"
    assert report.loc[report["selected"], "ticker"].tolist() == ["SXR8.DE"]
    expected = (12 * 0.04 + 4 * 0.10 + 2 * 0.20 + 0.40) / 4
    assert np.isclose(report.loc[report["ticker"] == "SXR8.DE", "score"].iloc[0], expected)


def test_strategy_fit_receives_only_past_labels_and_inactive_state_is_ignored():
    class FitSpy(AdaptiveMomentumStrategy):
        def fit(self, feature_panel, labels, as_of):
            self.fit_dates = pd.to_datetime(labels["decision_date"], utc=True).tolist()
            self.fit_panel_dates = pd.to_datetime(feature_panel["decision_date"], utc=True).tolist()

    as_of = pd.Timestamp("2024-02-01", tz="UTC")
    strategy = FitSpy(StrategyConfig(minimum_history=1, volatility_adjusted=False, top_n=1))
    features = pd.DataFrame([{
        "ticker": "VWCE.DE", "r1": 0.1, "r3": 0.1, "r6": 0.1, "r12": 0.1,
        "realized_vol": 0.2, "trend_distance": 0.1,
    }]).set_index("ticker", drop=False)
    metadata = {
        "VWCE.DE": {
            "id": 1, "ticker": "VWCE.DE", "type": "etf", "currency": "EUR",
            "active": False,
        },
    }
    labels = pd.DataFrame({
        "decision_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"], utc=True),
        "forward_return": [0.1, 0.2, 0.3],
    })
    panel = pd.DataFrame({
        "decision_date": pd.to_datetime(["2024-01-01", "2024-02-01"], utc=True),
        "feature": [1.0, 2.0],
    })
    report = strategy.rank(
        features, metadata, as_of,
        history_counts={"VWCE.DE": 300},
        feature_panel=panel,
        labels=labels,
    )
    assert strategy.fit_dates == [pd.Timestamp("2024-01-01", tz="UTC")]
    assert strategy.fit_panel_dates == [pd.Timestamp("2024-01-01", tz="UTC")]
    assert bool(report.loc[0, "selected"])


def test_portfolio_construction_caps_leave_excess_in_cash_and_risk_does_not_mutate():
    selected = pd.DataFrame([
        {"ticker": "A", "exposure_group": "a", "selected": True, "realized_vol": 0.1},
        {"ticker": "B", "exposure_group": "b", "selected": True, "realized_vol": 0.2},
    ])
    before = selected.copy(deep=True)
    weights = construct_target_weights(
        selected,
        PortfolioConfig(top_n=2, inverse_volatility=False, max_asset_weight=0.3,
                        max_exposure_group_weight=0.5, volatility_targeting=False),
    )
    assert np.allclose(weights["weight"], [0.3, 0.3])
    assert np.isclose(weights["cash_weight"].iloc[0], 0.4)
    assert selected.equals(before)
    assessment = validate_target_weights(weights)
    assert assessment["status"] == "valid"
    assert "Look-through constituent exposure is not available." in assessment["warnings"]
    invalid_cash = weights.copy()
    invalid_cash["cash_weight"] = 0.0
    assert validate_target_weights(invalid_cash)["status"] == "invalid"


def test_forward_labels_use_adjusted_execution_opens_and_monthly_dates():
    dates = pd.DatetimeIndex([
        "2024-01-31 00:00Z", "2024-02-01 00:00Z",
        "2024-02-29 00:00Z", "2024-03-01 00:00Z",
    ])
    frame = pd.DataFrame({"adjusted_open": [100, 105, 110, 120]}, index=dates)
    decisions = [dates[0], dates[2]]
    labels = forward_holding_returns({"A": frame}, decisions)
    assert np.isclose(labels.iloc[0]["forward_return"], 120 / 105 - 1)
    assert monthly_decision_dates({"A": frame}) == [dates[0], dates[2], dates[3]]


def test_monthly_decision_calendar_ignores_weekend_only_crypto_observations():
    fund_dates = pd.DatetimeIndex(["2024-03-28 00:00Z", "2024-03-29 00:00Z"])
    crypto_dates = pd.DatetimeIndex(["2024-03-29 00:00Z", "2024-03-31 00:00Z"])
    prices = {
        "VWCE.DE": pd.DataFrame({"adjusted_close": [100.0, 101.0]}, index=fund_dates),
        "BTC-USD": pd.DataFrame({"adjusted_close": [40000.0, 41000.0]}, index=crypto_dates),
    }
    assert monthly_decision_dates(prices) == [fund_dates[-1]]


def test_portfolio_metrics_and_synthetic_backtest_smoke():
    returns = pd.Series([0.01, -0.01, 0.02, 0.0])
    summary = portfolio_metrics(returns)
    assert summary["periods"] == 4
    assert np.isclose(summary["cumulative_return"], np.prod(1 + returns) - 1)

    days = pd.bdate_range("2018-01-01", periods=2200, tz="UTC")
    prices = {}
    metadata = {}
    for ticker, growth in (("VWCE.DE", 0.0005), ("EUNL.DE", 0.0004)):
        base = 100 * (1 + growth) ** np.arange(len(days))
        frame = pd.DataFrame({
            "adjusted_close": base,
            "adjusted_open": base * 0.999,
            "open": base * 0.999,
            "close": base,
        }, index=days)
        prices[ticker] = frame
        metadata[ticker] = {
            "id": ticker,
            "ticker": ticker,
            "type": "etf",
            "currency": "EUR",
            "country": "Germany",
            "sector": None,
        }
    result = run_backtest(
        prices,
        metadata,
        strategy_config=StrategyConfig(minimum_history=252, universe_mode="tradable", top_n=2),
        portfolio_config=PortfolioConfig(top_n=2, volatility_targeting=False, max_asset_weight=0.5,
                                         max_exposure_group_weight=0.5),
        config=BacktestConfig(universe_mode="tradable", holdout_months=12, include_holdout=True),
    )
    assert not result["monthly_returns"].empty
    assert {"in_sample", "holdout"} <= set(result["monthly_returns"]["period"])
    assert result["metrics"]["strategy"]["periods"] > 0
