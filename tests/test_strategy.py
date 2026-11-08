"""Prices, signals, selection, weights and the monthly decision, on synthetic data."""

import numpy as np
import pandas as pd
import pytest

from trade_tools.decision import make_decision, suggest_trades
from trade_tools.prices import prices_from_long, trading_dates
from trade_tools.selection import select_assets
from trade_tools.settings import Settings, load_settings, load_universe
from trade_tools.signals import asset_signals, compute_signals
from trade_tools.weights import cap_weights, compute_weights

SETTINGS = Settings()


def wide_prices(columns: dict[str, np.ndarray], start="2020-01-01") -> pd.DataFrame:
    n = len(next(iter(columns.values())))
    return pd.DataFrame(columns, index=pd.bdate_range(start, periods=n))


def universe(rows: list[tuple[str, str, bool]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["ticker", "group", "preferred"])
    frame["tradable"] = True
    frame["note"] = ""
    return frame.set_index("ticker", drop=False)


# --- settings ------------------------------------------------------------------------

def test_repository_config_files_load():
    settings = load_settings()
    table = load_universe()
    assert settings.top_n == 5 and settings.benchmark[0] == "VWCE.DE"
    assert table.loc["VWCE.DE", "preferred"] and table.loc["VWCE.DE", "tradable"]
    assert not table.loc["SPY", "tradable"]


def test_invalid_settings_are_rejected():
    with pytest.raises(ValueError, match="max_weight"):
        Settings(max_weight=1.5)


# --- prices --------------------------------------------------------------------------

def test_yahoo_midnight_local_stamps_map_to_the_trading_date():
    stamps = pd.Series(pd.to_datetime([
        "2026-11-05 23:00Z",  # Frankfurt midnight of 6 Nov (winter)
        "2026-11-06 05:00Z",  # New York midnight of 6 Nov
        "2026-11-06 00:00Z",  # London midnight of 6 Nov
    ]))
    assert (trading_dates(stamps) == pd.Timestamp("2026-11-06")).all()


def test_prices_are_converted_to_eur_and_bad_values_dropped():
    stamps = ["2024-01-02 05:00Z", "2024-01-03 05:00Z"]
    rows = pd.DataFrame({
        "ticker": ["USDFUND"] * 2 + ["PENCE"] * 2 + ["EURUSD=X"] * 2 + ["ODD"] * 2,
        "timestamp": pd.to_datetime(stamps * 4),
        "open": [100.0, 0.0, 1000.0, 1000.0, 1.25, 1.25, 1.0, 1.0],
        "close": [100.0, 110.0, 1000.0, 1000.0, 1.25, 1.25, 1.0, 1.0],
        "adjusted_close": [50.0, 55.0, 1000.0, 1000.0, 1.25, 1.25, 1.0, 1.0],
    })
    rows = pd.concat([rows, pd.DataFrame({
        "ticker": ["GBPUSD=X"] * 2, "timestamp": pd.to_datetime(stamps),
        "open": [1.5, 1.5], "close": [1.5, 1.5], "adjusted_close": [1.5, 1.5],
    })])
    prices = prices_from_long(rows, {"USDFUND": "USD", "PENCE": "GBp", "ODD": "XYZ"})
    day = pd.Timestamp("2024-01-02")
    assert prices.close.loc[day, "USDFUND"] == pytest.approx(100 / 1.25)
    assert prices.adj_open.loc[day, "USDFUND"] == pytest.approx(50 / 1.25)   # open x adj/close
    assert np.isnan(prices.adj_open.loc[pd.Timestamp("2024-01-03"), "USDFUND"])  # zero open
    assert prices.close.loc[day, "PENCE"] == pytest.approx(10 * 1.5 / 1.25)   # pence -> GBP -> EUR
    assert prices.skipped == {"EURUSD=X": "fx_rate", "GBPUSD=X": "fx_rate", "ODD": "unsupported_currency"}


# --- signals -------------------------------------------------------------------------

def test_signals_use_exact_trading_day_lags():
    prices = pd.Series(np.arange(1.0, 401.0), index=pd.bdate_range("2020-01-01", periods=400))
    signals = asset_signals(prices, SETTINGS)
    assert signals["r1"] == pytest.approx(400 / 379 - 1)
    assert signals["r12"] == pytest.approx(400 / 148 - 1)
    expected = (12 * signals["r1"] + 4 * signals["r3"] + 2 * signals["r6"] + signals["r12"]) / 4
    assert signals["momentum"] == pytest.approx(expected)
    assert signals["score"] == pytest.approx(signals["momentum"] / signals["volatility"])
    assert signals["trend_distance"] > 0  # rising prices sit above their moving average


def test_signals_ignore_prices_after_the_decision_date():
    rng = np.random.default_rng(0)
    close = wide_prices({"A": 100 * np.exp(np.cumsum(rng.normal(0, 0.01, 400)))})
    as_of = close.index[300]
    before = compute_signals(close, as_of, ["A"], SETTINGS)
    changed = close.copy()
    changed.iloc[301:] *= 3
    assert compute_signals(changed, as_of, ["A"], SETTINGS).equals(before)


def test_short_history_gives_missing_signals():
    signals = asset_signals(pd.Series([1.0, 2.0], index=pd.bdate_range("2024-01-01", periods=2)), SETTINGS)
    assert signals["n_obs"] == 2 and np.isnan(signals["score"])


# --- selection -----------------------------------------------------------------------

def make_signals(rows: dict[str, dict]) -> pd.DataFrame:
    defaults = {"n_obs": 500, "last_date": pd.Timestamp("2024-06-28"), "momentum": 0.1,
                "volatility": 0.2, "trend_distance": 0.05, "score": 0.5}
    return pd.DataFrame({ticker: {**defaults, **values} for ticker, values in rows.items()}).T.infer_objects()


def test_selection_explains_every_candidate():
    candidates = universe([
        ("SP_A", "sp500", True), ("SP_B", "sp500", False), ("GOLD", "gold", False),
        ("BOND", "bonds", False), ("EM", "em", False), ("NEW", "new", False), ("OLD", "old", False),
        ("DOWN", "down", False),
    ])
    signals = make_signals({
        "SP_A": {"score": 1.0}, "SP_B": {"score": 2.0},          # B scores higher, A is preferred
        "GOLD": {"score": 3.0}, "BOND": {"momentum": -0.01},
        "EM": {"trend_distance": -0.02}, "NEW": {"n_obs": 100},
        "OLD": {"last_date": pd.Timestamp("2024-05-01")}, "DOWN": {"score": 0.1},
    })
    table = select_assets(signals, candidates, pd.Timestamp("2024-06-28"), Settings(top_n=2))
    assert table["status"].to_dict() == {
        "GOLD": "selected", "SP_A": "selected", "DOWN": "not_in_top_n",
        "SP_B": "other_listing_in_group", "BOND": "negative_momentum", "EM": "below_trend",
        "NEW": "short_history", "OLD": "stale_price",
    }
    assert table.loc["GOLD", "rank"] == 1 and table.loc["SP_A", "rank"] == 2
    assert set(table.index[table["representative"]]) == {"SP_A", "GOLD", "BOND", "EM", "DOWN"}


def test_filters_can_be_switched_off():
    candidates = universe([("A", "a", False), ("B", "b", False)])
    signals = make_signals({"A": {"momentum": -0.1, "trend_distance": -0.1, "score": -0.5}, "B": {}})
    settings = Settings(absolute_momentum_filter=False, trend_filter=False)
    table = select_assets(signals, candidates, pd.Timestamp("2024-06-28"), settings)
    assert table["selected"].all()


# --- weights -------------------------------------------------------------------------

def test_cap_redistributes_excess_to_uncapped_assets():
    weights = cap_weights(pd.Series({"A": 0.6, "B": 0.25, "C": 0.15}), cap=0.4)
    assert weights["A"] == pytest.approx(0.4)
    assert weights.sum() == pytest.approx(1.0)
    assert weights["B"] / weights["C"] == pytest.approx(0.25 / 0.15)


def test_cap_leaves_cash_when_every_asset_is_capped():
    assert cap_weights(pd.Series({"A": 0.5, "B": 0.5}), cap=0.3).sum() == pytest.approx(0.6)


def test_weights_use_slot_budget_inverse_volatility_and_volatility_target():
    rng = np.random.default_rng(1)
    close = wide_prices({
        "CALM": 100 * np.exp(np.cumsum(rng.normal(0, 0.002, 100))),
        "WILD": 100 * np.exp(np.cumsum(rng.normal(0, 0.004, 100))),
    })
    selection = pd.DataFrame({"selected": [True, True], "volatility": [0.1, 0.2]}, index=["CALM", "WILD"])
    settings = Settings(top_n=4, max_weight=1.0, volatility_target=0)
    weights, _ = compute_weights(selection, close, settings)
    assert weights.sum() == pytest.approx(0.5)                     # 2 of 4 slots
    assert weights["CALM"] == pytest.approx(2 * weights["WILD"])    # 1/vol

    targeted, volatility = compute_weights(selection, close, Settings(top_n=4, max_weight=1.0, volatility_target=0.01))
    assert volatility == pytest.approx(0.01)
    assert targeted.sum() < weights.sum()


# --- decision and trades -------------------------------------------------------------

def test_decision_end_to_end_on_synthetic_prices():
    n = 400
    growth = {"UP": 0.001, "FLAT": 0.0, "DOWN": -0.001}
    rng = np.random.default_rng(2)
    close = wide_prices({
        t: 100 * np.exp(np.cumsum(g + rng.normal(0, 0.005, n))) for t, g in growth.items()
    })
    rows = close.stack().rename("adjusted_close").reset_index()
    rows.columns = ["date", "ticker", "adjusted_close"]
    rows["timestamp"] = pd.to_datetime(rows["date"]).dt.tz_localize("UTC")
    rows["open"] = rows["close"] = rows["adjusted_close"]
    prices = prices_from_long(rows, dict.fromkeys(growth, "EUR"))
    candidates = universe([(t, t.lower(), False) for t in growth])

    decision = make_decision(prices, candidates, SETTINGS, close.index[-1] + pd.Timedelta(days=2))
    assert decision.date == close.index[-1]
    assert decision.table.loc["UP", "selected"]
    assert decision.table.loc["DOWN", "status"] in {"negative_momentum", "below_trend"}
    assert decision.weights.sum() + decision.cash_weight == pytest.approx(1.0)


def test_suggested_trades_round_down_and_sell_first():
    trades = suggest_trades(
        weights=pd.Series({"NEW": 0.5}),
        holdings=pd.Series({"OLD": 10.0}),
        cash_eur=0.0,
        latest_prices=pd.Series({"NEW": 30.0, "OLD": 100.0}),
        settings=Settings(cost_bps=0, fixed_fee=1.0),
        contribution_eur=1000.0,
    )
    assert list(trades.index) == ["OLD", "NEW"]
    assert trades.loc["OLD", "side"] == "sell" and trades.loc["OLD", "units"] == 10
    assert trades.loc["NEW", "units"] == 33          # 50% of 2000 = 1000 / 30 = 33.3 -> 33
    assert trades.attrs["capital_eur"] == pytest.approx(2000)
    assert trades.attrs["cash_after_eur"] == pytest.approx(2000 - 990 - 2)


def test_history_before_an_impossible_price_jump_is_dropped():
    stamps = pd.to_datetime(["2024-01-02 00:00Z", "2024-01-03 00:00Z", "2024-01-04 00:00Z"])
    rows = pd.DataFrame({
        "ticker": "ETC", "timestamp": stamps,
        "open": [0.13, 234.0, 236.0], "close": [0.13, 234.0, 236.0], "adjusted_close": [0.13, 234.0, 236.0],
    })
    prices = prices_from_long(rows, {"ETC": "EUR"})
    assert prices.adj_close["ETC"].dropna().index.min() == pd.Timestamp("2024-01-03")
    assert "ETC" in prices.notes[0]
