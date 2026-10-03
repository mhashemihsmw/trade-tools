from trade_tools.market_data.classification import (
    BOND_MATRIX, STYLE_BOX, VALID_BOND_MATRIX, VALID_STYLE_BOX, bond_matrix_for, style_box_for,
)
from trade_tools.market_data.initial_universe import INITIAL_ASSET_UNIVERSE


def test_classification_values_are_valid():
    assert set(STYLE_BOX.values()) <= VALID_STYLE_BOX
    assert set(BOND_MATRIX.values()) <= VALID_BOND_MATRIX


def test_classified_tickers_exist_in_universe():
    tickers = {a["ticker"] for a in INITIAL_ASSET_UNIVERSE}
    assert set(STYLE_BOX) <= tickers
    assert set(BOND_MATRIX) <= tickers
    assert not set(STYLE_BOX) & set(BOND_MATRIX)


def test_defaults_to_not_applicable():
    assert style_box_for("EURUSD=X") == "N/A"
    assert bond_matrix_for("AAPL") == "N/A"
