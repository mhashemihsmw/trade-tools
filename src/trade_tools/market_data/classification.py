"""Manual style-box / bond-matrix classifications (Yahoo does not provide them reliably).

Tickers not listed here default to 'N/A' for both columns.
"""
from typing import Dict

NOT_APPLICABLE = "N/A"

VALID_STYLE_BOX = {
    "Large Value", "Large Blend", "Large Growth",
    "Mid Value", "Mid Blend", "Mid Growth",
    "Small Value", "Small Blend", "Small Growth",
    NOT_APPLICABLE,
}

VALID_BOND_MATRIX = {
    "Short High-Quality", "Short Medium-Quality", "Short Low-Quality",
    "Intermediate High-Quality", "Intermediate Medium-Quality", "Intermediate Low-Quality",
    "Long High-Quality", "Long Medium-Quality", "Long Low-Quality",
    NOT_APPLICABLE,
}

LB, LG, LV, SB = "Large Blend", "Large Growth", "Large Value", "Small Blend"

STYLE_BOX: Dict[str, str] = {
    # Equities
    "AAPL": LG, "MSFT": LG, "NVDA": LG, "AMZN": LG, "GOOGL": LG, "META": LG,
    "BRK-B": LB, "JPM": LV, "JNJ": LB, "XOM": LV, "SAP.DE": LG, "ASML.AS": LG, "NESN.SW": LB,
    # US / global ETFs
    "SPY": LB, "QQQ": LG, "VTI": LB, "VT": LB, "VXUS": LB, "EFA": LB, "EEM": LB, "IWM": SB,
    # UCITS ETFs
    "VWCE.DE": LB, "EUNL.DE": LB, "IUSQ.DE": LB, "SPYI.DE": LB, "SXR8.DE": LB, "VUAA.DE": LB,
    "SXRV.DE": LG, "EQQB.DE": LG, "IS3N.DE": LB, "VFEA.DE": LB, "EXSA.DE": LB, "EXS1.DE": LB,
    "IQQJ.DE": LB, "EUNK.DE": LB, "IUSN.DE": SB, "ZPRS.DE": SB, "CUSS.L": SB,
    "QDVE.DE": LG, "EXV3.DE": LG, "WITS.L": LG, "QDVG.DE": LB, "EXV4.DE": LB,
    "QDVH.DE": LV, "EXV1.DE": LV, "QDVF.DE": LV, "IQQH.DE": "Mid Growth", "2B7D.DE": LB,
    "VAPX.L": LB, "EUNJ.DE": LB, "ICGA.DE": LB, "ASHR.L": LB, "36BZ.DE": LB, "KWBE.DE": LG,
    "VWCG.DE": LB,
    # REITs
    "IQQ6.DE": LV, "VNQ": "Mid Blend", "O": LV, "PLD": LB, "AMT": LB,
}

BOND_MATRIX: Dict[str, str] = {
    "AGG": "Intermediate High-Quality",
    "BND": "Intermediate High-Quality",
    "VAGF.DE": "Intermediate High-Quality",
    "IS00.MU": "Intermediate Low-Quality",
    "IUST.DE": "Intermediate High-Quality",
    "IUS5.DE": "Intermediate High-Quality",
    "IS04.DE": "Long High-Quality",
    "IBCD.DE": "Intermediate Medium-Quality",
    "EUN5.DE": "Intermediate Medium-Quality",
    "IUS7.DE": "Intermediate Low-Quality",
    "36BD.DE": "Short High-Quality",
}


def style_box_for(ticker: str) -> str:
    return STYLE_BOX.get(ticker, NOT_APPLICABLE)


def bond_matrix_for(ticker: str) -> str:
    return BOND_MATRIX.get(ticker, NOT_APPLICABLE)
