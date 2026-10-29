"""Manual, point-in-time-independent research classifications for the seeded universe."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AssetClassification:
    is_fund: bool
    tradable: bool
    exposure_group: str
    preferred: bool = False
    uncertain: bool = False


_GROUPS = {
    "sp500": "SPY SXR8.DE VUAA.DE",
    "nasdaq100": "QQQ SXRV.DE EQQB.DE",
    "us_total_market": "VTI",
    "global_equity_all_world": "VT VWCE.DE IUSQ.DE SPYI.DE",
    "developed_world": "EUNL.DE",
    "developed_ex_us": "VXUS EFA",
    "emerging_markets": "EEM IS3N.DE VFEA.DE",
    "us_small_cap": "IWM CUSS.L",
    "world_small_cap": "IUSN.DE ZPRS.DE",
    "us_aggregate_bonds": "AGG BND",
    "europe_equity_600": "EXSA.DE",
    "germany_equity": "EXS1.DE",
    "japan_equity": "IQQJ.DE",
    "europe_equity": "EUNK.DE VWCG.DE",
    "us_information_technology": "QDVE.DE",
    "europe_information_technology": "EXV3.DE",
    "global_information_technology": "WITS.L",
    "us_healthcare": "QDVG.DE",
    "europe_healthcare": "EXV4.DE",
    "us_financials": "QDVH.DE",
    "europe_banks": "EXV1.DE",
    "us_energy": "QDVF.DE",
    "global_clean_energy": "IQQH.DE",
    "us_consumer_staples": "2B7D.DE",
    "asia_pacific_ex_japan": "VAPX.L EUNJ.DE",
    "china_equity": "ICGA.DE ASHR.L 36BZ.DE KWBE.DE",
    "gold": "GLD PPFB.DE",
    "silver": "SLV",
    "broad_commodities": "DBC",
    "copper": "COPA.L",
    "crude_oil": "CRUD.MI",
    "natural_gas": "NGAS.L",
    "corn": "CORN.L",
    "wheat": "WEAT.L",
    "soybeans": "SOYB.MI",
    "long_commodities": "ETL2.DE",
    "agriculture": "AGAP.L",
    "industrial_metals": "INDU.L",
    "global_aggregate_bonds_eur_hedged": "VAGF.DE",
    "us_high_yield_bonds": "IS00.MU",
    "us_tips": "IUST.DE",
    "global_inflation_linked_bonds": "IUS5.DE",
    "long_us_treasuries": "IS04.DE",
    "us_corporate_bonds": "IBCD.DE",
    "euro_corporate_bonds": "EUN5.DE",
    "em_bonds_usd": "IUS7.DE",
    "development_bank_bonds": "36BD.DE",
    "global_property": "IQQ6.DE",
    "us_property_reit": "VNQ",
    "us_equity_reit_o": "O",
    "us_equity_reit_pld": "PLD",
    "us_equity_reit_amt": "AMT",
}

_GROUP_BY_TICKER = {
    ticker: group
    for group, tickers in _GROUPS.items()
    for ticker in tickers.split()
}
_PREFERRED = {"VWCE.DE", "SXR8.DE", "SXRV.DE", "IS3N.DE", "VAGF.DE", "IQQ6.DE"}

_FUND_TICKERS = {
    item
    for group, tickers in _GROUPS.items()
    for item in tickers.split()
    if item not in {"O", "PLD", "AMT"}
}
_TRADABLE_TICKERS = {
    ticker
    for ticker in _GROUP_BY_TICKER
    if ticker.endswith((".DE", ".L", ".MI", ".MU", ".AS", ".SW"))
}
_TRADABLE_TICKERS.update({"SAP.DE", "ASML.AS", "NESN.SW"})
_TRADABLE_TICKERS.update({
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B",
    "JPM", "JNJ", "XOM", "O", "PLD", "AMT",
})

_OTHER = {
    "AAPL": "single_stock_aapl",
    "MSFT": "single_stock_msft",
    "NVDA": "single_stock_nvda",
    "AMZN": "single_stock_amzn",
    "GOOGL": "single_stock_googl",
    "META": "single_stock_meta",
    "BRK-B": "single_stock_brk_b",
    "JPM": "single_stock_jpm",
    "JNJ": "single_stock_jnj",
    "XOM": "single_stock_xom",
    "SAP.DE": "single_stock_sap",
    "ASML.AS": "single_stock_asml",
    "NESN.SW": "single_stock_nestle",
    "^TNX": "us_10y_yield",
    "^FVX": "us_5y_yield",
    "^IRX": "us_3m_yield",
    "BTC-USD": "bitcoin_spot",
    "ETH-USD": "ethereum_spot",
    "^GSPC": "sp500_index",
    "^IXIC": "nasdaq_composite_index",
    "^STOXX50E": "euro_stoxx_50_index",
    "^GDAXI": "dax_index",
    "^FTSE": "ftse_100_index",
    "^N225": "nikkei_225_index",
    "EURUSD=X": "eur_usd_fx",
    "GBPUSD=X": "gbp_usd_fx",
    "JPY=X": "usd_jpy_fx",
    "CHF=X": "usd_chf_fx",
}

UNCERTAIN_TICKERS = {"ASHR.L", "36BZ.DE", "KWBE.DE", "IQQH.DE"}


def classification_for(ticker: str) -> Optional[AssetClassification]:
    """Return a manually maintained classification, or None for an unreviewed ticker."""
    group = _GROUP_BY_TICKER.get(ticker) or _OTHER.get(ticker)
    if group is None:
        return None
    return AssetClassification(
        is_fund=ticker in _FUND_TICKERS,
        tradable=ticker in _TRADABLE_TICKERS,
        exposure_group=group,
        preferred=ticker in _PREFERRED,
        uncertain=ticker in UNCERTAIN_TICKERS,
    )


def all_classifications() -> dict[str, AssetClassification]:
    """Return a copy of the complete classification table."""
    return {
        ticker: classification_for(ticker)
        for ticker in sorted(set(_GROUP_BY_TICKER) | set(_OTHER))
        if classification_for(ticker) is not None
    }


def preferred_for(ticker: str) -> bool:
    classification = classification_for(ticker)
    return classification.preferred if classification else False
