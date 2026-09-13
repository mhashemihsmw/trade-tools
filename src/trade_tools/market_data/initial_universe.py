from typing import List, Dict

INITIAL_ASSET_UNIVERSE: List[Dict[str, str]] = [
    # Equities
    {"ticker": "AAPL", "type": "equity"},
    {"ticker": "MSFT", "type": "equity"},
    {"ticker": "NVDA", "type": "equity"},
    {"ticker": "AMZN", "type": "equity"},
    {"ticker": "GOOGL", "type": "equity"},
    {"ticker": "META", "type": "equity"},
    {"ticker": "BRK-B", "type": "equity"},
    {"ticker": "JPM", "type": "equity"},
    {"ticker": "JNJ", "type": "equity"},
    {"ticker": "XOM", "type": "equity"},
    {"ticker": "SAP.DE", "type": "equity"},
    {"ticker": "ASML.AS", "type": "equity"},
    {"ticker": "NESN.SW", "type": "equity"},
    # ETFs
    {"ticker": "SPY", "type": "etf"},
    {"ticker": "QQQ", "type": "etf"},
    {"ticker": "VTI", "type": "etf"},
    {"ticker": "VT", "type": "etf"},
    {"ticker": "VXUS", "type": "etf"},
    {"ticker": "EFA", "type": "etf"},
    {"ticker": "EEM", "type": "etf"},
    {"ticker": "IWM", "type": "etf"},
    {"ticker": "AGG", "type": "etf"},
    {"ticker": "BND", "type": "etf"},
    # REITs
    {"ticker": "VNQ", "type": "reit"},
    {"ticker": "O", "type": "reit"},
    {"ticker": "PLD", "type": "reit"},
    {"ticker": "AMT", "type": "reit"},
    # Fixed Income Proxies
    {"ticker": "^TNX", "type": "fixed_income_proxy"},
    {"ticker": "^FVX", "type": "fixed_income_proxy"},
    {"ticker": "^IRX", "type": "fixed_income_proxy"},
    # Commodities
    {"ticker": "GLD", "type": "commodity"},
    {"ticker": "SLV", "type": "commodity"},
    {"ticker": "DBC", "type": "commodity"},
    # Crypto
    {"ticker": "BTC-USD", "type": "crypto"},
    {"ticker": "ETH-USD", "type": "crypto"},
    # Indices
    {"ticker": "^GSPC", "type": "index"},
    {"ticker": "^IXIC", "type": "index"},
    {"ticker": "^STOXX50E", "type": "index"},
    {"ticker": "^GDAXI", "type": "index"},
    {"ticker": "^FTSE", "type": "index"},
    {"ticker": "^N225", "type": "index"},
    # FX
    {"ticker": "EURUSD=X", "type": "fx"},
    {"ticker": "GBPUSD=X", "type": "fx"},
    {"ticker": "JPY=X", "type": "fx"},
    {"ticker": "CHF=X", "type": "fx"},
]
