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
    # Additional UCITS ETFs and ETCs (EUR/Xetra where available)
    {"ticker": "VWCE.DE", "type": "etf"},  # Vanguard FTSE All-World, accumulating
    {"ticker": "EUNL.DE", "type": "etf"},  # iShares Core MSCI World, accumulating
    {"ticker": "IUSQ.DE", "type": "etf"},  # iShares MSCI ACWI, accumulating
    {"ticker": "SPYI.DE", "type": "etf"},  # SPDR MSCI ACWI IMI, accumulating
    {"ticker": "SXR8.DE", "type": "etf"},  # iShares Core S&P 500, accumulating
    {"ticker": "VUAA.DE", "type": "etf"},  # Vanguard S&P 500, accumulating
    {"ticker": "SXRV.DE", "type": "etf"},  # iShares NASDAQ 100, accumulating
    {"ticker": "EQQB.DE", "type": "etf"},  # Invesco NASDAQ-100, accumulating
    {"ticker": "IS3N.DE", "type": "etf"},  # iShares Core MSCI EM IMI, accumulating
    {"ticker": "VFEA.DE", "type": "etf"},  # Vanguard FTSE Emerging Markets, accumulating
    {"ticker": "EXSA.DE", "type": "etf"},  # iShares Core STOXX Europe 600
    {"ticker": "EXS1.DE", "type": "etf"},  # iShares Core DAX
    {"ticker": "IQQJ.DE", "type": "etf"},  # iShares MSCI Japan (distributing share class)
    {"ticker": "EUNK.DE", "type": "etf"},  # iShares MSCI Europe, accumulating
    {"ticker": "IUSN.DE", "type": "etf"},  # iShares MSCI World Small Cap, accumulating
    {"ticker": "ZPRS.DE", "type": "etf"},  # SPDR MSCI World Small Cap
    {"ticker": "CUSS.L", "type": "etf"},  # iShares MSCI USA Small Cap
    {"ticker": "QDVE.DE", "type": "etf"},  # iShares S&P 500 Information Technology
    {"ticker": "EXV3.DE", "type": "etf"},  # iShares STOXX Europe 600 Technology
    {"ticker": "WITS.L", "type": "etf"},  # iShares MSCI World Information Technology
    {"ticker": "QDVG.DE", "type": "etf"},  # iShares S&P 500 Health Care
    {"ticker": "EXV4.DE", "type": "etf"},  # iShares STOXX Europe 600 Health Care
    {"ticker": "QDVH.DE", "type": "etf"},  # iShares S&P 500 Financials
    {"ticker": "EXV1.DE", "type": "etf"},  # iShares STOXX Europe 600 Banks
    {"ticker": "QDVF.DE", "type": "etf"},  # iShares S&P 500 Energy
    {"ticker": "IQQH.DE", "type": "etf"},  # iShares Global Clean Energy
    {"ticker": "2B7D.DE", "type": "etf"},  # iShares S&P 500 Consumer Staples
    {"ticker": "VAPX.L", "type": "etf"},  # Vanguard FTSE Asia Pacific ex Japan
    {"ticker": "EUNJ.DE", "type": "etf"},  # iShares MSCI Pacific ex Japan
    {"ticker": "ICGA.DE", "type": "etf"},  # iShares MSCI China, accumulating
    {"ticker": "ASHR.L", "type": "etf"},  # Xtrackers Harvest CSI 300 China A-Shares
    {"ticker": "36BZ.DE", "type": "etf"},  # iShares MSCI China A, accumulating
    {"ticker": "KWBE.DE", "type": "etf"},  # KraneShares CSI China Internet
    {"ticker": "PPFB.DE", "type": "commodity"},  # iShares Physical Gold ETC
    {"ticker": "VAGF.DE", "type": "etf"},  # Vanguard Global Aggregate Bond, EUR-hedged
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
