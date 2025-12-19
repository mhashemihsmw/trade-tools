import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timedelta

def import_crypto_prices_coingecko(symbol: str = "hedera-hashgraph", vs_currency: str = "usd", days: int = 365, with_volume: bool = True) -> pd.DataFrame:
    """Import crypto OHLCV data from CoinGecko API.
        symbol: CoinGecko coin id, e.g., "hedera"
        vs_currency: Quote currency, e.g., "usd"
        days: Number of days to retrieve
    """
    
    ohlc_url = f"https://api.coingecko.com/api/v3/coins/{symbol}/ohlc"
    ohlc_params = {
        "vs_currency": vs_currency,
        "days": days
    }

    ohlc_resp = requests.get(ohlc_url, params=ohlc_params).json()
    ohlc_df = pd.DataFrame(
        ohlc_resp,
        columns=["timestamp", "open", "high", "low", "close"]
    )
    print(ohlc_df)

    ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], unit="ms")
    if not with_volume:
        return ohlc_df.sort_values("timestamp")
    # Volume data (from market chart)
    volume_url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart"
    volume_params = {
        "vs_currency": vs_currency,
        "days": days
    }

    volume_resp = requests.get(volume_url, params=volume_params).json()
    volume_df = pd.DataFrame(
        volume_resp["total_volumes"],
        columns=["timestamp", "volume"]
    )
    volume_df["timestamp"] = pd.to_datetime(volume_df["timestamp"], unit="ms")
    
    # Merge OHLC + volume
    df = pd.merge_asof(
        ohlc_df.sort_values("timestamp"),
        volume_df.sort_values("timestamp"),
        on="timestamp"
    )
    return df


def import_crypto_prices_cryptocompare(symbol: str = "HBAR", interval: str = "histoday", aggregate: int = 1, market: str = "USDT", limit: int = 365) -> pd.DataFrame:
    """Import crypto OHLCV data from CryptoCompare API.
        symbol: Trading pair symbol, e.g., "HBAR"
        aggregate: Candle size multiplier
        market: Quote currency, e.g., "USD" or "USDT"
        limit: Number of candles to retrieve
    """

    url = f"https://min-api.cryptocompare.com/data/v2/{interval}"
    params = {
        "fsym": symbol,
        "tsym": market,
        "limit": limit,
        "aggregate": aggregate
    }
    r = requests.get(url, params=params)
    data = r.json().get("Data", {}).get("Data", [])
    if not data:
        raise ValueError(f"Error fetching data for {symbol}: {r.json().get('Message', 'Unknown error')}")

    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"], unit="s")

    df = df.rename(columns={
        "time": "timestamp",
        "volumefrom": "volume_base",
        "volumeto": "volume_quote"
    })
    return df


def import_crypto_prices_binance(symbol="HBARUSDT", interval="1h", limit=10, current = True) -> pd.DataFrame:
    url = "https://api.binance.com/api/v3/klines"

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=limit)
    all_data = []
    limit_current = limit
    length = 1000
    if not current:
        limit_current = limit*1000
        length = 1000
        start_time = end_time - timedelta(days=5*365)
    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * length),
            "limit": limit_current
        }

        resp = requests.get(url, params=params).json()
        if not resp:
            break

        all_data.extend(resp)

        # move start_time to last candle close
        last_open_time = resp[-1][0]
        start_time = datetime.fromtimestamp(last_open_time / 1000) + timedelta(hours=1)

    df = pd.DataFrame(all_data, columns=[
        "time_stamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_base_volume", "taker_quote_volume", "ignore"
    ])

    df["time_stamp"] = pd.to_datetime(df["time_stamp"], unit="ms")

    return df
