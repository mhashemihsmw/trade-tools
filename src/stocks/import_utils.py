import pandas as pd
import numpy as np
import requests
import os



def import_prices_twelvedata(symbol: str, interval: str = "1h", outputsize: int = 1000) -> pd.DataFrame:
    """Import OHLCV data from TwelveData API.
        symbol: Trading pair symbol, e.g., "HBAR/USD"
        interval: Time interval between data points, e.g., "1h", "1d
        outputsize: Number of data points to retrieve
    """
    twelvedata_url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "apikey": os.getenv("twelvedata_api_key"),
        "outputsize": outputsize
    }
    r = requests.get(twelvedata_url, params=params)    
    data = r.json()
    if "values" not in data:
        raise ValueError(f"Error fetching data for {symbol}: {data.get('message', 'Unknown error')}")
    
    df = pd.DataFrame(data["values"])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df = df.astype(float)   
    return df.sort_index()

