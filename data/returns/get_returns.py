"""
Function to get returns.

Process (for now):
- read in returns pickle file
- check if desired returns (ticker, data range) are already stored.
- If stored, return.
- If not stored, collect data and update pickle file.s
- return the newly collected data.

Collection of data online entails:
- collecting the prices
- calculating returns

Right now, we only focus on daily data.
"""

import pandas as pd
import yfinance as yf

def get_returns_from_yf(tickers, store_data=False):
    """
    ...
    """

    # get the returns from Yahoo Finance
    try:
        df = yf.download(tickers=tickers, period='max').stack(future_stack=True)
    except:
        print("Getting data from yfinance failed.")
        raise

    # Format the dataframe as desired
    df.index = df.index.swaplevel()     # ensure the ticker is the first index
    df.sort_index(inplace=True)

    if store_data:
        pass
    
    return df




