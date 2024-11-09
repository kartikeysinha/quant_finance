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
import datetime as dt

from quant_finance import *
import quant_finance.data.utilities as data_utils


def get_cnadles_from_yf(tickers, store_data=False):
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
    df.columns.name = None

    if store_data:
        data_utils.update_stored_data(
            new_data=df, file_path=DATA_DIR+'returns/archive/', file_name='candles', cols_to_compare=['Ticker', 'Date', 'Adj. Close']
        )
    
    return df


def get_returns(tickers : str | list[str], start_date : str | dt.datetime, end_date : str | dt.datetime):
    """
    Calculated based on the Adj. Close prices for the ticker.
    """

    # read returns
    ...

    # check if all tickers exist between start_date and end_date in dataframe
    ...

    # If any don't exist, try installing them from yahoo finances
    ...

    # return the tickers within the start and end date
    ...

