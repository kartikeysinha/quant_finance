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
import numpy as np
import yfinance as yf
import datetime as dt

from quant_finance import *
import quant_finance.data.utilities as data_utils


def get_candles_from_yf(tickers : list[str] , store_data : bool = False) -> pd.DataFrame:
    """
    Gets candlestick data from Yahoo Finance for given tickers.

    Args:
        tickers (list[str]): List of ticker symbols to get data for
        store_data (bool, optional): Whether to store the data locally. Defaults to False.

    Returns:
        pd.DataFrame: Candlestick data in long format with MultiIndex (Ticker, Date)
    
    If store_data is True:
        - Saves candles data to DATA_DIR+'returns/archive/candles.parquet'
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
            data=df, file_path=DATA_DIR+'returns/archive/', file_name='candles', cols_to_compare=['Ticker', 'Date', 'Adj Close']
        )
        
    return df


def calculate_returns(prices : pd.Series | pd.DataFrame):
    """
    Calculates both normal and log returns from price data.

    Args:
        prices (pd.Series | pd.DataFrame): Price data in either Series or DataFrame format

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Normal returns and log returns in long format with MultiIndex
    """

    if type(prices) == pd.Series:
        prices = prices.to_frame()
    
    assert len(prices.columns) == 1, "1D data expected."

    if len(prices.index.names) == 1:
        # Wide format -> long format
        prices = prices.stack(future_stack=True)
    
    returns = prices.groupby('Ticker').pct_change(fill_method=None)     # normal returns
    returns.columns = ['returns']
    log_returns = np.log(prices / prices.groupby('Ticker').shift(1))    # log returns
    log_returns.columns = ['log_returns']

    return returns, log_returns


def get_returns_from_yf(tickers : list[str], store_data : bool, col_to_use : str = "Adj Close") -> pd.DataFrame:
    """
    Gets returns data from Yahoo Finance for the given tickers. Returns both normal and log returns.

    Args:
        tickers (list[str]): List of ticker symbols to get returns for
        store_data (bool): Whether to store the data locally
        col_to_use (str, optional): Column to calculate returns from. Defaults to "Adj Close".

    Returns:
        pd.DataFrame: DataFrame containing normal and log returns
    
    If store_data is True:
        - Saves returns data to DATA_DIR+'returns/archive/returns.parquet'
        - Saves log returns data to DATA_DIR+'returns/archive/log_returns.parquet'
    """

    # get candles from yfinance
    candles = get_candles_from_yf(tickers=tickers, store_data=store_data)

    # Calculate returns using the passed in argument (defualt: Adjusted close prices)
    returns, log_returns = calculate_returns(candles[[col_to_use]])

    if store_data:
        data_utils.update_stored_data(
            data=returns, file_path=DATA_DIR+'returns/archive/', file_name='returns', cols_to_compare=['Ticker', 'Date', 'returns']
        )
        data_utils.update_stored_data(
            data=log_returns, file_path=DATA_DIR+'returns/archive/', file_name='log_returns', cols_to_compare=['Ticker', 'Date', 'log_returns']
        )
    
    return pd.concat([returns, log_returns], axis=1, ignore_index=False)


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

