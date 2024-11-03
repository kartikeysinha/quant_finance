"""
Backfill the historical top gainers and losers data using
scrape.py and wayback archive.

This is imperfect as wayback doesn't always have 
"""

import datetime as dt
import pandas as pd
from waybackpy import WaybackMachineCDXServerAPI
import concurrent.futures

import quant_finance.data.top_movers.scrape as sc


def backfill_archived_data(
        start_date : dt.datetime, 
        end_date : dt.datetime, 
        base_url : str = "https://thestockmarketwatch.com/markets/pre-market/today.aspx",
        scrape_func = sc.scrape_data
    ) -> pd.DataFrame:
    """
    Backfills historical top gainers and losers data by scraping archived versions of a webpage using the Wayback Machine.

    Args:
        start_date: The starting date to begin backfilling data from.
        end_date: The ending date to stop backfilling data at.
        base_url: The URL to scrape historical versions from.
                 Defaults to thestockmarketwatch.com pre-market page.
        scrape_func: Function used to scrape data from the webpage.
                 Defaults to function used to scrape data from default url.

    Returns:
        pandas.DataFrame: A concatenated DataFrame containing the historical top movers data.
        If no data is found, returns an empty DataFrame.
        
    Notes:
        - Data quality depends on Wayback Machine's archive coverage
        - The resulting DataFrame structure is determined by the scrape_func(...) function
    """

    # initializations
    wbm = WaybackMachineCDXServerAPI(base_url, user_agent="Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0")
    weekdays = pd.bdate_range(start_date, end_date)
    frames = []

    def fetch_data_for_date(cur_date):
        """
        helper function to fetch and process data for one date from the wayback machine. 
        """
        cur_date = cur_date.to_pydatetime()
        try:
             # get the nearest url
            nearest_url = wbm.near(year=cur_date.year, month=cur_date.month, day=cur_date.day)

            # check if the url is of that day itself
            if nearest_url.datetime_timestamp.date() != cur_date.date():
                return None
            
            return scrape_func(url=nearest_url.archive_url, run_date=cur_date)
        except Exception as e:
            print(f"Error processing {cur_date}: {str(e)}")
            return None

    # Use multi-threading to get data
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
        results = exec.map(fetch_data_for_date, weekdays)
        frames = [df for df in results if df is not None]
    
    if len(frames) == 0:
        print("No relevant data found.")
        return pd.DataFrame()

    return pd.concat(frames, axis=0)        


