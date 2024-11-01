"""
Backfill the historical top gainers and losers data using
scrape.py and wayback archive.

This is imperfect as wayback doesn't always have 
"""

import datetime as dt
import pandas as pd
from waybackpy import WaybackMachineCDXServerAPI

import quant_finance.data.top_movers.scrape as sc


def backfill_top_movers(start_date, end_date, base_url : str = "https://thestockmarketwatch.com/markets/pre-market/today.aspx"):
    """
    
    """

    # initializations
    wbm = WaybackMachineCDXServerAPI(base_url, user_agent="Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0")
    cur_date = start_date
    frames = []

    while cur_date <= end_date:
        print(cur_date)
        # Skip to next iteration if cur_date is a weekend
        if cur_date.weekday() >= 5:
            cur_date = cur_date + dt.timedelta( days=1 )        # todo: If Saturday, you can just add 2. Edge case is when we start with Sunday.
            continue

        # get the nearest url
        try:
            nearest_url = wbm.near(year=cur_date.year, month=cur_date.month, day=cur_date.day)
        except:
            cur_date = cur_date + dt.timedelta( days=1 )
            continue

        # check if the url is of that day itself
        if nearest_url.datetime_timestamp.date() != cur_date.date():
            cur_date = cur_date + dt.timedelta( days=1 )
            continue

        # get data
        try:
            cur_date_df = sc.scrape_data(url=nearest_url.archive_url, run_date=cur_date)
        except:
            cur_date = cur_date + dt.timedelta( days=1 )
            continue

        # Store data so it can be combined later.
        frames.append(cur_date_df)

        cur_date = cur_date + dt.timedelta( days=1 )

    if len(frames) == 0:
        print("No relevant data found.")
        return pd.DataFrame()

    return pd.concat(frames, axis=0)        


