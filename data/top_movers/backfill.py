"""
Backfill the historical top gainers and losers data using
scrape.py and wayback archive.

This is imperfect as wayback doesn't always have 
"""

import datetime as dt
from waybackpy import WaybackMachineCDXServerAPI

import quant_finance.data.top_movers.scrape as sc


def backfill_top_movers(start_date, end_date):

    # generate weekdays form start to end date

    # for each weekdate
        # get the 'nearest' url
        # check if the 'nearest' url is of that day itself. If not, go to the next date, i.e. continue
        # using sc.scrape_data with the url and date to get that day's data.
        # Store data so it can be combined later.


