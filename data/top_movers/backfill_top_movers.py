"""
backfill top movers data
"""

import pandas as pd
import datetime as dt

from quant_finance import *
import quant_finance.data.wayback_machine_backfill as wm_bfill
import quant_finance.data.top_movers.scrape as sc
import quant_finance.data.utilities as data_utils

hist_df = wm_bfill.backfill_archived_data(
    start_date=dt.datetime(2024, 9, 1), 
    end_date=dt.datetime(2024, 9, 30), 
    base_url=sc.BASE_URL, 
    scrape_func=sc.scrape_data
)

print(hist_df)

_ = data_utils.update_stored_data(
    new_data=hist_df, 
    file_path=DATA_DIR + 'top_movers/archive/', 
    file_name='top_movers.csv', 
    cols_to_compare=['date', 'type', 'Symb']
)

