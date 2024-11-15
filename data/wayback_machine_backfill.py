"""
Backfill the historical top gainers and losers data using
scrape.py and wayback archive.

This is imperfect as wayback doesn't always have 
"""

import datetime as dt
import pandas as pd
from waybackpy import WaybackMachineCDXServerAPI
import concurrent.futures
import time
import random

import quant_finance.data.top_movers.scrape as sc


def get_snapshots_with_retry(base_url, start_date, end_date, max_retries=3, retry_delay=5):
    """
    Attempts to get snapshots from Wayback Machine with retry logic
    
    Args:
        base_url (str): URL to get snapshots for
        start_date (datetime): Start date for snapshots
        end_date (datetime): End date for snapshots
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Base delay between retries in seconds
        
    Returns:
        dict: Dictionary of date: url pairs for snapshots
        None: If no snapshots found or max retries exceeded
    """
    for attempt in range(max_retries):
        try:
            user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")
            wbm = WaybackMachineCDXServerAPI(base_url, user_agent=user_agent, start_timestamp=start_str, end_timestamp=end_str)
            
            # Get all snapshots with delay between requests
            all_snapshots = {}
            for sn in wbm.snapshots():
                url = sn.archive_url
                all_snapshots[url.split('/')[4][:8]] = url
                # Add random delay between requests
                time.sleep(random.uniform(1, 3))
            
            if not all_snapshots:
                print(f"No snapshots found between {start_date} and {end_date}")
                return None
                
            return all_snapshots
            
        except ConnectionError as e:
            if attempt < max_retries - 1:
                # wait_time = retry_delay * (attempt + 1)
                print(f"Connection error occurred. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                print("Max retries exceeded. Please try again later.")
                raise e

def backfill_archived_data(start_date, end_date, base_url=sc.BASE_URL, scrape_func=sc.scrape_data):
    """Main function to backfill data from Wayback Machine"""
    
    # Get snapshots with retry logic
    all_snapshots = get_snapshots_with_retry(base_url, start_date, end_date)
    if all_snapshots is None:
        return None

    print("Got all the snapshots...scraping now")

    frames = []

    # Helper function to handle scraping for each snapshot
    def scrape_snapshot(date_url_tuple):
        date, url = date_url_tuple
        try:
            # Convert date string to datetime object
            run_date = dt.datetime.strptime(date, '%Y%m%d')
            return scrape_func(url=url, run_date=run_date)
        except Exception as e:
            print(f"Error scraping {date}: {e}")
            return None

    # Use multi-threading to get data
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as exec:
        # Create list of futures
        futures = [exec.submit(scrape_snapshot, (date, url)) for date, url in all_snapshots.items()]
        # Get results as they complete
        frames = [f.result() for f in concurrent.futures.as_completed(futures) if f.result() is not None]
    
    if not frames:
        return None
        
    return pd.concat(frames, axis=0)


