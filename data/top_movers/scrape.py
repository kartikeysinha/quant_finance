"""
Scrape the top pre-market gainers and losers from Stock Market Watch.
"""

import pandas as pd
import datetime as dt
import requests
from bs4 import BeautifulSoup

def scrape_data(url : str, run_date=dt.datetime.now().date()) -> pd.DataFrame:
    """
    Scrapes pre-market gainers and losers data from a Stock Market Watch webpage.

    Args:
        url (str): The URL of the Stock Market Watch page to scrape.
        run_date (datetime.date, optional): The date of the scraping run. 
            Defaults to current date.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped data with the following:
            - Multi-index: (date, type) where type is 'G' for gainers or 'L' for losers
            - Columns: Stock information including ticker, price, change, etc. 
              (as provided by the website)

    Example:
        >>> url = 'https://thestockmarketwatch.com/markets/pre-market/today.aspx'
        >>> df = scrape_data(url)
    """

    #  Define helper function
    def _extract_data_from_html(html_code):
        data = []
        headers = [header.text for header in html_code.find_all("th")]
        for row in html_code.find_all("tr")[1:]:
            cells = row.find_all("td")
            data.append([cell.text for cell in cells])
        
        return pd.DataFrame(data, columns=headers)

    # Get the data from the specified url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
    }
    response = requests.get(url, headers=headers)

    # Initialize the scraper
    soup = BeautifulSoup(response.content, "html.parser")

    # get gainers and losers
    frames = []
    for rel_id in ("tdGainersDesktop", "tdLosersDesktop"):
        # Extract dataframe from html
        table_headers = soup.find("div", id=rel_id)
        table_html = table_headers.find("table", class_="tbldata")
        df = _extract_data_from_html(table_html)

        # Add relevant information and store
        df['type'] = rel_id[2]  # G: Gainer; L: Losers
        frames.append(df)

    df = pd.concat(frames, axis=0)
    df['date'] = run_date
    df.set_index(['date', 'type'], inplace=True)

    return df


def run_data_process(run_date : dt.date, file_loc : str, file_name : str) -> None:
    """
    ...
    """

    # get new data
    url = 'https://thestockmarketwatch.com/markets/pre-market/today.aspx'
    new_df = scrape_data(url, run_date=run_date)

    # read in existing data
    df = pd.read_csv(file_loc + file_name + '.csv')
    df.to_csv(file_loc + file_name + '_tmp.csv')        # storing tmp file as we'll overwrite
    df.set_index(['date', 'type'], inplace=True)

    # compare new data vs existing data to ensure new data is being added.
    # For now, we do one run per day, so we shouldn't have overlapping dates.
    # We can identify duplicates using the `date` column.
    if df.index.__contains__(run_date.strftime('%Y-%m-%d')):
        usr_res = input(f"Are you sure you want to overwrite the data for {run_date.strftime('%Y-%m-%d')}? Y or N?")

        if usr_res.lower().startswith('n'):
            print("Aborting data process based on user request.")
            return
    
        df.drop(run_date.strftime('%Y-%m-%d'), axis='index', inplace=True)
    
    # combine new data with old
    full_df = pd.concat([df, new_df], axis=0)

    # store csv
    full_df.to_csv(file_loc + file_name + '.csv')

    return



if __name__ == '__main__':
    run_date = dt.datetime.now()

    file_loc = '/Users/kartikeysinha/Desktop/ktk_dev.nosync/github/quant_finance/data/top_movers/archive/'       # TODO: store part of path as environment variable.
    run_data_process(run_date=run_date.date(), file_loc=file_loc, file_name='top_movers')
