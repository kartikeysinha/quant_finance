"""
Scrape the top pre-market gainers and losers from Stock Market Watch.
"""

import os
import pandas as pd
import datetime as dt
import requests
from bs4 import BeautifulSoup
from inputimeout import inputimeout


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
    df['%Chg'] = df['%Chg'].str.rstrip('%').astype('float64')
    df['Volume'] = df['Volume'].str.rstrip('k').astype('float64') * 1000

    return df


def is_df_subset(new_df, df):
    """
    check if new_df is a subset of df
    """
    merged_df = new_df.merge(df, how='left', indicator=True)
    return merged_df['_merge'].eq('both').all()


def find_unique_data(new_df, old_df):
    """
    Merge 2 dataframes, ensuring all the entries are unique. 
    If data exists in both dataframes, delete data from old
    """
    df = old_df.merge(new_df, how='left', indicator=True)
    return df[df['_merge'] == 'left_only'].drop(columns=['_merge'])


def update_stored_data(
        new_data :pd.DataFrame,  
        old_data: pd.DataFrame = None, 
        file_path :str = None,
        file_name :str = None,
        cols_to_compare :list[str] = None
    ) -> pd.DataFrame:
    """
    Update old data. Run some quality checks before.
    """
    
    assert (old_data is not None) or (file_path is not None), "please provide old_data or file_path"

    if old_data is None:
        old_data = pd.read_csv(file_path + file_name)
    elif isinstance(old_data.index, pd.RangeIndex):
        old_data = old_data.reset_index(drop=True)
    else:
        old_data = old_data.reset_index(drop=False)

    if file_path:
        fp = os.path.join(file_path, file_name.split('.')[0] + '_temp.csv')
        print(file_path, fp)
        old_data.to_csv( fp )      # store old data in temp file 

    # reset new data index if needed
    if isinstance(new_data.index, pd.RangeIndex):
        print('here')
        print(isinstance(new_data.index, pd.RangeIndex))
        print(new_data.index.name)
        new_data = new_data.reset_index(drop=True)
    else:
        new_data = new_data.reset_index(drop=False)

    if cols_to_compare is None or len(cols_to_compare) == 0:
        cols_to_compare = new_data.columns
    
    # check if new_data is a subset of old_data
    if is_df_subset(new_data[cols_to_compare], old_data[cols_to_compare]):
        # Ask user if they want to overwrite data
        prompt = f"The new_df has duplicates based on cols_to_compare. Do you want to overwrite the data? Y or N?\n"
        try:
            usr_res = inputimeout(prompt, 5)
        except:
            usr_res = 'n'       # default to 'no'

        if usr_res.lower().startswith('n'):
            print("Aborting data process based on user request.")
            return

        # override
        tmp_df = find_unique_data(new_data[cols_to_compare], old_data[cols_to_compare])
        if len(cols_to_compare) > 0 and len(cols_to_compare) < len(new_data.columns):
            retval = tmp_df.merge(old_data, how='left')
            old_data = retval.loc[~retval.duplicated()].reset_index(drop=True)
        else:
            old_data = tmp_df
    
    comb_df = pd.concat([old_data, new_data], axis=0).reset_index(drop=True)

    if len(cols_to_compare) > 0 and len(cols_to_compare) < len(new_data.columns):
        comb_df.set_index(cols_to_compare, inplace=True)

    if file_path:
        comb_df.to_csv(file_path + file_name)

    return comb_df
    

def run_data_process(run_date : dt.date, file_loc : str, file_name : str) -> None:
    """
    Process and update top movers data by combining new scraped data with existing historical data.

    Args:
        run_date (dt.date): The date for which to scrape and process data
        file_loc (str): Directory path where the data files are stored
        file_name (str): Base name of the CSV file (without extension)

    Returns:
        None: The function saves the updated data to a CSV file but doesn't return anything

    Notes:
        - Creates a temporary backup file before processing
        - Checks for existing data on the run_date and prompts for confirmation before overwriting
        - Combines new scraped data with historical data and saves to CSV
    
    Example:
        >>> run_date = dt.datetime.now().date()
        >>> file_loc = '/path/to/data/directory/'
        >>> run_data_process(run_date, file_loc, 'top_movers')
    """

    # get new data
    url = 'https://thestockmarketwatch.com/markets/pre-market/today.aspx'
    new_df = scrape_data(url, run_date=run_date)

    _ = update_stored_data(new_data=new_df, file_path=file_loc, file_name=file_name + '.csv', cols_to_compare=['date', 'type'])

    return


if __name__ == '__main__':
    run_date = dt.datetime.now()

    file_loc = '/Users/kartikeysinha/Desktop/ktk_dev.nosync/github/quant_finance/data/top_movers/archive/'       # TODO: store part of path as environment variable.
    run_data_process(run_date=run_date.date(), file_loc=file_loc, file_name='top_movers')

    # # testing
    # cur_df = pd.read_csv(file_loc + 'top_movers.csv', index_col=0)

    # new_df = cur_df.loc['2024-10-31']
    # old_df = cur_df.loc['2024-10-31']

    # ret = update_stored_data(new_data=new_df, old_data=cur_df, cols_to_compare=['date', 'type'])
    # print(ret)
    # print(ret.equals(cur_df.set_index('type', append=True)))
