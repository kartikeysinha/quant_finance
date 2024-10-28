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


if __name__ == '__main__':
    run_date = dt.datetime.now()

    # scrape today's data
    url = 'https://thestockmarketwatch.com/markets/pre-market/today.aspx'
    df = scrape_data(url, run_date=run_date.date())

    # for now, store the file
    file_name = f"top_movers_{run_date.strftime('%Y%m%d')}.csv"
    df.to_csv(f"./archive/{file_name}")
