"""
Author: @kartikeysinha

Get the data directly from the SEC's website.
https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets#:~:text=Section%2016%20of%20the%20Securities,transactions%20in%20the%20company's%20stock.

Each quarter has a separate zip file. Each zip file contains multiple csvs:

- DERIV_HOLDING.tsv
- DERIV_TRANS.tsv
- FOOTNOTES.tsv
- FORM_345_metadata.json
- FORM_345_readme.htm
- NONDERIV_HOLDING.tsv
- NONDERIV_TRANS.tsv
- OWNER_SIGNATURE.tsv
- REPORTINGOWNER.tsv
- SUBMISSION.tsv

"""


import os
import requests
import zipfile

# Global variables
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_bin")
BASE_URL = 'https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/'

# step 1: Download the zip file for the quarter and unzip it.
def get_insider_trading_data_quarter(year: int, quarter: int, data_dir: str = DATA_DIR) -> str:
    '''
    Download the zip file for the quarter & year and unzip it. Store the files in data_dir.
    
    Args:
        year (int): The year of the quarter.
        quarter (int): The quarter number.
        data_dir (str): The directory to store the downloaded files.
    
    Returns:
        The path to the directory where the files were stored.
    '''

    assert year >= 2000, "Year must be greater than or equal to 2000"
    assert quarter in [1, 2, 3, 4], "Quarter must be 1, 2, 3, or 4"

    folder_name = f"{year}q{quarter}_form345"
    zip_file_name = folder_name + ".zip"
    zip_file_path = os.path.join(data_dir, zip_file_name)
    url = f"{BASE_URL}{zip_file_name}"

    # Get the zip file
    retval = requests.get(url=url)
    
    # Store the zip file in data_dir
    with open(zip_file_path, "wb") as file:
        file.write(retval.content)
    
    # create a folder to store the extracted zip files
    os.makedirs(os.path.join(data_dir, folder_name))

    # Extract the zip files
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(data_dir, folder_name))
        print(f"Successfully extracted files for {folder_name}")
    except zipfile.BadZipFile as e:
        print(f"Error: The file {zip_file_path} is not a zip file or is corrupted.")

        # Remove the folder made for storing unzipped files
        os.rmdir(os.path.join(data_dir, folder_name))
        
        raise e
    
    # Remove the zip file after extraction
    os.remove(zip_file_path)
    
    return os.path.join(data_dir, folder_name)


# Run example
if __name__ == "__main__":
    get_insider_trading_data_quarter(2024, 3)
