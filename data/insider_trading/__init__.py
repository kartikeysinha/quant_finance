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
