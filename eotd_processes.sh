#!/bin/bash

# End-of-trading day scripts
date >> /Users/kartikeysinha/Desktop/ktk_dev.nosync/scheduled_tasks/crontabs.log
echo "Running top movers scraping" >> /Users/kartikeysinha/Desktop/ktk_dev.nosync/scheduled_tasks/crontabs.log
/opt/homebrew/bin/python3.10 /Users/kartikeysinha/Desktop/ktk_dev.nosync/github/quant_finance/data/top_movers/scrape.py
