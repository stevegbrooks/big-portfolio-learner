# The 'user interface' for our data loading program.

# This script will - when completed - grab data from the API,
# then pkg it up into a .zip. We then manually upload it to AWS S3 bucket (s3://cis545project/data/).
# which will then be the input to the Jupyter notebooks in this repository.

import shutil
import random
import os

from importlib import reload
import alpha_utils as au
reload(au)

stock_output_path = "stock_data"
tech_output_path = "technical_data"
api_key = au.get_alpha_key('secrets.yml')

############### GET TICKER SYMBOLS ###############

#get all active listings based as of today
all_active_listings = au.get_alpha_listings(api_key) 
#only need NYSE and NASDAQ...
all_active_listings = all_active_listings[all_active_listings.exchange.isin(['NYSE', 'NASDAQ'])]
symbols = all_active_listings['symbol'].unique()

#for testing
#symbols = ['IBM', 'MSFT', 'FB', 'AAPL', 'QQQ', 'AAP', 'GSPY', 'GUNR']
rand_sample = random.sample(range(len(symbols)), k = 5)
symbols = symbols[rand_sample]

############### GET STOCK DATA ###################

#returns a generator, so the calls don't happen until 'write_alpha_results' is called
stock_data = au.get_alpha_stock_data(
    function = 'TIME_SERIES_DAILY_ADJUSTED',
    symbols = symbols, 
    api_key = api_key,
    output_size = 'full',
    max_threads = 7
)

############### WRITE STOCK DATA #################
technical_data = au.get_alpha_technical_data(
    functions = [
        'SMA', 'EMA', 'MACD', 'STOCH', 'RSI', 'BBANDS'
    ],
    symbols = symbols,
    api_key = api_key,
    interval = 'daily',
    time_period = 60, 
    series_type = 'close', 
    max_threads = 7
)

au.write_alpha_results(
    results = stock_data, 
    symbols = symbols,
    dest_path = stock_output_path
)

au.write_alpha_results(
    results = technical_data, 
    symbols = symbols,
    dest_path = tech_output_path
)

shutil.make_archive(
    base_name = stock_output_path, 
    format = 'zip', 
    root_dir = stock_output_path
)

shutil.make_archive(
    base_name = tech_output_path, 
    format = 'zip', 
    root_dir = tech_output_path
)

############### PRINT RESULTS ###################

#num files
files = [f for f in os.listdir(stock_output_path) if not f.startswith('.')]
print(stock_output_path + "/", "contains", len(files), "files.")

#size of .zip output
zip_size = os.path.getsize(stock_output_path + '.zip')
print("Zipped data size:", round(zip_size / (1024 * 1024), 2), "MB")

#num files
files = [f for f in os.listdir(tech_output_path) if not f.startswith('.')]
print(tech_output_path + "/", "contains", len(files), "files.")

#size of .zip output
zip_size = os.path.getsize(tech_output_path + '.zip')
print("Zipped data size:", round(zip_size / (1024 * 1024), 2), "MB")

