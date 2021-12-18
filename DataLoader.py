# The 'user interface' for our data loading program.

# This script will - when completed - grab data from the API,
# then pkg it up into a .zip. We then manually upload it to AWS S3 bucket (s3://cis545project/data/).
# which will then be the input to the Jupyter notebooks in this repository.

import shutil
import random
import os
import time
import numpy

from importlib import reload
import alpha_utils as au
reload(au)

stock_output_path = "stock_data"
tech_output_path = "technical_data"
api_key = au.get_alpha_key('secrets.yml')

############### GET TICKER SYMBOLS ##############################################

#get all active listings based as of today
all_active_listings = au.get_alpha_listings(api_key) 
#only need NYSE and NASDAQ...
all_active_listings = all_active_listings[all_active_listings.exchange.isin(['NYSE', 'NASDAQ'])]
symbols = all_active_listings['symbol'].unique()

random.seed(42)
rand_sample = random.sample(range(len(symbols)), k = 2500)
symbols = symbols[rand_sample]

numpy.savetxt("symbols.csv", symbols, delimiter=",", fmt = "%s")

print("Total number of symbols:", len(symbols))

#for testing
# symbols = ['IBM', 'MSFT', 'FB', 'AAPL', 'QQQ', 'AAP', 'GSPY', 'GUNR']

BATCH_SIZE = 100

last = 0
for i in range((len(symbols) // BATCH_SIZE) + 1):
    left = last
    right = left + BATCH_SIZE
    print("Getting stock data for symbols {} to {}".format(left, right - 1))
    symbols_subset = symbols[left:right]

    stock_data = au.get_alpha_stock_data(
        function = 'TIME_SERIES_DAILY_ADJUSTED',
        symbols = symbols_subset, 
        api_key = api_key,
        output_size = 'full',
        max_threads = 7
    )

    try:

        au.write_alpha_results(
            results = stock_data, 
            symbols = symbols_subset,
            dest_path = stock_output_path,
            columns = ['symbol', 'timestamp', 'adjusted_close', 'volume']
        )

        zip_file_name = stock_output_path + '_' + str(left) + '_' + str(right - 1)
        
        shutil.make_archive(
            base_name = zip_file_name, 
            format = 'zip', 
            root_dir = stock_output_path
        )

        #num files
        files = [f for f in os.listdir(stock_output_path) if not f.startswith('.')]
        print(stock_output_path + "/", "contains", len(files), "files.")

        #size of .zip output
        zip_size = os.path.getsize(zip_file_name + '.zip')
        print("Zipped data size:", round(zip_size / (1024 * 1024), 2), "MB")

        #delete csvs
        _ = os.system('rm ' + stock_output_path + '/*.csv')

    except:
        print("Error writing stock data to file.")
        continue
    time.sleep(15)
    last = right


##############################################################################

BATCH_SIZE = 30

last = 0
for i in range((len(symbols) // BATCH_SIZE) + 1):
    left = last
    right = left + BATCH_SIZE
    print("Getting tech data for symbols {} to {}".format(left, right - 1))
    symbols_subset = symbols[left:right]
    technical_data = au.get_alpha_technical_data(
        functions = [
            'EMA', 'MACD', 'STOCH', 'RSI', 'BBANDS'
        ],
        symbols = symbols_subset,
        api_key = api_key,
        interval = 'daily',
        time_period = 60, 
        series_type = 'close', 
        max_threads = 7
    )

    try:

        au.write_alpha_results(
            results = technical_data, 
            symbols = symbols_subset,
            dest_path = tech_output_path
        )

        zip_file_name = tech_output_path + '_' + str(left) + '_' + str(right - 1)
        
        shutil.make_archive(
            base_name = zip_file_name, 
            format = 'zip', 
            root_dir = tech_output_path
        )

        #num files
        files = [f for f in os.listdir(tech_output_path) if not f.startswith('.')]
        print(tech_output_path + "/", "contains", len(files), "files.")

        #size of .zip output
        zip_size = os.path.getsize(zip_file_name + '.zip')
        print("Zipped data size:", round(zip_size / (1024 * 1024), 2), "MB")

        #delete csvs
        _ = os.system('rm ' + tech_output_path + '/*.csv')

    except:
        print("Error writing tech data to file.")
        continue

    time.sleep(60)
    last = right
