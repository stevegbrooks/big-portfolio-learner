# The 'user interface' for our data loading program.

# This script will - when completed - grab data from the API,
# then pkg it up into a .zip. We then manually upload it to AWS S3 bucket (s3://cis545project/data/).
# which will then be the input to the Jupyter notebooks in this repository.

from importlib import reload
import alpha_utils as au
reload(au)

api_key = au.get_alpha_key('secrets.yml')

#get all active listings based as of today
# all_active_listings = au.get_alpha_listings(api_key) 
# #only need NYSE and NASDAQ...
# all_active_listings = all_active_listings[all_active_listings.exchange.isin(['NYSE', 'NASDAQ'])]
# symbols = all_active_listings['symbol'].unique()

#for testing
symbols = ['IBM', 'MSFT', 'FB', 'AAPL', 'QQQ', 'AAP', 'ATHM', 'VIPS']

#returns a generator, so the calls don't happen until 'write_alpha_results' is called
stock_data = au.get_alpha_stock_data(
    function = 'TIME_SERIES_DAILY_ADJUSTED',
    symbols = symbols, 
    api_key = api_key,
    output_size = 'full',
    max_threads = 7
)

sma_data = au.get_alpha_technical_data(
    function = 'SMA',
    symbols = symbols, 
    interval = 'daily',
    time_period = 60, 
    series_type = 'close', 
    api_key = api_key,
    max_threads = 7
)


au.write_alpha_results(
    results = stock_data, 
    symbols = symbols,
    dest_path = "stock_data/"
)

au.write_alpha_results(
    results = sma_data, 
    symbols = symbols,
    dest_path = "technical_data/"
)

