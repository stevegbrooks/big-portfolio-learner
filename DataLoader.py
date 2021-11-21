# The 'user interface' for our data loading program.

# This script will - when completed - grab data from the API,
# then pkg it up into a .zip and upload it to Google Cloud Storage or AWS S3 bucket
# which will then be the input to the Jupyter notebooks in this repository.

from importlib import reload
import utils
reload(utils)

api_key = utils.get_alpha_key('secrets.yml')

all_active_listings = utils.get_alpha_listings(api_key)
symbols = all_active_listings['symbol']

stock_data = utils.yield_alpha_stock_data(
    function = 'TIME_SERIES_DAILY_ADJUSTED',
    symbols = symbols, 
    api_key = api_key,
    data_type = 'csv',
    output_size = 'compact'
)

stock_data = utils.join_alpha_results(stock_data, symbols)

stock_data
