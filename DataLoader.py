# The 'user interface' for our data loading program.

# This script will - when completed - grab data from the API,
# then pkg it up into a .zip and upload it to Google Cloud Storage or AWS S3 bucket
# which will then be the input to the Jupyter notebooks in this repository.

from importlib import reload
import utils
reload(utils)

base_url = 'https://www.alphavantage.co/query?'
function = 'TIME_SERIES_DAILY_ADJUSTED'
symbols = ['IBM', 'MSFT']
api_key = utils.get_alpha_key('secrets.yml')

stock_data = utils.yield_alpha_stock_data(base_url, function, symbols, api_key)

stock_data_df = utils.alpha_json_to_dataframe(stock_data)

stock_data_df

utils.yield_alpha_stock_data()