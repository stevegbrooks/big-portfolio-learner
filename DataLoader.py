# The 'user interface' for our data loading program.

from importlib import reload
import utils
reload(utils)

base_url = 'https://www.alphavantage.co/query?'
function = 'TIME_SERIES_DAILY'
symbols = ['IBM', 'MSFT']
api_key = utils.get_alpha_key('secrets.yml')

stock_data = utils.yield_stock_data(base_url, function, symbols, api_key)

for result in stock_data:
    print(result)
