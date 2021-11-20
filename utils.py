import requests
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from six import Iterator
import yaml

def get_alpha_key(credentials_file) -> None:
    """Grabs credentials for Alpha Vantage API

    Parameters
    -----------
    credentials_file: str
        path to .yml file containing credentials
        requires file to contain entries for 'alpha_key:'
    """
    with open(credentials_file, "r") as stream:
        try:
            credentials = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return credentials["alpha_key"]


def yield_alpha_stock_data(base_url: str, function: str, symbols: list, api_key: str, output_size = 'compact', max_threads = 5) -> Iterator:
    """Multi-threaded function for getting stock data from Alpha Vantage API
    Parameters
    -----------
    #TODO: add parameters
    --------
    generator
        a generator object of your API results in the same order as your list of stocks
    """
    urls = []
    for symbol in symbols:
        sequence = (base_url, 'function=', function, '&symbol=', symbol, '&outputsize=', output_size, '&apikey=', api_key)
        urls.append(''.join(map(str, sequence)))
    urls

    executor = ThreadPoolExecutor(max_threads)
    for result in executor.map(requests.get, urls):
        yield result.json()

def alpha_json_to_dataframe(stock_data: Iterator) -> pd.DataFrame:
    output = []
    for i, result in enumerate(stock_data):
        temp_df = None
        for key in result:
            if key != 'Meta Data':
                temp_df = pd.DataFrame().from_dict(result[key], orient = 'index')
        if temp_df is not None:
            temp_df['symbol'] = result['Meta Data']['2. Symbol']
            temp_df.reset_index(inplace = True)
            temp_df.rename(columns = {"index":"date"***REMOVED***, inplace = True)
            #reorder cols so symbol is first
            cols = list(temp_df.columns)
            cols = [cols[-1]] + cols[:-1]
            temp_df = temp_df[cols]
            output.append(temp_df)
    return pd.concat(output)


#convert to dict of tuples and remove NaN
# categories = {category[0]: tuple(stock for stock in category[1:] if not pd.isna(stock)) \
#                 for category in list(categories.T.itertuples(index=True)) ***REMOVED***