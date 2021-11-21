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
    base_url: str
        the alpha vantage URL for the API
    function: str
        the 'function' or endpoint you want to call. See AV API docs for more info.
    symbols: list
        A list of stock ticker symbols (strings)
    api_key: str
        The Alpha Vantage API key
    output_size: str
        'compact' or 'full'. See AV API docs for more info
    max_threads: int
        Number of threads for ThreadPool
    --------
    returns: Iterator
        a generator object of your API results in the same order as your list of symbols
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
    """Converts raw JSON output from Alpha Vantage API to a pandas dataframe
    Parameters
    -----------
    stock_data: Iterator
        This should be the raw JSON output from the API as an Iterator object, one element for each symbol
    --------
    returns: pd.DataFrame
        all the data concatenated into a DataFrame object. 
        The schema will be 'symbol', 'date', followed by whatever the API returned
    """
    output = []
    for i, result in enumerate(stock_data):
        #put non 'meta data' into a df if it exists
        temp_df = None
        for key in result:
            if key != 'Meta Data':
                temp_df = pd.DataFrame().from_dict(result[key], orient = 'index')
        #if there's data, add the relevant meta data in as cols
        if temp_df is not None:
            temp_df['symbol'] = result['Meta Data']['2. Symbol']
            temp_df.reset_index(inplace = True)
            temp_df.rename(columns = {"index":"date"}, inplace = True)
            #reorder cols so symbol is first
            cols = list(temp_df.columns)
            cols = [cols[-1]] + cols[:-1]
            temp_df = temp_df[cols]
            output.append(temp_df)
    return pd.concat(output)
