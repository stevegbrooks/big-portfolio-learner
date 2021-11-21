from typing import Iterable
import requests
from requests.adapters import HTTPAdapter
from requests.api import request
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from six import Iterator
import yaml
import csv
import re
import os

ALPHA_BASE_URL = 'https://www.alphavantage.co/query?'
CONNECT_RETRIES = 3
BACKOFF_FACTOR = 0.5

###############################################################################################
def get_alpha_key(credentials_file) -> None:
    """Grabs credentials for Alpha Vantage API from a yaml file
    Parameters
    -----------
    credentials_file: str
        path to .yml file containing credentials
        requires file to contain entries for 'alpha_key:'
    Returns
    -----------
    None
    """
    with open(credentials_file, "r") as stream:
        try:
            credentials = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return credentials["alpha_key"]

def get_alpha_listings(
    api_key: str, base_url: str = ALPHA_BASE_URL, 
    date: str = None, state: str = None) -> Iterator:
    """Gets all stock listings from Alpha Vantage
    Parameters
    -----------
    api_key: str
        The Alpha Vantage API key
    base_url: str
        the alpha vantage URL for the API
    date: str
        the listings as of a certain date. if None, then uses todays date
    state: str
        the state of the listings to be returned. See Alpha Vantage docs for more info
    Returns
    -----------
    pandas DataFrame
        a DataFrame containing the Alpha Vantage listings
    """
    sequence = (base_url, 'function=LISTING_STATUS')
    
    if date is not None:
        sequence += ('&date=', date)
    if state is not None:
        sequence += ('&state=', state)
    
    sequence = sequence + ('&apikey=', api_key)
    url = ''.join(map(str, sequence))
    response = requests.get(url)
    df = alpha_csv_to_dataframe(response)
    return df

def alpha_csv_to_dataframe(response):
    """Converts a CSV response from Alpha Vantage to a pandas DataFrame
    Parameters
    -----------
    response: requests.Response
        a response object from the Alpha Vantage API
    Returns
    --------
    pd.DataFrame
    """
    decoded_content = response.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    df = pd.DataFrame(cr)
    header, df = df.iloc[0], df[1:]
    df.columns = header
    df.reset_index(drop = True, inplace=True)
    df.columns.name = None
    return df

###############################################################################################

def get_alpha_stock_data(
    function: str, symbols: Iterable, api_key: str,
    base_url: str = ALPHA_BASE_URL, 
    output_size: str = 'compact', max_threads: int = 5) -> Iterator:
    """Multi-threaded function for getting stock data from Alpha Vantage API
    Parameters
    -----------
    base_url: str
        the alpha vantage URL for the API
    function: str
        the 'function' or endpoint you want to call. See AV API docs for more info.
    symbols: Iterable
        An iterable objects of stock ticker symbols (strings)
    api_key: str
        The Alpha Vantage API key
    output_size: str
        'compact' or 'full'. See AV API docs for more info
    max_threads: int
        Number of threads for ThreadPool
    Returns
    --------
    Iterator
        a generator object of your API results in the same order as your list of symbols
    """
    data_type = 'csv' #most memory efficient
    urls = []
    for symbol in symbols:
        sequence = (
            base_url, 'function=', function, '&symbol=', 
            symbol, '&outputsize=', output_size, '&apikey=', api_key,
            '&datatype=', data_type
        )
        urls.append(''.join(map(str, sequence)))

    executor = ThreadPoolExecutor(max_threads)
    for result in executor.map(request_alpha_data, urls):
        yield alpha_csv_to_dataframe(result)

def request_alpha_data(url):
    """ 
    Makes a request to the Alpha Vantage API and returns the response
    Uses retry logic to handle connection errors
    Parameters
    -----------
    url: str
        the url to request
    Returns
    --------
    requests.Response
    """
    session = requests.Session()
    retry = Retry(
        connect = CONNECT_RETRIES, 
        backoff_factor = BACKOFF_FACTOR
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session.get(url)

def write_alpha_results(results: Iterator, symbols: Iterable, dest_path: str) -> None:
    """Writes elements in an Iterator - with the stock ticker as an added column - to a folder as a csv
    Parameters
    -----------
    results: Iterator
        This should be the raw JSON output from the API as an Iterator object, one element for each symbol
    symbols: Iterable
        An iterable objects of stock ticker symbols (strings)
    Returns
    --------
    pd.DataFrame
        all the data concatenated into a DataFrame object. 
        The schema will be 'symbol', 'timestamp', followed by whatever cols the API returned
    """
    os.makedirs(dest_path, exist_ok = True)
    for i, result in enumerate(results):
        if isinstance(result, pd.DataFrame):
            result['symbol'] = symbols[i]
            result = reorder_last_to_first(result)
            result = clean_alpha_cols(result)
            result.to_csv(os.path.join(dest_path, symbols[i] + '.csv'), index=False)
        else:
            raise Exception("stock_data must be an Iterator of pandas DataFrames")
    return None

def clean_alpha_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans up column names coming out of the Alpha Vantage API
    Parameters
    -----------
    df: pd.DataFrame
    Returns
    --------
    pd.DataFrame
    """
    new_cols = df.columns
    new_cols = [col.strip() for col in new_cols]
    new_cols = [re.sub("[0-9]\\.\s", "", col) for col in new_cols]
    new_cols = [re.sub("\s", "_", col) for col in new_cols]
    df.columns = new_cols
    return df

def reorder_last_to_first(df: pd.DataFrame) -> pd.DataFrame:
    """Reorders columns so the last column is the first
    Parameters
    -----------
    df: pd.DataFrame
        a pandas dataframe
    Returns
    --------
    pd.DataFrame
        a dataframe with the last column first
    """
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    return df[cols]
