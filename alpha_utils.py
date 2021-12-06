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

def alpha_csv_to_dataframe(responses):
    output = []
    if isinstance(responses, list) != True:
        responses = [responses]
    for response in responses:
        decoded_content = response.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        df = pd.DataFrame(cr)
        header, df = df.iloc[0], df[1:]
        df.columns = header
        df.reset_index(drop = True, inplace=True)
        df.columns.name = None
        output.append(df)
    return output

def request_alpha_data(urls) -> requests.Response:
    session = requests.Session()
    retry = Retry(
        connect = CONNECT_RETRIES, 
        backoff_factor = BACKOFF_FACTOR
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    if isinstance(urls, list):
        return [session.get(url) for url in urls]
    else:
        return session.get(urls)

###############################################################################################

def get_alpha_stock_data(
    function: str, symbols: Iterable, api_key: str,
    base_url: str = ALPHA_BASE_URL, 
    output_size: str = 'compact', max_threads: int = 5
) -> Iterator:
    """Multi-threaded function for getting stock price data from Alpha Vantage API.
    This wrapper is for the functions in "Stock Time Series" (see AV API docs)
    Parameters
    -----------
    function: str
        A function or endpoint you want to call. See AV API docs for more info.
    symbols: Iterable
        An iterable objects of stock ticker symbols (strings)
    api_key: str
        The Alpha Vantage API key
    base_url: str
        the alpha vantage URL for the API
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

def get_alpha_technical_data(
    functions: Iterable, symbols: Iterable, api_key: str,
    base_url: str = ALPHA_BASE_URL, 
    interval: str = 'daily', time_period: int = 60, series_type: str = 'close', 
    max_threads: int = 5
) -> Iterator:
    """Multi-threaded function for getting technical data from Alpha Vantage API
    This wrapper is for the functions in "Technical Indicators" (see AV API docs).
    Parameters
    -----------
    functions: Iterable
        A list of functions or endpoints you want to call. See AV API docs for more info.
    symbols: Iterable
        An iterable objects of stock ticker symbols (strings)
    api_key: str
        The Alpha Vantage API key
    base_url: str
        the alpha vantage URL for the API
    interval: str
        1min, 5min, 15min, 30min, 60min, daily, weekly, monthly.   
    time_period: int
        Number of data points used to calculate each moving average value. 
    series_type: str
        The desired price type in the time series. Four types are supported: close, open, high, low
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
        url_list = []
        for function in functions:
            if function == 'VWAP' and interval not in ['daily', 'weekly', 'monthly']:
                raise ValueError(
                    'VWAP is only available for intraday time series intervals'
                )
            sequence = (
                base_url, 'function=', function, '&symbol=', symbol, 
                '&interval=', interval, '&time_period=', time_period, 
                '&series_type=', series_type, '&apikey=', api_key, '&datatype=', data_type
            )
            url_list.append(''.join(map(str, sequence)))
        urls.append(url_list)

    executor = ThreadPoolExecutor(max_threads)
    for result_list in executor.map(request_alpha_data, (url_list for url_list in urls)):
        yield alpha_csv_to_dataframe(result_list)

def write_alpha_results(results: Iterator, symbols: Iterable, dest_path: str) -> None:
    """Writes elements in an Iterator - with the stock ticker as an added column - to a folder as a csv
    Parameters
    -----------
    results: Iterator
        This should be the raw output from the API as an Iterator object.
        Each element should correspond to a stock symbol in 'symbols' and be a list of DataFrames, 
        where each DataFrame in that sub-list corresponds to a function in functions.
    symbols: Iterable
        An iterable object of stock ticker symbols (strings)
    Returns
    --------
    pd.DataFrame
        all the data concatenated into a DataFrame object. 
        The schema will be 'symbol', 'timestamp', followed by whatever cols the API returned
    """
    os.makedirs(dest_path, exist_ok = True)
    for i, result in enumerate(results):
        symbol_df = None
        if isinstance(result, list) != True:
            result = [result]
        for j, df in enumerate(result):
            if isinstance(df, pd.DataFrame) != True:
                raise Exception("stock_data must be an Iterator of pandas DataFrames")
            temp_df = df
            temp_df['symbol'] = symbols[i]
            temp_df = reorder_last_to_first(temp_df)
            temp_df = clean_alpha_cols(temp_df)
            if symbol_df is None:
                symbol_df = temp_df
            else:
                symbol_df = pd.merge(symbol_df, temp_df, on = ['symbol', 'timestamp'])

        symbol_df.to_csv(os.path.join(dest_path, symbols[i] + '.csv'), index=False)
    return None

##########################################################################################

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
    df.columns = ['symbol', 'timestamp'] + df.columns.tolist()[2:]
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
