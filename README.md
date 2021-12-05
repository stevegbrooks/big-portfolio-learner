# big-portfolio-learner v0.3.0

Final project for CIS 545 Big Data Analytics. 

The project proposal can be found [here](https://docs.google.com/document/d/1jpFaxwhn7syQj1THJNVp7wHw0W0kc132rCYXXk73hac/edit?usp=sharing)

## Prerequisites and setup

- requires an Alpha Vantage API key and knowledge of the API. For documentation of the Alpha Vantage API, please go (here)[https://www.alphavantage.co/documentation/]

- assumes the working directory has a file called `secrets.yml` which contains the credentials for the data APIs. For Alpha Vantage, there should be a line `alpha_key: "[YOUR API KEY]"`.

- `requirements.txt` contains the packages and versions used thus far. Generated using `pipreqs`. More information on `pipreqs` can be found [here](https://github.com/bndr/pipreqs).

- install packages using `pip install -r requirements.txt` or `sudo -H pip install -r requirements.txt`.

## DataLoader.py

Contains the interface for loading data from Alpha Vantage API.

The DataLoader will call the API using a ThreadPoolExecutor. It will then write .csv files (for each stock ticker) to `stock_data/` in the order of the stock ticker symbols passed to it.

Since each tickers' data is >1000 rows, and there are almost 10,000 tickers, this is the only way to manage it without blowing through our RAM.

The last step is to zip up the `stock_data/` directory so that it can be manually uploaded to our S3 bucket.

## alpha_utils.py

Contains utility functions for grabbing data from Alpha Vantage API.
