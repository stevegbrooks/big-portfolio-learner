# big-portfolio-learner v1.0.0

Final project for CIS 545 Big Data Analytics. 

The project proposal can be found [here](https://docs.google.com/document/d/1jpFaxwhn7syQj1THJNVp7wHw0W0kc132rCYXXk73hac/edit?usp=sharing)

## Prerequisites and setup

- requires an Alpha Vantage API key and knowledge of the API. For documentation of the Alpha Vantage API, please go (here)[https://www.alphavantage.co/documentation/]

- assumes the working directory has a file called `secrets.yml` which contains the credentials for the data APIs. For Alpha Vantage, there should be a line `alpha_key: "[YOUR API KEY]"`.

- `requirements.txt` contains the packages and versions used thus far. Generated using `pipreqs`. More information on `pipreqs` can be found [here](https://github.com/bndr/pipreqs).

- install packages using `pip install -r requirements.txt` or `sudo -H pip install -r requirements.txt`.

## Analysis and Portfolio Learner

### `notebooks/*.ipynb`

The subdirectory `notebooks/` contains the iPython notebooks designed to run on Google CoLab.

These notebook pull the data from an S3 bucket, do some Exploratory Data Analysis, clean and munges the data, and finally implements the core logic of predicting which stocks to invest in using time series analysis.

_NOTE:_ Due to the time constraints, we were only able to finish up to `step3a_single_tsa.ipynb` where we implement a time series analysis on a single stock. `step3b_global_tsa.ipynb` was intended to implement a global time series analysis on all stocks' time series, but wasn't finished.

## Pulling data from the Alpha Vantage API

### DataLoader.py

Contains the interface for loading data from Alpha Vantage API.

The DataLoader will call the API and write .csv files (for each stock ticker) to the output paths provided in the order of the stock ticker symbols passed to it.

The last step is to zip up the data directories so that it can be manually uploaded to our S3 bucket.

### alpha_utils.py

Contains utility functions for grabbing data from Alpha Vantage API.
