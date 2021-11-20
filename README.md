# big-portfolio-learner

Final project for CIS 545 Big Data Analytics. 

The project proposal can be found [here](https://docs.google.com/document/d/1jpFaxwhn7syQj1THJNVp7wHw0W0kc132rCYXXk73hac/edit?usp=sharing)

## Prerequisites and setup

- assumes the working directory has a file called `secrets.yml` which contains the credentials for the data APIs. For Alpha Vantage, there should be a line `alpha_key: "[YOUR API KEY]"`.

- `requirements.txt` contains the packages and versions used thus far. Generated using `pipreqs`. More information on `pipreqs` can be found [here](https://github.com/bndr/pipreqs).

- install packages using `pip install -r requirements.txt` or `sudo -H pip install -r requirements.txt`.

## DataLoader.py

Contains the interface for loading data from Alpha Vantage API

## utils.py

Contains utility functions for grabbing data from Alpha Vantage API
