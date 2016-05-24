# toolbelt_python_GA
python notebooks to analyze GA data

# How-to
- Add a client_secrets.json in root folder. More info : https://console.developers.google.com/.
- Add a config.py file.

# Notebooks
## _notebook_independence_analysis.ipynb
Process the independence of products that were bought.

## _notebook_pages_analysis.ipynb
Simple lr on pageviews/transactions & transactions

## _notebook_time_series.ipynb
Simple time series analysis with moving avg

## _notebook_checkout.ipynb
checkout by devices

## _notebook_bayesian_ab_testing.ipynb
Test if an A/B test can be stop. 

# structure config.py
- property : int
- ua : string
- profile_id : string

# todo
- Label on legend.
- label on nodes.