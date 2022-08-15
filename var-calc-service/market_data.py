# Name:     market_data.py
# Purpose:  Market Data
# Author:   Aric Rosenbaum


import json
import requests
from requests.status_codes import codes
import pandas as pd
import pandas_datareader as web 
from prometheus_client import Histogram
import time
import os


# Prometheus metrics
metric_risk_cache = Histogram('risk_cache', 'Risk: Cache performance', ['namespace', 'method', 'hit_miss'])
metric_risk_md_yahoo_get_latency = Histogram('risk_md_yahoo_get_latency', 'Risk: Market data get from Yahoo! latency (seconds)')


class MarketData:

    # Constructor
    #def __init__(self, url = None, cache_name = None, username = None, password = None):
        # print("Market Data init")

    # Return a dataframe of daily closing prices
    def get(self, symbols, start, end):
        #print("ENTER GET MARKET DATA")
        _datagrid_url = "http://risk-calc-cache.infinispan.svc.cluster.local:11222/rest/v2/caches/market-data"
        _cache_user = "testuser"
        _cache_pass = "testpassword"

        # Create an empty dataframe to hold closing prices
        symbols_price = pd.DataFrame()

        # Iterate thru symbols
        for symbol in symbols:

            #print("LOOKING UP PRICES FOR {}".format(symbol))
            _cache_url = _datagrid_url + "/" + symbol

            perf_start = time.perf_counter()
            r = requests.get(_cache_url, verify=False)
            perf_end = time.perf_counter()

            if r.status_code == 200:
                #print("DATAGRID RETURNED 200")
                s = pd.Series(json.loads(r.text))
                symbols_price[symbol] = s
                
           # else:
                # Add appropriate error handling here (i.e. unauthorized)
            #print("ERROR: market_data.py - Unable to retrieve data from cache.  symbol: " + symbol + ", response code: " + str(r.status_code))

        #print("EXIT GET MARKET DATA")
        return symbols_price
