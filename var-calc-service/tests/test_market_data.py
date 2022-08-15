# Name:     test_market_data.py
# Purpose:  Test cases for market data
# Author:   Aric Rosenbaum


import datetime
from market_data import MarketData
from pytest import approx
 

# Fetch data if cache is available
def test_cache_hit():

    md = MarketData("http://localhost:11222", "market-data", "demo", "password")

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 3, 31)
    price = md.get(["ibm"], start, end)
    assert(price.shape[0] == 61)
    assert(price.shape[1] == 1)

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 3, 31)
    price = md.get(["aapl", "ibm"], start, end)
    assert(price.shape[0] == 61)
    assert(price.shape[1] == 2)

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 1, 31)
    price = md.get(["aapl", "ibm", "t"], start, end)
    assert(price.shape[0] == 19)
    assert(price.shape[1] == 3)


# Fetch data if no cache
def test_no_cache():

    md = MarketData()

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 3, 31)
    price = md.get(["ibm"], start, end)
    assert(price.shape[0] == 61)
    assert(price.shape[1] == 1)

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 3, 31)
    price = md.get(["aapl", "ibm"], start, end)
    assert(price.shape[0] == 61)
    assert(price.shape[1] == 2)

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 1, 31)
    price = md.get(["aapl", "ibm", "t"], start, end)
    assert(price.shape[0] == 19)
    assert(price.shape[1] == 3)
