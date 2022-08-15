# Name:     test_portfolio.py
# Purpose:  Test cases for portfolio
# Author:   Aric Rosenbaum


import numpy as np
import pandas as pd
from portfolio import Portfolio
from pytest import approx


# Add a position to a new portfolio
def test_add_position():

    p = Portfolio()
    assert p.rows() == 0
    
    p.addPosition("T", 100, 10)
    assert p.rows() == 1

    p.addPosition("IBM", 100, 20.12)
    assert p.rows() == 2


# Calculate a portfolio's mark-to-market (MTM)
def test_mtm():

    p = Portfolio()
    p.addPosition("T", 100, 10)
    assert p.mtm() == approx(1000)

    p = Portfolio()
    p.addPosition("T", 100, 10)
    p.addPosition("IBM", 200, 20)
    assert p.mtm() == approx(5000)
    
    p = Portfolio()
    p.addPosition("T", 100, 0.01)
    p.addPosition("IBM", 200, 20.3333)
    assert p.mtm() == approx(4067.66)


# Weight of each position in a portfolio
def test_weights():

    p = Portfolio()
    p.addPosition("T", 100, 10)
    weights = p.weights()
    assert len(weights) == 1
    assert weights[0] == 1

    p = Portfolio()
    p.addPosition("T", 100, 10)
    p.addPosition("IBM", 200, 20)
    weights = p.weights()
    assert len(weights) == 2
    np.testing.assert_array_equal(weights, np.array([0.2, 0.8]))

    p = Portfolio()
    p.addPosition("T", 100, 20)
    p.addPosition("IBM", 100, 20)
    p.addPosition("MSFT", 100, 20)
    weights = p.weights()
    assert len(weights) == 3
    np.testing.assert_array_equal(weights, np.array([1/3, 1/3, 1/3]))


# Weight of each position in a portfolio
def test_symbols():

    p = Portfolio()
    p.addPosition("T", 100, 20)
    p.addPosition("IBM", 100, 20)
    p.addPosition("MSFT", 100, 20)
    symbols = p.symbols()
    assert len(symbols) == 3
    np.testing.assert_array_equal(symbols, np.array(['T', 'IBM', 'MSFT']))


# Mean weighted return of a portfolio
def test_mean_return():

    p = Portfolio()
    returns = pd.Series([0.06, 0.10, 0.08])
    weights = np.array([0.17857142857142858, 0.5714285714285714, 0.25])
    assert p.mean_return(returns, weights) == approx(0.08785714286)

    returns = pd.Series([0.2366, 0.2138, 0.1843, 0.0551, 0.2763, 0.1763])
    weights = np.array([1/6, 1/6, 1/6, 1/6, 1/6, 1/6])
    assert p.mean_return(returns, weights) == approx(0.1904)

    returns = pd.Series([0.079, 0.828, 1.776, -0.063, 0.980])
    weights = np.array([1/5, 1/5, 1/5, 1/5, 1/5])
    assert p.mean_return(returns, weights) == approx(0.720)


# Variance of portfolio return 
def test_variance():

    p = Portfolio()
    weights = np.array([1/6, 1/6, 1/6, 1/6, 1/6, 1/6])
    covariance_matrix = pd.DataFrame({
        'GE':   {'GE': 0.1035033959, 'MSFT': 0.0758485175, 'JNJ': 0.0221593970, 'K': -0.0042982318, 'BA': 0.0857454038, 'IBM': 0.0122997714}, 
        'MSFT': {'GE': 0.0758485175, 'MSFT': 0.1657441941, 'JNJ': 0.0412281787, 'K': -0.0051726477, 'BA': 0.0379254409, 'IBM': -0.0022397304}, 
        'JNJ':  {'GE': 0.0221593970, 'MSFT': 0.0412281787, 'JNJ': 0.0359678522, 'K': 0.0181109500, 'BA': 0.0101011622, 'IBM': -0.0039328902}, 
        'K':    {'GE': -0.0042982318, 'MSFT': -0.0051726477, 'JNJ': 0.0181109500, 'K': 0.0569526295, 'BA': -0.0076207963, 'IBM': -0.0046186177}, 
        'BA':   {'GE': 0.0857454038, 'MSFT': 0.0379254409, 'JNJ': 0.0101011622, 'K': -0.0076207963, 'BA': 0.0895710313, 'IBM': 0.0248229795}, 
        'IBM':  {'GE': 0.0122997714, 'MSFT': -0.0022397304, 'JNJ': -0.0039328902, 'K': -0.0046186177, 'BA': 0.0248229795, 'IBM': 0.0183913055}
        })
    assert p.variance(weights, covariance_matrix) == approx(0.0297457828)


# Variance of portfolio return 
def test_std_dev():

    p = Portfolio()
    
    assert p.std_dev(0) == 0
    assert p.std_dev(0.0257) == approx(0.16031220)
    
