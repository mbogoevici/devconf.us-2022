# Name:     test_value_at_risk.py
# Purpose:  Test cases for value at risk (VaR)
# Author:   Aric Rosenbaum


import numpy as np
from portfolio import Portfolio
from value_at_risk import ValueAtRisk
from pytest import approx


# Add a position to a new portfolio
def test_var_covar():

    var = ValueAtRisk()
    p = Portfolio()
    p.addPosition("GME", 100)
    p.addPosition("T", 200)
    p.addPosition("IBM", 50)
    #TODO: assert var.calculate(p, 0.99) == approx(1)


#TODO:
# assert var._var_covar(0.95, mtm, mean, std_dev) == approx(223347.78)

