import json
import random

tickers = ['AMZN', 'DAL', "DIS", 'GE', 'GME', 'GOOG', 'IBM']

count = 1000

requests = []

for i in range (0, count-1):
    keys = random.sample(tickers, k=3)
    request = {}
    request['confidence'] = 0.99
    portfolio = {}
    for key in keys:
        portfolio[key] = random.choice(range(100, 200))
    request['portfolio'] = portfolio
    requests.append(request)

print(json.dumps(requests))