# Name:     app.py
# Purpose:  Calculate value at risk for a portfolio of US equities using the variance-covariance method
# Author:   Aric Rosenbaum
 

import datetime
from flask import Flask, json, request
from prometheus_client import Counter, Gauge, generate_latest
from market_data import MarketData
from portfolio import Portfolio
from value_at_risk import ValueAtRisk
import os


# Flask init
app = Flask(__name__)


# Prometheus metrics
#   N.B. - In production, a label for an entity may not be advisable if there is high cardinality (i.e. entity=account)
metric_risk_entity_var = Gauge('risk_entity_var', 'Risk: Entity value at risk (VaR)', ['entity_type', 'entity_id', 'confidence'])
metric_risk_calc_pref_seconds = Gauge('risk_calc_perf', 'Risk: Performance of risk calculation (seconds)', ['method'])
metric_risk_app = Counter('risk_app', 'Risk: Application', ['endpoint', 'method', 'status'])


# Value at risk endpoint
@app.route("/value-at-risk", methods=['POST'])
def value_at_risk():
    print("RECEIVED VAR CALC REQUEST")

    # Parms
    request_data = request.json

    print("REQUEST: {}".format(request_data))

    data = calculate_value_at_risk(request_data)
    return data, 200, {"Content-Type": "application/json"}


def calculate_value_at_risk(request_data):
    if (isinstance(request_data, list)) :
        requests = request_data
    else:
        requests = [request_data]

    results = []

    for request in requests:
        portfolio = Portfolio();

        for position in request.get("portfolio"):
            portfolio.addPosition(position["symbol"], position["quantity"])
            confidence = request.get("confidence", 0.99)
            correlation_id = request.get("correlationID", "")
            entity_type = request.get("entity_type", "")
            entity_id = request.get("entity_id", "")
            # Fetch closing price for each symbol for the last year
            #   N.B. - Comment/uncomment appropriate MarketData init depending on if you are running a cache
            start = datetime.date.today() - datetime.timedelta(days=365)
            end = datetime.date.today()
            market_data = MarketData()
            # print("CALLING MARKET DATA GET")
            hist_prices = market_data.get(portfolio.symbols(), start, end)
            # Calculate value at risk
            var = ValueAtRisk()
            with metric_risk_calc_pref_seconds.labels('VaR').time():
                results = var.calculate(portfolio, hist_prices, confidence)
            # Entity value at risk
            metric_risk_entity_var.labels(entity_type, entity_id, confidence).set(results)
            metric_risk_app.labels("/value-at-risk", "GET", 200).inc()
            # Return VaR
            value_at_risk = {"correlationID": correlation_id, "entity_type": entity_type, "entity_id": entity_id,
                    "confidence": confidence, "valueAtRisk": results,
                    "valueAtRiskAsOf": int(datetime.datetime.utcnow().timestamp() * 1000)}
            results.add(value_at_risk)

    if isinstance(request_data, list):
        return results
    else:
        return results[0]


# Crash test endpoint.  Useful to confirm error logging if working.
@app.route("/crash", methods=['GET'])
def crash():
    metric_risk_app.labels("/crash", "GET", 200).inc()
    a = 10 / 0
    return


# Health endpoint
@app.route("/health", methods=['GET'])
def health():
    metric_risk_app.labels("/health", "GET", 200).inc()
    return "", 200


# Info endpoint
@app.route("/info", methods=['GET'])
def info():
    metric_risk_app.labels("/info", "GET", 200).inc()
    return "Value at Risk (VaR), version 1.2.0"


# App error
@app.errorhandler(Exception)
def server_error(err):
    # N.B. - In prod, don't return detailed error descriptions.
    metric_risk_app.labels("/value-at-risk", "GET", 500).inc()
    return "A runtime exception has occurred: " + err.args[0], 500


# Prometheus metrics
@app.route("/metrics")
def metrics():
    metric_risk_app.labels("/metrics", "GET", 200).inc()
    return generate_latest()


# Bootstrap
if __name__ == '__main__':
    if (os.getenv('PORTFOLIO_DATA') != None):
        portfolio_data = json.loads(os.getenv('PORTFOLIO_DATA'))
        value_at_risk = calculate_value_at_risk(portfolio_data);
        print("{}".format(json.dumps(value_at_risk)))
    else:
        app.run()
