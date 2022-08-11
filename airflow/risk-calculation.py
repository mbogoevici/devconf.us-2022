import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

with DAG(dag_id="risk_calculation", start_date=pendulum.datetime(2022, 12, 12)) as dag:

    def generate_numbers():
        return [*range(1,10)]
    
    @task
    def pre_calculation():
        return generate_numbers()

    @task
    def add_one(x:int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        return total

    @task
    def post_calculation(total):
        print(f"Total was {total}")

    data = pre_calculation() 
    total = sum_it(add_one.expand(x = data))
    post_calculation(total)
