import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

with DAG(dag_id="risk_calculation", start_date=pendulum.datetime(2022, 3, 4)) as dag:


    def generate_numbers():
        return [*range(1,100)]
    
    @task
    def pre_calculation():
        printf("Beginning risk calculation")

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        return total

    @task
    def post_calculation(total):
        print(f"Total was {total}")


    added_values = add_one.expand(x = generate_numbers())

    pre_calculation >> added_values >> post_calculation

