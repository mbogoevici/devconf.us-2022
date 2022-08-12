import json
import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator


with DAG(dag_id="risk_calculation", start_date=pendulum.datetime(2022, 2, 12), catchup = False) as dag:

    def generate_numbers():
        return [*range(1,10)]
    
    @task
    def pre_calculation():
        s3_hook = S3Hook(aws_conn_id='s3')
        file = s3_hook.read_key('portfolios.json', 'risk-calc')
        data = json.load(file)
        return data

    @task
    def add_one(portfolio_data):
        return portfolio_data['quantity'];

    @task
    def sum_it(values):
        total = sum(values)
        return total

    @task
    def post_calculation(total):
        print(f"Total was {total}")

    data = pre_calculation() 
    total = sum_it(add_one.expand(portfolio_data = data))
    post_calculation(total)
