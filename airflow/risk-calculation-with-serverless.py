import json
import logging
import os
from builtins import int
from datetime import datetime

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.providers.http.hooks.http import HttpHook;
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(dag_id="risk_calculation-serverless", start_date=pendulum.datetime(2022, 2, 12),
         catchup = False, concurrency=100, max_active_tasks = 100) as dag:

    def remove_suffix(input_string, suffix):
        if suffix and input_string.endswith(suffix):
            return input_string[:-len(suffix)]
        return input_string

    def remove_prefix(input_string, prefix):
        if prefix and input_string.startswith(prefix):
            return input_string[len(prefix):]
        return input_string

    @task
    def populate_cache():
        s3_hook = S3Hook(aws_conn_id='s3')
        print(s3_hook.get_credentials())
        print(s3_hook.get_connection('s3').host)
        print(s3_hook.get_connection('s3').port)
        print(s3_hook.get_connection('s3').conn_type)
        keys = s3_hook.list_keys(bucket_name='risk-calc', prefix='market-data');
        for key in keys:
            print(key)
            ticker = remove_prefix(remove_suffix(key, ".json"), 'market-data/')
            print(ticker)
            HttpHook(method='PUT', http_conn_id='data-grid').run(endpoint='rest/v2/caches/market-data/{}'.format(ticker),
                                                   data=s3_hook.read_key(key, bucket_name='risk-calc'))

    
    @task
    def read_portfolios():
        s3_hook = S3Hook(aws_conn_id='s3')
        file = s3_hook.read_key('portfolios.json', 'risk-calc')
        data = json.loads(file)
        print(data)
        return list(map(lambda d: "{}".format(json.dumps(d)), data));

    calculate_var = SimpleHttpOperator.partial(
        task_id = 'calculate_var',
        endpoint = '/value-at-risk',
        headers = { 'Content-type': "application/json"},
        http_conn_id='risk-calc-service',
        retries=10,
        retry_delay = 1
    ).expand(data=read_portfolios());

    @task
    def aggregate(values):
        return values

    def _write_results_to_S3(**kwargs):
        ti = kwargs['ti']
        results = ti.xcom_pull(key='return_value', task_ids=['calculate_var'])
        s3_hook = S3Hook(aws_conn_id='s3')
        s3_hook.load_string(json.dumps(list(map(lambda r: json.loads(r), results)), indent=2), bucket_name= 'risk-calc',
                            key="results/value-at-risk-{}-{}.json".format(str(datetime.utcnow()).split()[0],
                                                                          str(datetime.utcnow()).split()[1]))

    publish_results = PythonOperator(
        task_id='publish_results',
        python_callable=_write_results_to_S3,
        provide_context=True,
        dag=dag)


    populate_cache() >> calculate_var >> publish_results