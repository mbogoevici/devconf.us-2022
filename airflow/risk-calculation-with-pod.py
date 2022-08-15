import json
import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook;


with DAG(dag_id="risk_calculation-with-pod", start_date=pendulum.datetime(2022, 2, 12), catchup = False) as dag:

    def generate_numbers():
        return [*range(1,10)]


    @task
    def populate_cache():
        s3_hook = S3Hook(aws_conn_id='s3')
        keys = s3_hook.list_keys(bucket_name='risk-calc', prefix='market-data');
        for key in keys:
            HttpHook(http_conn_id='hazelcast').run(endpoint='rest/v2/caches/market-data/{}'.format(str(key).removesuffix('.json')),
                 data=s3_hook.read_key(key, 'risk-calc/market-data'))

    
    @task
    def extract_portfolios():
        s3_hook = S3Hook(aws_conn_id='s3')
        file = s3_hook.read_key('portfolios.json', 'risk-calc')
        data = json.loads(file)
        return list(map(lambda p: {'PORTFOLIO_DATA': "{}".format(json.dumps(p))}, data))


    calculate_var = KubernetesPodOperator.partial(
        # unique id of the task within the DAG
        task_id='calculate_var',
        # the Docker image to launch
        image='fsi/var:v7',
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace='airflow',
        do_xcom_push=True,
        # Pod configuration
        # name the Pod
        name='fsi-var',
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        # attach labels to the Pod, can be used for grouping
        labels={'app': 'backend', 'env': 'dev'},
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=True,
        # get log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
    ).expand(env_vars=extract_portfolios());

    @task
    def aggregate(values):
        return values


    def _print_results(**kwargs):
        ti = kwargs['ti']
        results = ti.xcom_pull(key='return_value', task_ids=['calculate_var'])
        print(results)


    display_results = PythonOperator(
        task_id='display_results',
        python_callable=_print_results,
        provide_context=True,
        dag=dag)

    def _print_results(total):
        print(f"Total was {total}")


    populate_cache() >> calculate_var >> display_results