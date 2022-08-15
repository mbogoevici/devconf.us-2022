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
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


with DAG(dag_id="risk_calculation-with-pod", start_date=pendulum.datetime(2022, 2, 12), catchup = False) as dag:

    def generate_numbers():
        return [*range(1,10)]
    
    @task
    def pre_calculation():
        s3_hook = S3Hook(aws_conn_id='s3')
        file = s3_hook.read_key('portfolios.json', 'risk-calc')
        data = json.loads(file)
        return data


    calculate_var = KubernetesPodOperator.partial(
        # unique id of the task within the DAG
        task_id='calculate_var',
        # the Docker image to launch
        image='fsi/var:v4',
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
    ).expand(env_vars = [{'PORTFOLIO_DATA': '{"confidence": 0.99,"portfolio": [{"symbol": "GME", "quantity": 100}, {"symbol": "IBM", "quantity": 50}]}'}, {'PORTFOLIO_DATA': '{"confidence": 0.99,"portfolio": [{"symbol": "GME", "quantity": 100}, {"symbol": "IBM", "quantity": 100}]}'}]);

    @task
    def aggregate(values):
        return values

    @task
    def post_calculation(total):
        print(f"Total was {total}")

    calculate_var