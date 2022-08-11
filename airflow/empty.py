from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

with DAG(
    "basic_dag", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:
    t1 = EmptyOperator(task_id="task_1")
    t2 = EmptyOperator(task_id="task_2")

    t1 >> t2