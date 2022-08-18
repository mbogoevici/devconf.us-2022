FROM apache/airflow:2.3.3

COPY --chown=airflow:root risk-calculation-with-pod.py /opt/airflow/dags
COPY --chown=airflow:root risk-calculation-with-serverless.py /opt/airflow/dags
COPY --chown=airflow:root task_cube.py /opt/airflow/dags