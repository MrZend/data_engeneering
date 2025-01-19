from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'spark_dag',
    default_args=default_args,
    description='DAG to run Spark jobs',
    schedule_interval='@daily',
)

spark_task = SparkSubmitOperator(
    task_id='spark_task',
    application='/opt/airflow/dags/spark_job.py',
    conn_id='spark_default',
    dag=dag,
)

