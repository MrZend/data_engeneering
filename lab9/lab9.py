from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time


def delayed_task():
    print("Початок завдання з затримкою...")
    time.sleep(5)
    print("Завдання робиться!")
    print("Завдання завершено після затримки.")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'daily_delayed_task',
    default_args=default_args,
    description='DAG з затримкою виконання завдань',
    schedule_interval='@daily',
)


delayed_task = PythonOperator(
    task_id='delayed_task',
    python_callable=delayed_task,
    dag=dag,
)

