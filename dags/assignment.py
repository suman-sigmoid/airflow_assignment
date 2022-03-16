from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from suman import welcome
from question1 import write_csv
from question2 import create_weather_table

default_args = {
    "owner": "Suman",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 16),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("Assignment", default_args=default_args, schedule_interval="0 6 * * *")
t1= PythonOperator(task_id='Greet', python_callable=welcome, dag=dag)
t2= PythonOperator(task_id='Write_into_csv', python_callable=write_csv, dag=dag)
t3=PythonOperator(task_id='create_table', python_callable=create_weather_table, dag=dag)
t1 >> t2 >> t3
