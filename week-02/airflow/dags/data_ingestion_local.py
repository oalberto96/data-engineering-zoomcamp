import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable


PG_HOST= os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

local_worflow = DAG(
    "TaxiNYDataDAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'yello_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_worflow:
    wget_task = BashOperator(
        task_id="wget",
        # bash_command=f"curl -sSL {url} > {AIRFLOW_HOME}/output.csv"
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user = PG_USER, 
            password=PG_PASSWORD, 
            host=PG_HOST, 
            port=PG_PORT, 
            db=PG_DATABASE, 
            table_name=TABLE_NAME_TEMPLATE, 
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    wget_task >> ingest_task