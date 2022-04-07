import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable
from airflow.utils.dates import days_ago


PG_HOST= os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

local_worflow = DAG(
    "TaxiNYZonesDataDAG",
    start_date=days_ago(1),
    schedule_interval=None,
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc'
URL_TEMPLATE = URL_PREFIX + '/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_taxi_zones.csv'
TABLE_NAME_TEMPLATE = 'taxi_zones'

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