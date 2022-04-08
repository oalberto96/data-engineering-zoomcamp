import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_to_gcp import format_to_parquet, upload_to_gcs

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

local_worflow = DAG(
    "TaxiNYDataGCPDAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_FILE_NAME = "ny_output_{{ execution_date.strftime(\'%Y-%m\') }}"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f'/{OUTPUT_FILE_NAME}.csv'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'


with local_worflow:
    wget_task = BashOperator(
        task_id="wget",
        # bash_command=f"curl -sSL {url} > {AIRFLOW_HOME}/output.csv"
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            'src_file': OUTPUT_FILE_TEMPLATE
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_NAME}",
            "local_file": f"{OUTPUT_FILE_NAME}.parquet"
        }
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_NAME_TEMPLATE,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{OUTPUT_FILE_NAME}"]
            }
        }
    )

    

    wget_task >> format_to_parquet >> local_to_gcs_task >> bigquery_external_table_task
