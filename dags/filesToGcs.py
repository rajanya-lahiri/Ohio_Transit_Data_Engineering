# dags/upload_and_load_gtfs.py
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

INCLUDE_PATH = "/usr/local/airflow/include/dataset/cotagtfs"
BUCKET_NAME = "ohio_transit_storage"
GCP_CONN_ID = "google_gcs_conn"
BQ_PROJECT_ID = "ohio-transit-460000"
BQ_DATASET = "ohio_gtfs"

DBT_DIR = "/usr/local/airflow/include/dbt/ohio_transit"           # <-- NEW
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt/profiles"      # <-- NEW
GOOGLE_CREDS = "/usr/local/airflow/include/ohio-transit-creds.json"

@dag(
    start_date=datetime(2025, 8, 12),
    schedule=None,
    catchup=False,
    tags=["gtfs", "upload", "bigquery", "dbt"],
)
def upload_gtfs_to_gcs():

    # 1) Collect file list once, outside groups (so both can use it)
    csv_filenames = [
        f for f in os.listdir(INCLUDE_PATH)
        if os.path.isfile(os.path.join(INCLUDE_PATH, f)) and f.endswith(".txt")
    ]

    # 2) Upload: Local -> GCS
    with TaskGroup("upload_all_files") as upload_group:
        for filename in csv_filenames:
            LocalFilesystemToGCSOperator(
                task_id=f"upload_{filename.replace('.', '_')}",
                src=os.path.join(INCLUDE_PATH, filename),
                dst=f"raw/{filename}",
                bucket=BUCKET_NAME,
                gcp_conn_id=GCP_CONN_ID,
                mime_type="text/plain",
            )

    # 3) Load: GCS -> BigQuery
    with TaskGroup("load_to_bigquery") as load_group:
        for filename in csv_filenames:
            table_name = filename.replace(".txt", "")
            GCSToBigQueryOperator(
                task_id=f"load_{table_name}_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[f"raw/{filename}"],
                destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}",
                source_format="CSV",
                field_delimiter=",",
                skip_leading_rows=1,
                allow_quoted_newlines=True, 
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id=GCP_CONN_ID,
            )

    # 4) dbt 
    run_dbt = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt deps && "
            "dbt run && "
            "dbt test"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "GOOGLE_APPLICATION_CREDENTIALS": GOOGLE_CREDS,
        },
        retries=2,
        retry_delay=timedelta(minutes=3),
        do_xcom_push=False,
    )

    # 4) Chain: Upload -> Load >> run_dbt
    upload_group >> load_group >> run_dbt

upload_gtfs_to_gcs()