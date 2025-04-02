import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime

# Set the environment variable so the GCP SDK can find your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/keys/retail-data-pipeline-453817-5c7165d921e7.json"

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="upload_to_gcs_raw_data_dag",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    description="Upload local raw retail data file to GCS",
) as dag:

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_raw_file",
        src="/opt/airflow/data/retail_new_data.csv",  # Make sure the file is here
        dst="raw/retail_raw_data.csv",  # GCS path inside bucket
        bucket="retail-data-pipeline-453817-bucket",  # Your GCS bucket name
    )

    upload_file  # This runs the task
