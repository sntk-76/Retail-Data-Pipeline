import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner':'airflow',
    'start_date':datetime(2024,1,1),
    'retries':1
}

with DAG (
    dag_id = 'bucket_to_bigquery',
    description='automate the process of the loading data to the big query dataset from the storage bucket',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as dag:
    
    transfer_file = GCSToBigQueryOperator(

        task_id = 'load_to_bq',
        bucket='retail-data-pipeline-453817-bucket',
        source_objects=['clean/retail_clean_data.csv'],
        destination_project_dataset_table='retail-data-pipeline-453817.transformed_retail_data_pipeline_dataset.retail_clean_data',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[
            {"name" : "Transaction_ID", "type": "INTEGER","mode":"NULLABLE"},
            {"name" : "Customer_ID", "type": "INTEGER","mode":"NULLABLE"},
            {"name" : "City", "type": "STRING","mode":"NULLABLE"},
            {"name" : "State", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Zipcode", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Country", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Age", "type": "INTEGER","mode":"NULLABLE"},
            {"name" : "Gender", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Income", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Customer_Segment", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Date", "type": "DATE","mode":"NULLABLE"},
            {"name" : "Year", "type": "INTEGER","mode":"NULLABLE"},
            {"name" : "Month", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Time", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Total_Purchases", "type": "INTEGER","mode":"NULLABLE"},
            {"name" : "Amount", "type": "FLOAT","mode":"NULLABLE"},
            {"name" : "Total_Amount", "type": "FLOAT","mode":"NULLABLE"},
            {"name" : "Product_Category", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Product_Brand", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Product_Type", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Shipping_Method", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Payment_Method", "type": "STRING","mode":"NULLABLE"},
            {"name" : "Ratings", "type": "FLOAT","mode":"NULLABLE"},
            {"name" : "products", "type": "STRING","mode":"NULLABLE"},

        ]
    )

transfer_file    