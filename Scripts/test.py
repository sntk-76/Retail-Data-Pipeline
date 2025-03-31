from pyspark.sql import SparkSession
import os

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/sinatavakoli2022/Retail-Data-Pipeline/Authentication/retail-data-pipeline-453817-5c7165d921e7.json'

spark = SparkSession.builder \
    .appName("Retail Data Transformation") \
    .getOrCreate()

# Read from BigQuery
df = spark.read.format("bigquery") \
    .option("table", "retail-data-pipeline-453817.retail_data_pipeline_dataset.retail_raw_data") \
    .load()

# Do transformations here
df_clean = df.dropna(subset=["Transaction_ID", "Customer_ID", "Date"])

# Write back to BigQuery
df_clean.write.format("bigquery") \
    .option("table", "retail-data-pipeline-453817.transformed_retail_data_pipeline_dataset.retail_clean_data") \
    .option('temporaryGcsBucket','retail-data-pipeline-453817-bucket')\
    .mode("overwrite") \
    .save()
