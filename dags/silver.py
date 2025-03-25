from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

# Define a ML function that will be executed as a PythonOperator task
def machine_learning_task_2(**context):
    import pyspark
    from pyspark.sql import SparkSession,functions as F


    spark = SparkSession.builder \
        .appName("CSV File to Delta Lake Table") \
        .master("spark://spark:7077") \
        .config("spark.sql.adaptive.enabled","true") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key","minio123") \
        .config("spark.hadoop.fs.s3a.secret.key","minio123") \
        .config("spark.hadoop.fs.s3a.endpoint","http://s3:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access","true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    temp = spark.read.json("s3a://database/Bronze/")

    temp = (
        temp\
        .withColumn('latitude',F.col('latitude').cast('Double'))\
        .withColumn('longitude',F.col('longitude').cast('Double'))\
        .withColumn('country',F.trim(F.col('country')))\
        .withColumn('state',F.trim(F.col('state')))\
        .withColumn('city',F.trim(F.col('city')))\
    )

    temp.write4
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .partitionBy('country','state','city')\
    .save("s3a://database/Silver/")



# Define the DAG
with DAG(
    dag_id="machine_learning_dag_2",
    description="A DAG for executing a machine learning",
    start_date=datetime(2023, 7, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task_python = PythonOperator(
        task_id="python_ML",
        python_callable=machine_learning_task_2,
        provide_context=True  # Passes the context to the Python function
    )


