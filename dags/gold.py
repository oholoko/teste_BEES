from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

# Define a ML function that will be executed as a PythonOperator task
def machine_learning_task_3(**context):
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

    temp = spark.read.format('delta').load("s3a://database/Silver/")

    city_brewery = temp.groupBy('country','state','city','brewery_type')\
        .agg(
            F.count('id').alias('NumBreweries')
        )
    state_brewery = temp.groupBy('country','state','brewery_type')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('city',F.lit(None))
    country_brewery = temp.groupBy('country','brewery_type')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('city',F.lit(None))\
        .withColumn('state',F.lit(None))
    brewery = temp.groupBy('brewery_type')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('country',F.lit(None))\
        .withColumn('city',F.lit(None))\
        .withColumn('state',F.lit(None))
    city = temp.groupBy('country','state','city')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('brewery_type',F.lit(None))
    state = temp.groupBy('country','state')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('city',F.lit(None))\
        .withColumn('brewery_type',F.lit(None))
    country = temp.groupBy('country')\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('city',F.lit(None))\
        .withColumn('state',F.lit(None))\
        .withColumn('brewery_type',F.lit(None))
    total = temp.groupBy()\
        .agg(
            F.count('id').alias('NumBreweries')
        )\
        .withColumn('country',F.lit(None))\
        .withColumn('city',F.lit(None))\
        .withColumn('state',F.lit(None))\
        .withColumn('brewery_type',F.lit(None))

    city_brewery.union(
        state_brewery.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        country_brewery.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        state_brewery.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        brewery.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        city.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        country.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    ).union(
        total.select(
            'country','state','city','brewery_type','NumBreweries'
        )
    )\
    .write\
    .format('delta')\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save("s3a://database/Gold/AggregatedByCityAndBrewery_KPI_PRECALCULATED")

    city_brewery\
    .write\
    .format('delta')\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save("s3a://database/Gold/AggregatedByCityAndBrewery")


# Define the DAG
with DAG(
    dag_id="machine_learning_dag_3",
    description="A DAG for executing a machine learning",
    start_date=datetime(2023, 7, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task_python = PythonOperator(
        task_id="python_ML",
        python_callable=machine_learning_task_3,
        provide_context=True  # Passes the context to the Python function
    )


