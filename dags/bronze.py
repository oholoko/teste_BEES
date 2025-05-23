from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

# Define a ML function that will be executed as a PythonOperator task
def bronze_insert(**context):
    import boto3
    
    import requests
    from requests.adapters import HTTPAdapter

    s3_target = boto3.client('s3',
        endpoint_url='http://s3:9000',
        aws_access_key_id='minio123',
        aws_secret_access_key='minio123',
    )

    all_elements = requests.get('https://api.openbrewerydb.org/v1/breweries/meta',timeout=5).json()
    fail = []

    for brewery_page in range(0,int(int(all_elements['total'])/50)+1,1):
        try:
            temp = requests.get('https://api.openbrewerydb.org/v1/breweries?page={}'.format(brewery_page+1),timeout=5)
            for brewery in temp.json():
                s3_target.put_object(
                    Body=json.dumps(brewery),
                    Bucket='database',
                    Key='Bronze/{}'.format(brewery['id'])
                )
        except:
            fail.append(brewery_page)

    context["ti"].xcom_push('fail', fail)

def failure_pages(**context):
    import boto3
    
    import requests
    from requests.adapters import HTTPAdapter

    page_list = context['ti'].xcom_pull(key='fail')

    fail = []

    for brewery_page in page_list:
        try:
            temp = requests.get('https://api.openbrewerydb.org/v1/breweries?page={}'.format(brewery_page+1),timeout=5)
            for brewery in temp.json():
                s3_target.put_object(
                    Body=json.dumps(brewery),
                    Bucket='database',
                    Key='Bronze/{}'.format(brewery['id'])
                )
        except:
            fail.append(brewery_page)

    if len(fail) > 0:
        raise ValueError('Pages with error ({})'.format(fail))

def check_countries(**context):
    import pyspark
    from pyspark.sql import SparkSession,functions as F
    
    import requests

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

    temp = temp.withColumn('country',F.trim(F.col('country')))

    countries = temp\
        .groupBy("country")\
        .agg(F.count('id'))

    for each in countries.collect():
        all_elements = requests.get('https://api.openbrewerydb.org/v1/breweries/meta?by_country={}'.format(each[0]),timeout=5).json()
        if not(int(all_elements['total']) == each[1]):
            raise ValueError('country {} not filled properly'.format(each[0]))



# Define the DAG
with DAG(
    dag_id="Insert_Bronze",
    description="Insert in bronze",
    start_date=datetime(2023, 7, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    bronze_insert = PythonOperator(
        task_id="bronze_insert",
        python_callable=bronze_insert,
        provide_context=True  # Passes the context to the Python function
    )
    bronze_failures = PythonOperator(
        task_id="bronze_failures",
        python_callable=failure_pages,
        provide_context=True  # Passes the context to the Python function
    )
    bronze_check = PythonOperator(
        task_id="bronze_check",
        python_callable=check_countries,
        provide_context=True  # Passes the context to the Python function
    )
bronze_insert >> bronze_failures
bronze_failures >> bronze_check
