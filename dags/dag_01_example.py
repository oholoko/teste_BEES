from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a ML function that will be executed as a PythonOperator task
def machine_learning_task(**context):
    # libraries for machine learning
    import pyspark
    from pyspark.sql import SQLContext
    conf = pyspark.SparkConf().setAppName('MyApp').setMaster('spark://spark:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = SQLContext(sc)
    df = spark.createDataFrame(
        [
            (1, "foo"),  # create your data here, be consistent in the types.
            (2, "bar"),
        ],
        ["id", "label"]  # add your column names here
    )
    print(df.collect())
    import boto3

    s3_target = boto3.client('s3',
        endpoint_url='http://s3:9000',
        aws_access_key_id='GDcSKv55DKrPiguvdT3F',
        aws_secret_access_key='IwYRCnJXeaW2g2NEaTXUo91m8h1gIrw2y1BOeBZS',
    )
    print(s3_target.list_buckets())



# Define the DAG
with DAG(
    dag_id="machine_learning_dag",
    description="A DAG for executing a machine learning",
    start_date=datetime(2023, 7, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task_python = PythonOperator(
        task_id="python_ML",
        python_callable=machine_learning_task,
        provide_context=True  # Passes the context to the Python function
    )


