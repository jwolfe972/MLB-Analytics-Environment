from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

# Function to create Spark session using Airflow conn_id
def create_spark_session_from_airflow(conn_id="pyspark-conn"):
    # Get connection info from Airflow
    connection = BaseHook.get_connection(conn_id)

    spark_master_url = f"{connection.host}:{connection.port}"
    
    # Use connection details to create a Spark session
    spark = SparkSession.builder \
        .appName("PitchDataProcessing") \
        .master(spark_master_url)  # Assuming the host is Spark master URL
    
    # Add other Spark configurations from connection details if available
    for key, value in connection.extra_dejson.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

# Example Airflow DAG
dag = DAG(
    dag_id="spark_example_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

# Use the create_spark_session_from_airflow function in your DAG task
def my_spark_task():
    spark = create_spark_session_from_airflow(conn_id="pyspark-conn")
    # You can now use the spark session for your transformations
    #full_pitch_by_pitch_spark = spark.createDataFrame(full_pitch_by_pitch)
    # Perform transformations here

    df = spark.createDataFrame(
    [
        (1, "John Doe", 21),
        (2, "Jane Doe", 22),
        (3, "Joe Bloggs", 23),
    ],
    ["id", "name", "age"],
    )
    df.show()

    print("Spark session is ready and processing data...")

    return df.toPandas()

# Operator to trigger the task in Airflow
task = PythonOperator(
    task_id="run_spark_processing",
    python_callable=my_spark_task,
    dag=dag,
)

task
