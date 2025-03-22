# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    dag_id='play-dag-example'
)
def my_dag():
    
    # submit_job = SparkSubmitOperator(
    #     task_id="submit_job",
    #     conn_id="my_spark_conn",
    #     application="include/scripts/read.py",
    #     verbose=True,
    # )
    
    # submit_job
    
    @task.pyspark(conn_id="pyspark-conn")
    def read_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df = spark.createDataFrame(
            [
                (1, "John Doe", 21),
                (2, "Jane Doe", 22),
                (3, "Joe Bloggs", 23),
            ],
            ["id", "name", "age"],
        )
        df.show()

        return df.toPandas()
    
    read_data()

my_dag()