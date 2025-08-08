from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nakulesh", 
    "start_date": datetime(2025, 1, 1)
    }

with DAG(
    dag_id="start_spark_consumer",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
) as dag:
    delay = BashOperator(
        task_id="wait",
        bash_command="sleep 3",
    )
    start_spark = BashOperator(
        task_id="start_spark_consumer",
        bash_command="""
      docker exec spark-master bash -lc '/opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
        --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.0,org.postgresql:postgresql:42.7.3 \
        /opt/spark-apps/stream_consumer.py'
    """
        )
    delay >> start_spark
