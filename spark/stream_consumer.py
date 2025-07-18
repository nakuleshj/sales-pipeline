from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *

invoice_schema = StructType(
    [
        StructField("invoice_id", StringType()),
        StructField("customer_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("country",StringType())
        #Add products info
    ]
)

spark=SparkSession.builder \
    .appName("SalesStreamConsumer") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "latest") \
    .load()

df_str = raw_df.selectExpr("CAST(value AS STRING)")

query = df_str.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate","false") \
    .start()

query.awaitTermination()
