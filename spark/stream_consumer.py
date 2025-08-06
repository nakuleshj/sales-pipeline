from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    explode,
    when,
    round
)
from pyspark.sql.types import *

main_schema = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("invoice_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("country", StringType()),
        StructField(
            "products",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", StringType()),
                        StructField("description", StringType()),
                        StructField("quantity", IntegerType()),
                        StructField("price", DoubleType()),
                    ]
                )
            ),
        ),
    ]
)

spark = (
    SparkSession.builder.appName("SalesStreamConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
        "org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales")
    .option("startingOffsets", "latest")
    .load()
)

df_str = raw_df.selectExpr("CAST(value AS STRING)")

df_parsed = df_str.select(from_json(col("value"), main_schema).alias("data")).select(
    "data.*"
)


def write_to_postgres(fact_batch, batch_id):

    df_raw_transactions = fact_batch.select(
            col("invoice_id").alias("invoice_id"),
            col("customer_id").alias("customer_id"),
            col("country").alias("country"),
            col("timestamp").alias("timestamp"),
            explode(col("products")).alias("product"),
        ) \
        .select(
            col("invoice_id"),
            col("customer_id"),
            col("country"),
            col("timestamp"),
            col("product.product_id").alias("product_id"),
            col("product.description").alias("description"),
            col("product.quantity").alias("quantity"),
            col("product.price").alias("price"),
        ) \
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "MM-dd-yyyy HH:mm:ss")
        ) \
        .withColumn(
            "total",
            round(col("quantity") * col("price"),2)
        ) \
        .withColumn(
            "is_returned", 
            when(col("quantity") < 0,True).otherwise(False)) \
        .withColumn(
            "sales_channel",
            when(col("country") == "United Kingdom", "In-Store").otherwise("Online")
        ) \
    # Add customer names to each customer_id
    df_raw_transactions.show()

    df_raw_transactions.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/salesdb"
    ).option("dbtable", "raw_transactions").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

query = (
    df_parsed.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()