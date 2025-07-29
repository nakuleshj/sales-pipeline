from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    explode,
    when,
    sum,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
import faker

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

fake = faker.Faker()

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
    df_customer.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/salesdb"
    ).option("dbtable", "dim_customer").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    df_product.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/salesdb"
    ).option("dbtable", "dim_product").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    df_invoice.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/salesdb"
    ).option("dbtable", "dim_invoice").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    fact_batch.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/salesdb"
    ).option("dbtable", "fact_product_sales").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

df_customer = (
    df_parsed.select(
        col("customer_id").alias("customer_id"),
        col("country").alias("country"),
    )
    .distinct()
    .withColumn(
        "customer_name",
        fake.name()
    )
)

df_product = (
    df_parsed.select(
        explode(col("products")).alias("product"),
    )
    .select(
        col("product.product_id").alias("product_id"),
        col("product.description").alias("description"),
    )
)

df_invoice = (
    df_parsed.select(
        col("invoice_id").alias("invoice_id")
    )
    .withColumn(
        "sales_channel",
        when(col("country") == "United Kingdom", "In-Store").otherwise("Online")
    )
    .withColumn(
        "total_amount",
        sum(col("product.price") * col("product.quantity")).over(
            Window.partitionBy("invoice_id")
    )
))

df_fact_sales = df_parsed.select(
        col("invoice_id").alias("invoice_id"),
        col("customer_id").alias("customer_id"),
        col("country").alias("country"),
        explode(col("products")).alias("product"),
    ) \
    .select(
        col("invoice_id"),
        col("customer_id"),
        col("product.product_id").alias("product_id"),
        col("product.quantity").cast(IntegerType()).alias("quantity"),
        col("product.price").cast(DoubleType()).alias("price"),
    ) \
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    ) \
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    ) \
    .withColumn(
        "is_returned", 
        when(col("quantity") < 0,True).otherwise(False)).cast(BooleanType())


query = (
    df_fact_sales.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()