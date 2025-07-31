from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    explode,
    when,
    sum,
    udf
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

    df_customer = (
    fact_batch.select(
        col("customer_id").alias("customer_id"),
        col("country").alias("country"),
    )
    .distinct()
    .withColumn(
        "customer_name",
        fake_name()
    )
    )

    df_product = (
    fact_batch.select(
        explode(col("products")).alias("product"),
    )
    .select(
        col("product.product_id").alias("product_id"),
        col("product.description").alias("description"),
    )
    )

    df_invoice = (
        fact_batch.select(
            col("invoice_id").alias("invoice_id"),
            col("country").alias("country")
        )
        .withColumn(
            "sales_channel",
            when(col("country") == "United Kingdom", "In-Store").otherwise("Online")
        ).select(
            col("invoice_id"),
            col("sales_channel")
    ))

    df_fact_sales = fact_batch.select(
            col("invoice_id").alias("invoice_id"),
            col("customer_id").alias("customer_id"),
            col("country").alias("country"),
            col("timestamp").alias("timestamp"),
            explode(col("products")).alias("product"),
        ) \
        .select(
            col("invoice_id"),
            col("customer_id"),
            col("product.product_id").alias("product_id"),
            col("product.quantity").cast(IntegerType()).alias("quantity"),
            col("product.price").cast(DoubleType()).alias("price"),
            col("timestamp").alias("timestamp"),
        ) \
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "MM-dd-yyyy HH:mm:ss")
        ) \
        .withColumn(
            "total_amount",
            col("quantity") * col("price")
        ) \
        .withColumn(
            "is_returned", 
            when(col("quantity") < 0,True).otherwise(False))

    df_customer.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/salesdb"
    ).option("dbtable", "dim_customer").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    df_product.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/salesdb"
    ).option("dbtable", "dim_product").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    df_invoice.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/salesdb"
    ).option("dbtable", "dim_invoice").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    df_fact_sales.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/salesdb"
    ).option("dbtable", "fact_product_sales").option("user", "retail_admin").option(
        "password", "retail123"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

@udf(StringType())
def fake_name():
    return fake.name()

query = (
    df_parsed.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()