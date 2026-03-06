import argparse
from lib.utils import get_spark
from lib.spark_transform import parse_message
from lib.schema import orders_schema
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date

    df_orders = spark \
        .read \
        .format("iceberg") \
        .load("bronze.orders") \
        .where(
            (F.col("ingest_date") >= F.lit(ingest_date)) &
            (F.col("ingest_date") < F.date_add(F.to_date(F.lit(ingest_date)), 1))
        )

    df_parsed = parse_message(df_orders, orders_schema)

    df_transformed = df_parsed \
        .filter(F.col("status").isin("COMPLETED", "CANCELED")) \
        .withColumn("created_at", F.to_timestamp(F.col("created_at") / 1000000)) \
        .withColumn("completed_at", F.to_timestamp(F.col("completed_at") / 1000000)) \
        .withColumn("canceled_at", F.to_timestamp(F.col("canceled_at") / 1000000)) \
        .withColumn("total_amount", F.col("total_amount").cast("decimal(12,0)")) \
        .drop_duplicates(subset=["order_id"]) \
        .withColumnRenamed("status", "status_name")

    df_orders_cleaned = df_transformed \
        .select(F.col("order_id"), 
                F.col("customer_id"),
                F.col("branch_id"),
                F.col("payment_id"),
                F.col("total_amount"),
                F.col("created_at"),
                F.col("completed_at"), 
                F.col("canceled_at"), 
                F.col("status_name"), 
                F.col("ingest_ts"), 
                F.col("ingest_date"))
                

    df_orders_cleaned.createOrReplaceTempView("stg_orders_cleaned")
    
    spark.sql("""
              MERGE INTO silver.orders_cleaned o
              USING stg_orders_cleaned s
              ON o.order_id = s.order_id
              WHEN NOT MATCHED THEN INSERT *
              """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_orders_cleaned")
    main(spark, args)