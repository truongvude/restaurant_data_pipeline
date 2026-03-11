import argparse
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib.utils import get_spark

def main(spark, params):
    ingest_date = params.ingest_date


    df_orders = spark \
        .read.format("iceberg") \
        .load("silver.orders_cleaned") \
        .where((F.col("created_at") >= F.lit(ingest_date)) &
            (F.col("created_at") < F.date_add(F.to_date(F.lit(ingest_date)), 1))) \
        .alias("ord")
    

    df_order_items = spark \
        .read.format("iceberg") \
        .load("silver.order_items_cleaned") \
        .where((F.col("created_at") >= F.lit(ingest_date)) &
            (F.col("created_at") < F.date_add(F.to_date(F.lit(ingest_date)), 1))) \
        .alias("ord_i")

    df_customers = spark.read.format("iceberg").load("gold.dim_customers").alias("cus")
    df_branches = spark.read.format("iceberg").load("gold.dim_branches").alias("br")
    df_products = spark.read.format("iceberg").load("gold.dim_products").alias("prd")
    df_payments = spark.read.format("iceberg").load("gold.dim_payments").alias("pm")
    df_order_status = spark.read.format("iceberg").load("gold.dim_order_status").alias("s")


    df_join = df_order_items \
        .withColumn("order_date_key", F.date_format(F.to_date("created_at"), "yyyyMMdd").cast("int")) \
        .join(df_orders, 
            on="order_id",
            how="inner") \
        .join(df_customers,
            (F.col("ord.customer_id") == F.col("cus.customer_id")) &
            (F.col("ord.created_at") >= F.col("cus.effective_from")) &
            (F.col("ord.created_at") < F.col("cus.effective_to")),
            "inner") \
        .join(df_branches,
            (F.col("ord.branch_id") == F.col("br.branch_id")) &
            (F.col("ord.created_at") >= F.col("br.effective_from")) &
            (F.col("ord.created_at") < F.col("br.effective_to")),
            "inner") \
        .join(df_products,
            (F.col("ord_i.product_id") == F.col("prd.product_id")) &
            (F.col("ord_i.created_at") >= F.col("prd.effective_from")) &
            (F.col("ord_i.created_at") < F.col("prd.effective_to")),
            "inner") \
        .join(df_payments,
            (F.col("ord.payment_id") == F.col("pm.payment_id")) &
            (F.col("ord.created_at") >= F.col("pm.effective_from")) &
            (F.col("ord.created_at") < F.col("pm.effective_to")),
            "inner") \
        .join(df_order_status,
            F.col("ord.status_name") == F.col("s.status_name"),
            "inner")


    fact_order_line = df_join \
        .select(F.col("order_date_key"),
                F.col("order_items_id"),
                F.col("order_id"),
                F.col("product_key"),
                F.col("customer_key"),
                F.col("branch_key"),
                F.col("payment_key"),
                F.col("quantity"),
                F.col("unit_cost"),
                F.col("unit_price"),
                F.col("ord.created_at"),
                F.col("completed_at"),
                F.col("canceled_at"),
                F.col("status_key"))

    fact_order_line.createOrReplaceTempView("stg_fact_order_line")
    spark.sql("""
              MERGE INTO gold.fact_order_line f
              USING stg_fact_order_line s
              ON f.order_items_id = s.order_items_id
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
              """)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()
    spark = get_spark("fact_order_line")
    main(spark, args)
    spark.stop()