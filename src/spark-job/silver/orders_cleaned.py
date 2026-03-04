import argparse
from lib.utils import get_spark
from lib.spark_transform import parse_message
from lib.schema import order_items_schema
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date

    df_order_items = spark \
        .read \
        .format("iceberg") \
        .load("bronze.order_items") \
        .where(
            (F.col("ingest_ts") >= F.to_timestamp(F.lit(ingest_date))) &
            (F.col("ingest_ts") < F.to_timestamp(F.date_add(F.to_date(F.lit(ingest_date)), 1)))
        )

    df_parsed = parse_message(df_order_items, order_items_schema)

    df_transformed = df_parsed \
        .withColumn("created_at", F.to_timestamp(F.col("created_at") / 1000000)) \
        .withColumn("unit_cost", F.col("unit_cost").cast("decimal(10,0)")) \
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,0)")) \
        .drop_duplicates(subset=["order_items_id"])

    df_silver = df_transformed \
        .select(F.col("order_id"), 
                F.col("order_items_id"),
                F.col("product_id"),
                F.col("quantity"),
                F.col("unit_cost"),
                F.col("unit_price"),
                F.col("created_at")) \
        .withColumn("ingest_ts", F.current_timestamp())

    df_silver.createOrReplaceTempView("stg_order_items_cleaned")
    
    spark.sql("""
              MERGE INTO polaris.silver.order_items_cleaned o
              USING stg_order_items_cleaned s
              ON o.order_items_id = s.order_items_id
              WHEN NOT MATCHED THEN INSERT *
              """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_order_items_cleaned")
    main(spark, args)