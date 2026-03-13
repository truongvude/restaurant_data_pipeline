import argparse
from lib.utils import get_spark
from lib.spark_loader import merge_table_with_scd2
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date

    df_payments = spark \
        .read \
        .format("iceberg") \
        .load("bronze.payments") \
        .where(
            (F.col("ingest_date") >= F.lit(ingest_date)) &
            (F.col("ingest_date") < F.date_add(F.to_date(F.lit(ingest_date)), 1))
        )
    
    df_payments_cleaned = df_payments \
        .drop_duplicates(subset=["payment_type", "bank_name"])
    

    df_payments_silver = df_payments_cleaned \
        .select(F.col("payment_id"),
                F.col("payment_type"),
                F.col("bank_name"),
                F.col("created_at"),
                F.col("updated_at"),
                F.col("ingest_ts"),
                F.col("ingest_date"))

    merge_table_with_scd2(spark, df_payments_silver, "silver.payments_cleaned", "payment_id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_payments_cleaned")
    main(spark, args)
    spark.stop()