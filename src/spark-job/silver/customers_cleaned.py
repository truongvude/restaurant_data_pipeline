import argparse
from lib.utils import get_spark
from lib.spark_loader import merge_table_with_scd2
from lib.spark_transform import transform_phone, masked_phone
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date

    df_customers = spark \
        .read \
        .format("iceberg") \
        .load("bronze.customers") \
        .where(
            (F.col("ingest_date") >= F.lit(ingest_date)) &
            (F.col("ingest_date") < F.date_add(F.to_date(F.lit(ingest_date)), 1))
        )

    df_customers_transformed_phone = transform_phone(df_customers, "phone")
    df_customers_masked_phone = masked_phone(df_customers_transformed_phone, "phone")
    df_customers_cleaned = df_customers_masked_phone \
        .dropna(subset=["customer_id"], how="any") \
        .drop_duplicates(subset=["customer_id"])
    
    df_customers_silver = df_customers_cleaned \
        .select(F.col("customer_id"),
                F.col("name"),
                F.col("birthday"),
                F.col("city"),
                F.col("phone"),
                F.col("customer_segment"),
                F.col("registration_date"),
                F.col("updated_at"),
                F.col("ingest_ts"),
                F.col("ingest_date"))
    
    merge_table_with_scd2(spark, df_customers_silver, "silver.customers_cleaned", "customer_id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_customers_cleaned")
    main(spark, args)
    spark.stop()