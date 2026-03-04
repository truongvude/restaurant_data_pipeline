import argparse
from lib.utils import get_spark
from lib.spark_loader import merge_table_with_scd2
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date


    df_products = spark \
        .read \
        .format("iceberg") \
        .load("bronze.products") \
        .where(
            (F.col("ingest_ts") >= F.to_timestamp(F.lit(ingest_date))) &
            (F.col("ingest_ts") < F.to_timestamp(F.date_add(F.to_date(F.lit(ingest_date)), 1)))
        )


    df_categories = spark \
        .read \
        .format("iceberg") \
        .load("bronze.categories") \
        .where(
            (F.col("ingest_ts") >= F.to_timestamp(F.lit(ingest_date))) &
            (F.col("ingest_ts") < F.to_timestamp(F.date_add(F.to_date(F.lit(ingest_date)), 1)))
        )
    


    df_join = df_products \
        .join(df_categories, 
              df_products.category_id == df_categories.category_id,
              how="left") \
        .select(df_products.product_id,
                df_products.product_name,
                df_products.price,
                df_products.cost,
                df_categories.name.alias("category_name"),
                df_products.created_at,
                df_products.updated_at,
                df_products.ingest_ts)

    df_cleaned = df_join \
        .drop_duplicates(subset=["product_id"])
    
    merge_table_with_scd2(spark, df_cleaned, "silver.products_cleaned", "product_id")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_products_cleaned")
    main(spark, args)