import argparse
from lib.utils import get_spark
from lib.spark_loader import merge_table_with_scd2
from lib.spark_transform import transform_phone
from pyspark.sql import functions as F

def main(spark, params):
    ingest_date = params.ingest_date

    df_branches = spark \
        .read \
        .format("iceberg") \
        .load("bronze.branches") \
        .where(
            (F.col("ingest_date") >= F.lit(ingest_date)) &
            (F.col("ingest_date") < F.date_add(F.to_date(F.lit(ingest_date)), 1))
        )

    df_branches_transformed_phone = transform_phone(df_branches, "phone")
    df_branches_cleaned = df_branches_transformed_phone \
        .drop_duplicates(subset=["branch_id"])

    df_branches_silver = df_branches_cleaned \
        .select(F.col("branch_id"),
                F.col("name"),
                F.col("city"),
                F.col("address"),
                F.col("phone"),
                F.col("status"),
                F.col("created_at"),
                F.col("updated_at"),
                F.col("ingest_ts"),
                F.col("ingest_date"))
    
    merge_table_with_scd2(spark, df_branches_silver, "silver.branches_cleaned", "branch_id")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    spark = get_spark("silver_branches_cleaned")
    main(spark, args)
    spark.stop()