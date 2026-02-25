import argparse
from lib.utils import get_spark, get_jdbc_config
from lib.spark_loader import load_bronze_dim_table

def main(spark, config, params):
    ingest_date = params.ingest_date

    load_bronze_dim_table(spark, config, "customers", ingest_date)
    load_bronze_dim_table(spark, config, "customers", ingest_date)
    load_bronze_dim_table(spark, config, "customers", ingest_date)
    load_bronze_dim_table(spark, config, "customers", ingest_date)
    load_bronze_dim_table(spark, config, "customers", ingest_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date")
    args = parser.parse_args()

    try:
        config = get_jdbc_config()
        spark = get_spark()
        main(spark, config, args)
    finally:
        spark.stop()