import argparse
from lib.utils import get_spark, get_jdbc_config
from lib.spark_loader import load_bronze_dim_table

def main(spark, config, params):
    ingest_ts = params.ingest_ts

    load_bronze_dim_table(spark, config, "categories", ingest_ts)
    load_bronze_dim_table(spark, config, "customers", ingest_ts)
    load_bronze_dim_table(spark, config, "branches", ingest_ts)
    load_bronze_dim_table(spark, config, "payments", ingest_ts)
    load_bronze_dim_table(spark, config, "products", ingest_ts)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_ts")
    args = parser.parse_args()

    try:
        config = get_jdbc_config()
        spark = get_spark()
        main(spark, config, args)
    finally:
        spark.stop()