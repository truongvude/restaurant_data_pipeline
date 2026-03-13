from lib.utils import get_spark, get_jdbc_config
from lib.spark_loader import load_bronze_dim_table
from pyspark.sql import functions as F

def main(spark, config):
    load_bronze_dim_table(spark, config, "categories", F.current_timestamp())
    load_bronze_dim_table(spark, config, "customers", F.current_timestamp())
    load_bronze_dim_table(spark, config, "branches", F.current_timestamp())
    load_bronze_dim_table(spark, config, "payments", F.current_timestamp())
    load_bronze_dim_table(spark, config, "products", F.current_timestamp())

if __name__ == "__main__":

    try:
        config = get_jdbc_config()
        spark = get_spark("bronze_dimension")
        main(spark, config)
    finally:
        spark.stop()