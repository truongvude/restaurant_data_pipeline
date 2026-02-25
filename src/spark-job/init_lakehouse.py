from pyspark.sql import functions as F
from lib.utils import get_spark, get_jdbc_config
from lib.ddl import bronze_schema_ddl, bronze_branches_ddl, bronze_categories_ddl, bronze_customers_ddl, bronze_payments_ddl, bronze_products_ddl, bronze_orders_ddl, bronze_order_items_ddl
from lib.spark_loader import load_bronze_dim_table

def main(spark, config):
    spark.sql("USE polaris;")
    spark.sql(bronze_schema_ddl)
    spark.sql(bronze_branches_ddl)
    spark.sql(bronze_categories_ddl)
    spark.sql(bronze_customers_ddl)
    spark.sql(bronze_payments_ddl)
    spark.sql(bronze_products_ddl)
    
    spark.sql(bronze_orders_ddl)
    spark.sql(bronze_order_items_ddl)
    
    load_bronze_dim_table(spark, config, "categories", F.current_timestamp())
    load_bronze_dim_table(spark, config, "branches", F.current_timestamp())
    load_bronze_dim_table(spark, config, "customers", F.current_timestamp())
    load_bronze_dim_table(spark, config, "payments", F.current_timestamp())
    load_bronze_dim_table(spark, config, "products", F.current_timestamp())

    print("Successfully init lakehouse!")  
if __name__ == "__main__":
    spark = get_spark("init_lakehouse")
    config = get_jdbc_config()
    try:
        main(spark, config)
    finally:
        spark.stop()