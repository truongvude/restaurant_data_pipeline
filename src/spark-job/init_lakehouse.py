from pyspark.sql import functions as F
from lib.utils import get_spark, get_jdbc_config
from lib.ddl import (bronze_schema_ddl, silver_schema_ddl, gold_schema_ddl,
                     bronze_branches_ddl, bronze_categories_ddl, 
                     bronze_customers_ddl, bronze_payments_ddl, bronze_products_ddl, 
                     bronze_orders_ddl, bronze_order_items_ddl,
                     silver_products_ddl, silver_branches_ddl,
                     silver_customers_ddl, silver_payments_ddl,
                     silver_order_items_ddl, silver_orders_ddl,
                     gold_dim_branches_ddl, gold_dim_customers_ddl,
                     gold_dim_payments_ddl, gold_dim_products_ddl,
                     gold_dim_date_ddl, gold_dim_order_status_ddl,
                     insert_dim_order_status_dml, gold_fact_order_line_ddl)

from lib.spark_loader import load_bronze_dim_table

def main(spark, config):
    spark.sql("USE polaris;")
    spark.sql(bronze_schema_ddl)
    spark.sql(silver_schema_ddl)
    spark.sql(gold_schema_ddl)
    spark.sql(bronze_branches_ddl)
    spark.sql(bronze_categories_ddl)
    spark.sql(bronze_customers_ddl)
    spark.sql(bronze_payments_ddl)
    spark.sql(bronze_products_ddl)
    spark.sql(bronze_orders_ddl)
    spark.sql(bronze_order_items_ddl)

    spark.sql(silver_products_ddl)
    spark.sql(silver_branches_ddl)
    spark.sql(silver_payments_ddl)
    spark.sql(silver_customers_ddl)
    spark.sql(silver_orders_ddl)
    spark.sql(silver_order_items_ddl)
    
    spark.sql(gold_dim_branches_ddl)
    spark.sql(gold_dim_customers_ddl)
    spark.sql(gold_dim_payments_ddl)
    spark.sql(gold_dim_products_ddl)
    spark.sql(gold_dim_order_status_ddl)
    spark.sql(gold_dim_date_ddl)
    spark.sql(gold_fact_order_line_ddl)

    spark.sql(insert_dim_order_status_dml)

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