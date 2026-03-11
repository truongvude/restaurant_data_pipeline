from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib.utils import get_spark

def main(spark):
    df_products = spark.read.format("iceberg").load("silver.products_cleaned")
    dim_products = df_products \
        .withColumn("product_key", 
                    F.md5(F.concat_ws("|", 
                                    F.col("product_id"), 
                                    F.col("effective_from")))) \
        .select(F.col("product_key"),
                F.col("product_id"),
                F.col("product_name"),
                F.col("price"),
                F.col("cost"),
                F.col("category_name"),
                F.col("effective_from"),
                F.col("effective_to"),
                F.col("is_current"))

    dim_products.writeTo("gold.dim_products").overwritePartitions()

if __name__ == "__main__":
    spark = get_spark("dim_products")
    main(spark)
    spark.stop()