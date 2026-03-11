from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib.utils import get_spark

def main(spark):
    df_customers = spark.read.format("iceberg").load("silver.customers_cleaned")
    dim_customers = df_customers \
        .withColumn("customer_key", 
                    F.md5(F.concat_ws("|", 
                                    F.col("customer_id"), 
                                    F.col("effective_from")))) \
        .select(F.col("customer_key"),
                F.col("customer_id"),
                F.col("name"),
                F.col("city"),
                F.col("customer_segment"),
                F.col("registration_date"),
                F.col("effective_from"),
                F.col("effective_to"),
                F.col("is_current"))


    dim_customers.writeTo("gold.dim_customers").overwritePartitions()
    

if __name__ == "__main__":
    spark = get_spark("dim_customers")
    main(spark)
    spark.stop()