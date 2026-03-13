from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib.utils import get_spark

def main(spark):
    df_payments = spark.read.format("iceberg").load("silver.payments_cleaned")

    dim_payments = df_payments \
    .withColumn("payment_key", 
                F.md5(F.concat_ws("|", 
                                  F.col("payment_id"), 
                                  F.col("effective_from")))) \
    .select(F.col("payment_key"),
            F.col("payment_id"),
            F.col("payment_type"),
            F.col("bank_name"),
            F.col("effective_from"),
            F.col("effective_to"),
            F.col("is_current"))


    dim_payments.writeTo("gold.dim_payments").overwritePartitions()
    

if __name__ == "__main__":
    spark = get_spark("dim_payments")
    main(spark)
    spark.stop()