from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib.utils import get_spark

def main(spark):
    df_branches = spark.read.format("iceberg").load("silver.branches_cleaned")
    dim_branches = df_branches \
        .withColumn("branch_key", 
                    F.md5(F.concat_ws("|", 
                                    F.col("branch_id"), 
                                    F.col("effective_from")))) \
        .select(F.col("branch_key"),
                F.col("branch_id"),
                F.col("name"),
                F.col("city"),
                F.col("status"),
                F.col("created_at"),
                F.col("effective_from"),
                F.col("effective_to"),
                F.col("is_current"))


    dim_branches.writeTo("gold.dim_branches").overwritePartitions()
    

if __name__ == "__main__":
    spark = get_spark("dim_branches")
    main(spark)
    spark.stop()