import os
import argparse
from pyspark.sql import functions as F
from lib.utils import get_spark
from lib.spark_loader import write_batch_to_iceberg

cp_location_base = "s3a://lakehouse/checkpoints/"

def main(spark, params):
    table = params.table
    kafka_bootstrap_servers = params.kafka_bootstrap_servers
    topic = params.topic

    table_cp = os.path.join(cp_location_base, table)


    df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .load()
        .selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
    )

    df_bronze = df_raw.withColumn("ingest_ts", 
                                  F.current_timestamp())
        
    query = df_bronze.writeStream \
        .foreachBatch(write_batch_to_iceberg(f"polaris.bronze.{table}")) \
        .trigger(processingTime="1 minutes") \
        .option("fanout-enabled", "false") \
        .option("checkpointLocation", table_cp) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table")
    parser.add_argument("--kafka_bootstrap_servers")
    parser.add_argument("--topic")
    args = parser.parse_args()

    try:
        spark = get_spark(f"{args.table}_consumer")
        main(spark, args)
    finally:
        spark.stop()