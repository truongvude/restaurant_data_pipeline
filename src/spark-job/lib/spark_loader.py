def load_bronze_dim_table(spark, config, table_name, ingest_date):
    df_table = spark.read \
        .format("jdbc") \
        .option("url", config["url"]) \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .load()
    df_bronze = df_table \
        .withColumn("ingest_date", ingest_date)
    df_bronze.writeTo(f"bronze.{table_name}").overwritePartitions()

def write_batch_to_iceberg(table_name):
    def write_batch(batch_df, batch_id):
        (
            batch_df
            .writeTo(table_name) \
            .append()
        )
    return write_batch


