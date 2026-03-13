from pyspark.sql import functions as F
def load_bronze_dim_table(spark, config, table_name, ingest_ts):
    df_table = spark.read \
        .format("jdbc") \
        .option("url", config["url"]) \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .load()
    df_bronze = df_table \
        .withColumn("ingest_ts", ingest_ts) \
        .withColumn("ingest_date", F.to_date(F.from_utc_timestamp("ingest_ts", "Asia/Ho_Chi_Minh")))
    df_bronze.writeTo(f"bronze.{table_name}").overwritePartitions()

def write_batch_to_iceberg(table_name):
    def write_batch(batch_df, batch_id):
        (
            batch_df
            .writeTo(table_name) \
            .append()
        )
    return write_batch

def merge_table_with_scd2(spark, source_table, target_table, primary_key):
    source_table_cols = source_table.columns
    target_table_cols = source_table_cols + ["effective_from", "effective_to", "is_current"]
    target_schema = ", ".join(f"{col}" for col in target_table_cols)
    target_values = ", ".join(f"s.{col}" for col in source_table_cols) + ", s.updated_at, DATE '9999-12-31', true"
    
    source_table.createOrReplaceTempView("stg_source_table")
    spark.sql(f"""
        WITH table_updates AS (
        SELECT s.*
        FROM stg_source_table s
        JOIN {target_table} t
        ON s.{primary_key} = t.{primary_key}
        WHERE s.updated_at > t.updated_at
        AND t.is_current = true
        )
        MERGE INTO 
            {target_table} t
        USING 
            (
            SELECT 
                {primary_key} AS join_key, 
                * FROM stg_source_table
            UNION ALL
            SELECT 
                NULL AS join_key,
                * FROM table_updates 
            ) s
        ON 
            t.{primary_key} = s.join_key
            
        WHEN MATCHED AND is_current = true AND s.updated_at > t.updated_at
        THEN UPDATE SET is_current = false, effective_to = s.updated_at
    
        WHEN NOT MATCHED 
        THEN INSERT ({target_schema}) 
        VALUES ({target_values})
        
        WHEN NOT MATCHED BY SOURCE 
        THEN UPDATE SET is_current = false AND effective_to = current_timestamp()
        """)