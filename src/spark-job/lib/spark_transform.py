from pyspark.sql import functions as F

def parse_message(df, schema):
    df_parsed = df \
    .select(F.from_json(F.col("value"), schema).alias("data"), "*") \
    .select("data.payload.*")

    return df_parsed

def transform_phone(df, phone_col):
    df_transformed = df \
    .withColumn(phone_col, 
                F.regexp_replace(phone_col, r"[\s()+-]", "")) \
    .withColumn(phone_col,
                F.regexp_replace(phone_col, "^84", "0"))

    return df_transformed

def masked_phone(df, phone_col):
    df_masked_phone = df \
        .withColumn(phone_col,
                    F.concat(F.substring(phone_col, 1, 3),
                             F.lit("****"),
                             F.expr(f"substring({phone_col}, length({phone_col})-2, 3)"))
                   )
    return df_masked_phone