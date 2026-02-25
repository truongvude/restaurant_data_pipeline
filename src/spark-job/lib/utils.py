import os
from pyspark.sql import SparkSession

def get_spark(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS")) \
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST")) \
        .config("spark.sql.extensions",os.getenv("SPARK_SQL_EXTENSIONS")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("AWS_PATH_STYLE_ACCESS")) \
        .config("spark.sql.catalog.polaris", os.getenv("SPARK_SQL_CATALOG")) \
        .config("spark.sql.catalog.polaris.type", os.getenv("SPARK_SQL_CATALOG_TYPE")) \
        .config("spark.sql.catalog.polaris.uri", os.getenv("SPARK_SQL_CATALOG_URI")) \
        .config("spark.sql.catalog.polaris.token-refrest-enabled", os.getenv("SPARK_SQL_CATALOG_TOKEN_REFREST_ENABLED")) \
        .config("spark.sql.catalog.polaris.warehouse", os.getenv("SPARK_SQL_CATALOG_WAREHOUSE")) \
        .config("spark.sql.catalog.polaris.scope","PRINCIPAL_ROLE:ALL") \
        .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation","vended-credentials") \
        .config("spark.sql.catalog.polaris.credential", os.getenv("POLARIS_CREDENTIAL")) \
        .config("spark.sql.catalog.polaris.client.region","irrelevant") \
        .getOrCreate()

    return spark

def get_jdbc_config():
    return {
        "url": f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }

