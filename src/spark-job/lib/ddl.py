bronze_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS bronze;"

bronze_customers_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.customers (
        customer_id LONG,
        name STRING,
        birthday DATE,
        city STRING,
        phone STRING,
        customer_segment STRING,
        registration_date TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (ingest_date)
    """

bronze_branches_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.branches (
        branch_id LONG,
        name STRING,
        city STRING,
        address STRING,
        phone STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (ingest_date)
    """

bronze_categories_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.categories (
        category_id LONG,
        name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (ingest_date)
    """

bronze_payments_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.payments (
        payment_id LONG,
        payment_type STRING,
        bank_name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (ingest_date)
    """

bronze_products_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.products (
        product_id LONG,
        product_name STRING,
        price DECIMAL(10,0),
        cost DECIMAL(10,0),
        category_id LONG,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (ingest_date)
    """

bronze_orders_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.orders (
        key STRING,
        value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP,
        ingest_ts TIMESTAMP,
        ingest_date DATE)
    PARTITIONED BY (ingest_date)
    """

bronze_order_items_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.order_items (
        key STRING,
        value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP,
        ingest_ts TIMESTAMP,
        ingest_date DATE)
    PARTITIONED BY (ingest_date)
    """
