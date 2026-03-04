bronze_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS bronze;"
silver_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS silver;"

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
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(ingest_ts))
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
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(ingest_ts))
    """

bronze_categories_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.categories (
        category_id LONG,
        name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(ingest_ts))
    """

bronze_payments_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.payments (
        payment_id LONG,
        payment_type STRING,
        bank_name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(ingest_ts))
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
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(ingest_ts))
    """

bronze_orders_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.orders (
        key STRING,
        value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP,
        ingest_ts TIMESTAMP)
    PARTITIONED BY (days(ingest_ts))
    """

bronze_order_items_ddl = """
    CREATE TABLE IF NOT EXISTS bronze.order_items (
        key STRING,
        value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP,
        ingest_ts TIMESTAMP)
    PARTITIONED BY (days(ingest_ts))
    """

silver_branches_ddl = """
    CREATE TABLE IF NOT EXISTS silver.branches_cleaned (
        branch_id LONG,
        name STRING,
        city STRING,
        address STRING,
        phone STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN)
        """

silver_products_ddl = """
    CREATE TABLE IF NOT EXISTS silver.products_cleaned (
        product_id LONG,
        product_name STRING,
        price NUMERIC(10,0),
        cost NUMERIC(10,0),
        category_name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN)
        """

silver_customers_ddl = """
    CREATE TABLE IF NOT EXISTS silver.customers_cleaned (
        customer_id LONG,
        name STRING,
        birthday DATE,
        city STRING,
        phone STRING,
        customer_segment STRING,
        registration_date TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN)
        """

silver_payments_ddl = """
    CREATE TABLE IF NOT EXISTS silver.payments_cleaned (
        payment_id LONG,
        payment_type STRING,
        bank_name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN)
        """

silver_order_items_ddl = """
    CREATE TABLE IF NOT EXISTS silver.order_items_cleaned (
        order_id LONG,
        order_items_id LONG,
        product_id LONG,
        quantity INTEGER,
        unit_cost NUMERIC(10,0),
        unit_price NUMERIC(10,0),
        created_at TIMESTAMP,
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(created_at))
    """

silver_orders_ddl = """
    CREATE TABLE IF NOT EXISTS silver.orders_cleaned (
        order_id LONG,
        customer_id LONG,
        branch_id LONG,
        total_amount NUMERIC(12,0),
        created_at TIMESTAMP,
        completed_at TIMESTAMP,
        canceled_at TIMESTAMP,
        status STRING,
        ingest_ts TIMESTAMP
        )
    PARTITIONED BY (days(created_at))
    """