bronze_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS bronze;"
silver_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS silver;"
gold_schema_ddl = "CREATE NAMESPACE IF NOT EXISTS gold;"

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
        ingest_ts TIMESTAMP,
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
        ingest_ts TIMESTAMP,
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
        ingest_ts TIMESTAMP,
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
        ingest_ts TIMESTAMP,
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
        ingest_ts TIMESTAMP,
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
        ingest_date DATE,
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
        ingest_date DATE,
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
        ingest_date DATE,
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
        ingest_date DATE,
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
        ingest_ts TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (days(created_at))
    """

silver_orders_ddl = """
    CREATE TABLE IF NOT EXISTS silver.orders_cleaned (
        order_id LONG,
        customer_id LONG,
        branch_id LONG,
        payment_id LONG,
        total_amount NUMERIC(12,0),
        created_at TIMESTAMP,
        completed_at TIMESTAMP,
        canceled_at TIMESTAMP,
        status_name STRING,
        ingest_ts TIMESTAMP,
        ingest_date DATE
        )
    PARTITIONED BY (days(created_at))
    """

gold_dim_customers_ddl = """
    CREATE TABLE IF NOT EXISTS gold.dim_customers (
        customer_key STRING,
        customer_id LONG,
        name STRING,
        city STRING,
        customer_segment STRING,
        registration_date TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        )
        """

gold_dim_branches_ddl = """
    CREATE TABLE IF NOT EXISTS gold.dim_branches (
        branch_key STRING,
        branch_id LONG,
        name STRING,
        city STRING,
        status STRING,
        created_at TIMESTAMP,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        )
        """

gold_dim_products_ddl = """
    CREATE TABLE IF NOT EXISTS gold.dim_products (
        product_key STRING,
        product_id LONG,
        product_name STRING,
        price DECIMAL(10,0),
        cost DECIMAL(10,0),
        category_name STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        )
        """

gold_dim_payments_ddl = """
    CREATE TABLE IF NOT EXISTS gold.dim_payments (
        payment_key STRING,
        payment_id LONG,
        payment_type STRING,
        bank_name STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        )
        """

gold_dim_order_status_ddl = """
    CREATE TABLE IF NOT EXISTS gold.dim_order_status (
        status_key LONG,
        status_name STRING,
        description STRING
        )
        """

gold_fact_order_line_ddl = """
    CREATE TABLE IF NOT EXISTS gold.fact_order_line (
        order_date_key INT,
        order_items_id LONG,
        order_id LONG,
        product_key STRING,
        customer_key STRING,
        branch_key STRING,
        payment_key STRING,
        quantity INT,
        unit_cost NUMERIC(10,0),
        unit_price NUMERIC(10,0),
        created_at TIMESTAMP,
        completed_at TIMESTAMP,
        canceled_at TIMESTAMP,
        status_key LONG)
    PARTITIONED BY (days(created_at))
    """

insert_dim_order_status_dml = """
    INSERT INTO 
        gold.dim_order_status (status_key, status_name, description)
    VALUES (1, 'PENDING', 'Order created but not processed'),
            (2, 'COMPLETED', 'Order completed'),
            (3, 'CANCELED', 'Order created but canceled')
    """


gold_dim_date_ddl = """
    CREATE OR REPLACE TABLE gold.dim_date AS
    SELECT
        CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
        calendar_date AS date_full,
        YEAR(calendar_date) AS year,
        MONTH(calendar_date) AS month,
        DAYOFMONTH(calendar_date) AS day,
        DATE_FORMAT(calendar_date, 'EEEE') AS day_name,
        CASE WHEN DAYOFWEEK(calendar_date) IN (1,7) THEN 0 ELSE 1 END AS is_weekday
    FROM (
        SELECT EXPLODE(SEQUENCE(DATE '2000-01-01', DATE '2099-12-31', INTERVAL 1 DAY)) AS calendar_date
        ) AS dates;
        """