from pyspark.sql.types import *

orders_schema = StructType([
    StructField('payload', StructType([
        StructField('__db', StringType(), True), 
        StructField('__op', StringType(), True), 
        StructField('__table', StringType(), True), 
        StructField('__ts_ms', LongType(), True),
        StructField('__source_ts_ms', LongType(), True),
        StructField('order_id', LongType(), True), 
        StructField('customer_id', LongType(), True), 
        StructField('branch_id', LongType(), True), 
        StructField('payment_id', LongType(), True), 
        StructField('total_amount', StringType(), True), 
        StructField('created_at', StringType(), True),
        StructField('completed_at', StringType(), True),
        StructField('canceled_at', StringType(), True),
        StructField('status', StringType(), True)]), True), 
    StructField('schema', StructType([
        StructField('fields', ArrayType(StructType([
            StructField('field', StringType(), True), 
            StructField('name', StringType(), True), 
            StructField('optional', BooleanType(), True), 
            StructField('parameters', StructType([
                StructField('connect.decimal.precision', StringType(), True), 
                StructField('scale', StringType(), True)]), True), 
            StructField('type', StringType(), True), 
            StructField('version', LongType(), True)]), True), True), 
        StructField('name', StringType(), True), 
        StructField('optional', BooleanType(), True), 
        StructField('type', StringType(), True)]), True)])


order_items_schema = StructType([
    StructField('payload', StructType([
        StructField('__db', StringType(), True), 
        StructField('__op', StringType(), True), 
        StructField('__table', StringType(), True), 
        StructField('__ts_ms', LongType(), True),
        StructField('__source_ts_ms', LongType(), True),
        StructField('order_id', LongType(), True), 
        StructField('order_items_id', LongType(), True), 
        StructField('product_id', LongType(), True), 
        StructField('quantity', LongType(), True), 
        StructField('unit_cost', StringType(), True), 
        StructField('unit_price', StringType(), True),
        StructField('created_at', StringType(), True)]), True), 
    StructField('schema', StructType([
        StructField('fields', ArrayType(StructType([
            StructField('field', StringType(), True), 
            StructField('name', StringType(), True), 
            StructField('optional', BooleanType(), True), 
            StructField('parameters', StructType([
                StructField('connect.decimal.precision', StringType(), True), 
                StructField('scale', StringType(), True)]), True), 
            StructField('type', StringType(), True), 
            StructField('version', LongType(), True)]), True), True), 
        StructField('name', StringType(), True), 
        StructField('optional', BooleanType(), True), 
        StructField('type', StringType(), True)]), True)])

