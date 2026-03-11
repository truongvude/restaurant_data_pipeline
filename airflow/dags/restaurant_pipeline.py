import pendulum
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import task_group

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
SPARK_JOB_FOLDER = os.path.join(AIRFLOW_HOME, "spark-job")
PY_FILE_LOCATION = os.path.join(SPARK_JOB_FOLDER, "lib.zip")
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"

local_tz = pendulum.timezone("Asia/Bangkok")

dag = DAG(
    dag_id="test_dag1",
    start_date=pendulum.datetime(2026, 3, 10, tz=local_tz),
    schedule=None,
    max_active_runs=1,
    catchup=False
)


with dag:
    submit_bronze_dim_task = SparkSubmitOperator(
        task_id="submit_bronze_dim_task",
        application=f"{SPARK_JOB_FOLDER}/bronze/bronze_dimension.py",
        conn_id="spark_default",
        py_files=PY_FILE_LOCATION,
        deploy_mode="client",
        executor_cores=1,
        total_executor_cores=1
    )

    @task_group
    def silver_dim_group():
        submit_silver_branches_task = SparkSubmitOperator(
            task_id="submit_silver_branches_task",
            application=f"{SPARK_JOB_FOLDER}/silver/branches_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )

        submit_silver_customers_task = SparkSubmitOperator(
            task_id="submit_silver_customers_task",
            application=f"{SPARK_JOB_FOLDER}/silver/customers_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )

        submit_silver_products_task = SparkSubmitOperator(
            task_id="submit_silver_products_task",
            application=f"{SPARK_JOB_FOLDER}/silver/products_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )

        submit_silver_payments_task = SparkSubmitOperator(
            task_id="submit_silver_payments_task",
            application=f"{SPARK_JOB_FOLDER}/silver/payments_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )


    @task_group
    def silver_fact_group():
        submit_silver_orders_task = SparkSubmitOperator(
            task_id="submit_silver_orders_task",
            application=f"{SPARK_JOB_FOLDER}/silver/orders_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )

        submit_silver_order_items_task = SparkSubmitOperator(
            task_id="submit_silver_order_items_task",
            application=f"{SPARK_JOB_FOLDER}/silver/order_items_cleaned.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
            executor_cores=1,
            total_executor_cores=1
        )

    @task_group
    def gold_dim_group():
        submit_gold_dim_branches_task = SparkSubmitOperator(
            task_id="submit_gold_dim_branches_task",
            application=f"{SPARK_JOB_FOLDER}/gold/dim_branches.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            executor_cores=1,
            total_executor_cores=1
        )

        submit_gold_dim_customers_task = SparkSubmitOperator(
            task_id="submit_gold_dim_customers_task",
            application=f"{SPARK_JOB_FOLDER}/gold/dim_customers.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            executor_cores=1,
            total_executor_cores=1
        )


        submit_gold_dim_payments_task = SparkSubmitOperator(
            task_id="submit_gold_dim_payments_task",
            application=f"{SPARK_JOB_FOLDER}/gold/dim_payments.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            executor_cores=1,
            total_executor_cores=1
        )


        submit_gold_dim_products_task = SparkSubmitOperator(
            task_id="submit_gold_dim_products_task",
            application=f"{SPARK_JOB_FOLDER}/gold/dim_products.py",
            conn_id="spark_default",
            py_files=PY_FILE_LOCATION,
            deploy_mode="client",
            executor_cores=1,
            total_executor_cores=1
        )

    submit_gold_fact_task = SparkSubmitOperator(
        task_id="submit_gold_fact_task",
        application=f"{SPARK_JOB_FOLDER}/gold/fact_order_line.py",
        conn_id="spark_default",
        py_files=PY_FILE_LOCATION,
        application_args=["--ingest_date", "{{ logical_date.strftime('%Y-%m-%d') }}"],
        deploy_mode="client",
        executor_cores=1,
        total_executor_cores=1
    )

    silver_dim = silver_dim_group()
    silver_fact = silver_fact_group()
    gold_dim = gold_dim_group()
    submit_bronze_dim_task >> silver_dim >> silver_fact >> gold_dim >> submit_gold_fact_task