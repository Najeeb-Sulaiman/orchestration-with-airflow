from airflow.sdk import DAG
from pendulum import DateTime
from airflow.providers.standard.operators.python import PythonOperator
from include.orders_util import extract, load, notify
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="orders_sql",
    start_date=DateTime(2026, 10, 23),
    schedule=None
):

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract
    )

    load_orders = PythonOperator(
            task_id="load_orders",
            python_callable=load
        )

    transform_orders = SQLExecuteQueryOperator(
                task_id="transform_orders",
                sql=[
                    "DROP TABLE IF EXISTS sales;",
                    """
                    CREATE TABLE sales AS
                    SELECT DISTINCT
                    order_id,
                    CAST(order_date AS TIMESTAMP)          AS order_date,
                    COALESCE(customer_name, 'Unknown')     AS customer_name,
                    COALESCE(product, 'Unknown')           AS product,
                    quantity,
                    price,
                    COALESCE(region, 'Unknown'),
                    (quantity * price)                       AS total_sale
                FROM raw_sales
                    """
                ],
                conn_id="supabse_conn"        
            )

    notify_orders = PythonOperator(
                    task_id="notify_orders",
                    python_callable=notify
                )

extract_orders >> load_orders >> transform_orders >> notify_orders
