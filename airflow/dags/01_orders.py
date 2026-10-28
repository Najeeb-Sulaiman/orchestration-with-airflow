from airflow.sdk import DAG
from pendulum import DateTime
from airflow.providers.standard.operators.python import PythonOperator
from include.orders_util import extract, load, transform, notify


with DAG(
    dag_id="orders",
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

    transform_orders = PythonOperator(
                task_id="transform_orders",
                python_callable=transform
            )

    notify_orders = PythonOperator(
                    task_id="notify_orders",
                    python_callable=notify
                )

extract_orders >> load_orders >> transform_orders >> notify_orders
