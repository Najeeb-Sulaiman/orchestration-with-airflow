from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


with DAG(
    dag_id="first_dag",
    start_date=datetime(2026, 10, 21),
    schedule=None
):

    first_task = BashOperator(
        task_id="first_task",
        bash_command="echo 'This is our first Airflow task'"
    )

    def message():
        print("This is our second task")

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=message
    )

    first_task >> second_task
