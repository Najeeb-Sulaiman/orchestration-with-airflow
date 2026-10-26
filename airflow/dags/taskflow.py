from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2026, 10, 26), schedule=None)
def dag1():

    @task()
    def task1(**context):
        var = "This is an xcom"
        context["task_instance"].xcom_push(key="var_id", value=var)

    @task()
    def task2(**context):
        var = context["task_instance"].xcom_pull(task_ids="task1", key="var_id")
        print(var)

    task1() >> task2()


dag1()
