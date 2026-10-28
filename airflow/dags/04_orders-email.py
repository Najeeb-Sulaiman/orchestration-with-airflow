from airflow.sdk import DAG, Variable
from pendulum import DateTime
from airflow.providers.standard.operators.python import PythonOperator
from include.orders_email import extract, load, notify, check_data
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

# environment = Variable.get("environment", default="dev")

with DAG(
    dag_id="orders_email",
    start_date=DateTime(2026, 10, 23),
    schedule=None
):

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract
    )

    check_extract = BranchPythonOperator(
        task_id="check_extract",
        python_callable=check_data
    )

    load_orders = PythonOperator(
            task_id="load_orders",
            python_callable=load
        )

    transform_orders = SQLExecuteQueryOperator(
                task_id="transform_orders",
                sql="sql/orders.sql",
                conn_id="supabse_conn"
            )

    generate_report = PythonOperator(
                    task_id="generate_report",
                    python_callable=notify
                )

    send_report = EmailOperator(
        task_id="send_report",
        to=["nihikah175@hiringview.com", "nmajuuche@gmail.com", "olashileayomi23@gmail.com"],
        subject="CDEBite's Daily Orders Report for {{ ds }}",
        html_content="""
        <p>Hi Amara,</p>
        <p>The orders report is now ready. Find the summary report below:</p>
        <p>
        <b>{{ ti.xcom_pull(task_ids="generate_report", key="report") | replace("\n", "<br>") }}</b>
        </p>
        <p>This report is generated from the {{ var.value.get("environment") }} environment</p>
        <p>
        Best Regards <br>
        Najeeb Sulaiman
        </p>
        """,
        conn_id="email_conn"

    )

    alert_no_record = EmailOperator(
            task_id="alert_no_record",
            to=["nihikah175@hiringview.com", "nmajuuche@gmail.com", "olashileayomi23@gmail.com"],
            subject="CDEBite's Daily Orders Report for {{ ds }}",
            html_content="""
            <p>Hi Amara,</p>
            <p>There is no report for the date: {{ ds }}</p>
            <p>This report is generated from the {{ var.value.get("environment") }} environment</p>
            <p>
            Best Regards <br>
            Najeeb Sulaiman
            </p>
            """,
            conn_id="email_conn"
    
        )

# extract_orders >> load_orders >> transform_orders >> generate_report >> send_report
extract_orders >> check_extract >> [load_orders, alert_no_record]
load_orders >> transform_orders >> generate_report >> send_report