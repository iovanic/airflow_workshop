"""
## ETL example Dag

This Dag is a first easy example
"""

from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="0 * * * *",
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 3,
        "retry_delay": duration(seconds=5)
    },
    tags=["example", "etl"],
    is_paused_upon_creation=True,
)
def example_dag_pipeline():
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")

    extract_data = HttpOperator(
        task_id="extract_data",
        http_conn_id="http_wttr",
        endpoint="Madrid?format=3",
        method="GET",
        log_response=True
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command="echo 'Data ok'"
    )

    @task_group(group_id="transform_data")
    def transform_data():
        @task
        def drop_na():
            return "Drop Null values"
        @task
        def chage_data_types():
            return "Data types updated"

        drop_na() >> chage_data_types()

    @task
    def load_warehouse():
        return "Data loaded"

    @task
    def gernerate_report():
        print("Proceso terminado")

    # Dependencies
    start >> extract_data >> validate_data >> [transform_data(), load_warehouse()] >> gernerate_report() >> stop


# Instantiate the Dag
example_dag_pipeline()
