from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_DIR = "/opt/airflow/dbt"

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='run_dbt_pipeline',
    schedule_interval=timedelta(minutes=1),
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir /opt/airflow/.dbt --target dev"
    )

    """
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_DIR} && dbt test"
    )
    """

    dbt_run