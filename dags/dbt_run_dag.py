from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
}

with DAG('dbt_run_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         description='Run dbt models, tests, and snapshots from shell script'
) as dag:

    # Task 1: Run dbt models
    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_lab2_music && dbt run --profiles-dir=/home/airflow/.dbt --log-level=info > dbt_run_log.txt 2>&1',
    )

    # Task 2: Run dbt tests
    dbt_test = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt_lab2_music && dbt test --profiles-dir=/home/airflow/.dbt --log-level=info > dbt_test_log.txt 2>&1',
    )

    # Task 3: Run dbt snapshots
    dbt_snapshot = BashOperator(
        task_id='snapshot_dbt_models',
        bash_command='cd /opt/airflow/dbt_lab2_music && dbt snapshot --profiles-dir=/home/airflow/.dbt --log-level=info > dbt_snapshot_log.txt 2>&1',
    )

    # Chaining the tasks
    dbt_run >> dbt_test >> dbt_snapshot

