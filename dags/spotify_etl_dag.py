from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from load_spotify_to_snowflake import load_csv_to_snowflake

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1
}

with DAG('spotify_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Task 1: Load CSV to Snowflake
    load_task = PythonOperator(
        task_id='load_spotify_csv_to_snowflake',
        python_callable=load_csv_to_snowflake
    )

    # Task 2: Idempotent transformation using SQL
    transform_task = SnowflakeOperator(
        task_id='transform_spotify_data',
        sql="""
        BEGIN;

        -- Remove existing data for today's run
        DELETE FROM analytics.spotify_summary
        WHERE run_date = '{{ ds }}';

        -- Insert aggregated stream data
        INSERT INTO analytics.spotify_summary (song_name, total_streams, run_date)
        SELECT song_name, SUM(streams) as total_streams, '{{ ds }}'
        FROM raw.spotify_streams
        GROUP BY song_name;

        COMMIT;
        """,
        snowflake_conn_id='snowflake_conn',
        warehouse='COBRA_QUERY_WH',
        database='lab2_db',
        schema='RAW',
        role='TRAINING_ROLE',       
        autocommit=False
    )

    load_task >> transform_task

