from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import logging
import os
import snowflake.connector

# Enhance the load_csv_to_snowflake function with robust error handling
def load_csv_to_snowflake(**kwargs):
    """
    Load Spotify streaming data from CSV to Snowflake with idempotent processing.
    Implements comprehensive error handling and transaction management.
    """
    # Get execution date from context or use current date
    execution_date = kwargs.get('execution_date', datetime.now())
    formatted_date = execution_date.strftime('%Y-%m-%d')
    
    # Initialize Snowflake hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    try:
        # Establish connection and cursor
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        # Begin transaction
        cursor.execute("BEGIN")
        
        try:
            # Check if data for this date already exists in tracking table
            check_query = f"""
                SELECT COUNT(*) FROM SPOTIFY_LOAD_TRACKER 
                WHERE LOAD_DATE = '{formatted_date}' AND STATUS = 'SUCCESS'
            """
            
            try:
                cursor.execute(check_query)
                result = cursor.fetchone()
                if result[0] > 0:
                    logging.info(f"Data for {formatted_date} already processed successfully. Skipping.")
                    cursor.execute("ROLLBACK")
                    return
            except Exception as e:
                # If tracking table doesn't exist, create it
                logging.info(f"Tracking table check failed: {str(e)}. Creating table if needed.")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS SPOTIFY_LOAD_TRACKER (
                        LOAD_ID NUMBER AUTOINCREMENT PRIMARY KEY,
                        LOAD_DATE DATE,
                        RECORD_COUNT NUMBER,
                        STATUS VARCHAR(50),
                        ERROR_MESSAGE VARCHAR(1000),
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """)
            
            # Get CSV file path from Airflow Variables
            csv_path = Variable.get("spotify_csv_path", default_var="/path/to/spotify_data.csv")
            logging.info(f"Processing CSV file: {csv_path}")
            
            try:
                # Read and process CSV data
                df = pd.read_csv(csv_path)
                logging.info(f"Successfully read {len(df)} records from CSV")
                
                # Clean & transform data
                df = df.fillna({
                    'year': 0,
                    'main_genre': 'Unknown',
                    'first_genre': 'Unknown',
                    'second_genre': 'Unknown',
                    'third_genre': 'Unknown'
                })
                
                # Create a temporary file for staging
                temp_file = f"/tmp/spotify_data_{formatted_date}.csv"
                df.to_csv(temp_file, index=False)
                
                # Ensure stage exists
                cursor.execute("""
                    CREATE STAGE IF NOT EXISTS SPOTIFY_STAGE
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                """)
                
                # Upload to stage
                put_command = f"PUT file://{temp_file} @SPOTIFY_STAGE OVERWRITE=TRUE"
                snowflake_hook.run(put_command, autocommit=False)
                
                # Clear existing data for idempotency
                cursor.execute(f"""
                    DELETE FROM raw.spotify_streams 
                    WHERE LOAD_DATE = '{formatted_date}'
                """)
                
                # Load data from stage to table
                copy_command = f"""
                    COPY INTO raw.spotify_streams (
                        artist_and_title, artist, streams, daily, year,
                        main_genre, genres, first_genre, second_genre, third_genre,
                        load_date
                    )
                    FROM (
                        SELECT 
                            t.$1, t.$2, t.$3, t.$4, t.$5, 
                            t.$6, t.$7, t.$8, t.$9, t.$10,
                            '{formatted_date}'
                        FROM @SPOTIFY_STAGE/{os.path.basename(temp_file)} t
                    )
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                    ON_ERROR = 'CONTINUE'
                """
                cursor.execute(copy_command)
                
                # Get count of loaded records
                cursor.execute("SELECT COUNT(*) FROM raw.spotify_streams WHERE load_date = %(date)s",
                               {"date": formatted_date})
                load_count = cursor.fetchone()[0]
                
                # Record successful load in tracking table
                cursor.execute(f"""
                    INSERT INTO SPOTIFY_LOAD_TRACKER (LOAD_DATE, RECORD_COUNT, STATUS)
                    VALUES ('{formatted_date}', {load_count}, 'SUCCESS')
                """)
                
                # Commit transaction
                cursor.execute("COMMIT")
                logging.info(f"Successfully loaded {load_count} records for {formatted_date}")
                
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                
            except pd.errors.EmptyDataError as e:
                logging.error(f"CSV file is empty: {str(e)}")
                cursor.execute(f"""
                    INSERT INTO SPOTIFY_LOAD_TRACKER (LOAD_DATE, RECORD_COUNT, STATUS, ERROR_MESSAGE)
                    VALUES ('{formatted_date}', 0, 'ERROR', 'Empty CSV file')
                """)
                cursor.execute("COMMIT")  # Commit the error status
                raise AirflowException(f"CSV file is empty: {str(e)}")
                
            except Exception as e:
                logging.error(f"Error processing CSV: {str(e)}")
                cursor.execute("ROLLBACK")
                # Record failure
                cursor.execute(f"""
                    INSERT INTO SPOTIFY_LOAD_TRACKER (LOAD_DATE, RECORD_COUNT, STATUS, ERROR_MESSAGE)
                    VALUES ('{formatted_date}', 0, 'ERROR', '{str(e).replace("'", "''")}')
                """)
                cursor.execute("COMMIT")  # Commit the error status
                raise AirflowException(f"Failed to process CSV: {str(e)}")
                
        except snowflake.connector.errors.ProgrammingError as e:
            error_message = str(e)
            logging.error(f"Snowflake SQL error: {error_message}")
            
            # Rollback the main transaction
            try:
                cursor.execute("ROLLBACK")
            except:
                pass
                
            # Record error details in a separate transaction
            try:
                conn.cursor().execute(f"""
                    INSERT INTO SPOTIFY_LOAD_TRACKER (LOAD_DATE, RECORD_COUNT, STATUS, ERROR_MESSAGE)
                    VALUES ('{formatted_date}', 0, 'ERROR', '{error_message.replace("'", "''")}')
                """)
                conn.cursor().execute("COMMIT")
            except:
                logging.error("Failed to record error in tracking table")
                
            raise AirflowException(f"Snowflake SQL error: {error_message}")
            
    except Exception as e:
        logging.error(f"Connection error: {str(e)}")
        raise AirflowException(f"Failed to connect to Snowflake: {str(e)}")
        
    finally:
        # Always clean up resources
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1
}

with DAG('spotify_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Task 1: Enhanced load CSV to Snowflake with robust error handling
    load_task = PythonOperator(
        task_id='load_spotify_csv_to_snowflake',
        python_callable=load_csv_to_snowflake,
        provide_context=True
    )

    # Task 2: Idempotent transformation using SQL with enhanced error handling
    transform_task = SnowflakeOperator(
        task_id='transform_spotify_data',
        sql="""
        BEGIN;
        
        -- Track transformation execution
        CREATE TABLE IF NOT EXISTS analytics.TRANSFORM_TRACKER (
            TRANSFORM_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            TRANSFORM_DATE DATE,
            ROWS_PROCESSED NUMBER,
            STATUS VARCHAR(50),
            ERROR_MESSAGE VARCHAR(1000),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        
        -- Check if transformation already ran successfully for this date
        DECLARE
          success_count NUMBER;
        BEGIN
          SELECT COUNT(*) INTO :success_count 
          FROM analytics.TRANSFORM_TRACKER 
          WHERE TRANSFORM_DATE = '{{ ds }}' AND STATUS = 'SUCCESS';
          
          IF (:success_count > 0) THEN
            -- Transformation already ran successfully
            RETURN 'Transformation already completed for {{ ds }}';
          END IF;
        END;

        -- Remove existing data for today's run
        DELETE FROM analytics.spotify_summary
        WHERE run_date = '{{ ds }}';

        -- Insert aggregated stream data
        INSERT INTO analytics.spotify_summary (song_name, total_streams, run_date)
        SELECT song_name, SUM(streams) as total_streams, '{{ ds }}'
        FROM raw.spotify_streams
        GROUP BY song_name;
        
        -- Track successful execution
        INSERT INTO analytics.TRANSFORM_TRACKER (TRANSFORM_DATE, ROWS_PROCESSED, STATUS)
        SELECT '{{ ds }}', COUNT(*), 'SUCCESS'
        FROM analytics.spotify_summary
        WHERE run_date = '{{ ds }}';

        COMMIT;
        
        EXCEPTION
          WHEN OTHER THEN
            ROLLBACK;
            
            -- Log the error
            INSERT INTO analytics.TRANSFORM_TRACKER (
                TRANSFORM_DATE, ROWS_PROCESSED, STATUS, ERROR_MESSAGE
            )
            VALUES (
                '{{ ds }}', 0, 'ERROR', SQLSTATE || ': ' || SQLERRM
            );
            
            COMMIT;  -- Commit just the error logging
            RAISE;  -- Re-throw the exception for Airflow to catch
        """,
        snowflake_conn_id='snowflake_conn',
        warehouse='COBRA_QUERY_WH',
        database='lab2_db',
        schema='RAW',
        role='TRAINING_ROLE',
        autocommit=False
    )

    load_task >> transform_task
