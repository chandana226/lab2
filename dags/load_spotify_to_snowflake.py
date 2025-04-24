import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def load_csv_to_snowflake():
    # Load CSV file
    file_path = "/opt/airflow/dags/data/spotify_full_list_20102023.csv"
    df = pd.read_csv(file_path)

    # Clean up columns
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Expected columns and rename
    expected_columns = [
        "artist_and_title", "artist", "streams", "daily", "year",
        "main_genre", "genres", "first_genre", "second_genre", "third_genre"
    ]
    df = df[expected_columns]
    df = df.rename(columns={"daily": "daily_streams", "artist_and_title": "artist_title"})

    df = df.where(pd.notnull(df), None)

    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        #  Start transaction
        cursor.execute("BEGIN")

        # cursor.execute("DELETE FROM RAW.TOP_STREAMED_SONGS WHERE DATE_COLUMN = CURRENT_DATE()")

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO RAW.TOP_STREAMED_SONGS (
                    artist_title, artist, streams, daily_streams, year,
                    main_genre, genres, first_genre, second_genre, third_genre
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        print(" Data successfully inserted into Snowflake.")

    except Exception as e:
        print(" Error occurred:", e)
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

