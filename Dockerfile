FROM apache/airflow:2.10.1-python3.10

USER root
RUN apt-get update && apt-get install -y git

USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.9.4 \
    dbt-snowflake==1.9.1 \

