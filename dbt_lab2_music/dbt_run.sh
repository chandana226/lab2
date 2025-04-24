#!/bin/bash

# Just run dbt inside container environment (no virtualenv)
cd /opt/airflow/dbt_lab2_music
dbt run

