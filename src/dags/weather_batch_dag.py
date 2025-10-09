from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
import os

# Hardcoded paths (no config import needed)
CSV_PATH = '/opt/airflow/data/WeatherEvents_Jan2016-Dec2022.csv'
PARQUET_PATH = '/opt/airflow/data/historical.parquet'


def extract_kaggle_data(input_path, output_path):
    api = KaggleApi()
    api.authenticate()  # Uses env vars from docker-compose
    api.dataset_download_files('sobhanmoosavi/us-weather-events', path='/opt/airflow/data', unzip=True)
    # Rename unzipped file (dataset unzips to WeatherEvents_Jan2016-Dec2022.csv)
    os.rename('/opt/airflow/data/WeatherEvents_Jan2016-Dec2022.csv', input_path)
    print(f"Extracted to {input_path}. Next: Transform to {output_path}")


dag = DAG('weather_batch', start_date=datetime(2025, 9, 21), schedule_interval='@daily')

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_kaggle_data,
    op_kwargs={'input_path': CSV_PATH, 'output_path': PARQUET_PATH},
    dag=dag
)

transform = SparkSubmitOperator(
    task_id='transform',
    application='/opt/airflow/src/transform_load_local.py',
    conn_id='spark_default',  # Your tested connection
    application_args=[CSV_PATH, PARQUET_PATH],
    dag=dag
)

load_sql = SQLExecuteQueryOperator(
    task_id='load_sql',
    conn_id='snowflake_default',
    sql="""
    COPY INTO WEATHER_DB.PUBLIC.weather_table 
    FROM @weather_stage/data/historical.parquet 
    FILE_FORMAT=(TYPE=PARQUET) 
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

    CREATE OR REPLACE VIEW aggregated AS 
    SELECT "State", AVG("Precipitation(in)") AS avg_precip, 
           CASE WHEN avg_precip > 0.5 THEN 'High' ELSE 'Normal' END AS anomaly 
    FROM weather_table GROUP BY "State";
    """,
    dag=dag
)

extract >> transform >> load_sql