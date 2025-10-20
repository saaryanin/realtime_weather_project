from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import matplotlib.pyplot as plt
import pandas as pd
import boto3

# Hardcoded paths
CSV_PATH = '/opt/airflow/data/WeatherEvents_Jan2016-Dec2022.csv'
PARQUET_PATH = '/opt/airflow/data/historical.parquet'

def extract_kaggle_data(input_path, output_path):
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('sobhanmoosavi/us-weather-events', path='/opt/airflow/data', unzip=True)
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
    conn_id='spark_default',
    application_args=[CSV_PATH, PARQUET_PATH],
    dag=dag
)

load_sql = SQLExecuteQueryOperator(
    task_id='load_sql',
    conn_id='snowflake_default',
    sql="""
    CREATE OR REPLACE TABLE WEATHER_DB.PUBLIC.weather_table (
        "EventId" STRING,
        "Type" STRING,
        "Severity" STRING,
        "StartTime(UTC)" TIMESTAMP,
        "EndTime(UTC)" TIMESTAMP,
        "Precipitation(in)" FLOAT,
        "TimeZone" STRING,
        "AirportCode" STRING,
        "LocationLat" FLOAT,
        "LocationLng" FLOAT,
        "City" STRING,
        "County" STRING,
        "State" STRING,
        "ZipCode" STRING
    );

    COPY INTO WEATHER_DB.PUBLIC.weather_table 
    FROM @weather_stage/historical.parquet/  
    FILE_FORMAT=(TYPE=PARQUET) 
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    PATTERN='.*\\.parquet';

    CREATE OR REPLACE VIEW aggregated AS 
    SELECT "State", AVG("Precipitation(in)") AS avg_precip, 
           CASE WHEN avg_precip > 0.5 THEN 'High' ELSE 'Normal' END AS anomaly 
    FROM weather_table GROUP BY "State";
    """,
    dag=dag
)

def analyze_rain_severity(**context):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    query = """
    SELECT "Severity", COUNT(*) AS "count"
    FROM weather_table
    WHERE "Type" = 'Rain' AND "City" = 'Los Angeles'
    GROUP BY "Severity"
    ORDER BY "count" DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Plot with matplotlib
    plt.figure(figsize=(8, 6))
    plt.bar(df['Severity'], df['count'])
    plt.title('Count of Rain Events by Severity in Los Angeles')
    plt.xlabel('Severity')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_path = '/opt/airflow/data/rain_severity_la.png'
    plt.savefig(plot_path)

    # Upload to S3 (for Flask API access)
    s3 = boto3.client('s3')
    s3.upload_file(plot_path, 'weather-etl-bucket-yanin', 'plots/rain_severity_la.png')
    print("Graph uploaded to S3")

def analyze_rain_frequency_trends(**context):
    cities = ['New York', 'Los Angeles', 'Chicago', 'Philadelphia', 'Houston', 'Phoenix', 'Washington']
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    placeholders = ', '.join(['%s'] * len(cities))  # For parameterized query (ELT best practice)
    query = f"""
    SELECT "City", YEAR("StartTime(UTC)") AS "year", COUNT(*) AS "count"
    FROM weather_table
    WHERE "Type" = 'Rain' AND "City" IN ({placeholders})
    GROUP BY "City", "year"
    ORDER BY "City", "year";
    """
    df = hook.get_pandas_df(query, parameters=cities)

    # Pivot for multi-line plot
    pivot_df = df.pivot(index='year', columns='City', values='count').fillna(0)

    # Plot with matplotlib (all cities on one graph, different colors)
    plt.figure(figsize=(10, 6))
    pivot_df.plot(kind='line', marker='o')
    plt.title('Rain Event Frequency Trends by City (2016-2022)')
    plt.xlabel('Year')
    plt.ylabel('Number of Rain Events')
    plt.legend(title='City', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plot_path = '/opt/airflow/data/rain_frequency_trends.png'
    plt.savefig(plot_path)

    # Upload to S3 (for Flask API access)
    s3 = boto3.client('s3')
    s3.upload_file(plot_path, 'weather-etl-bucket-yanin', 'plots/rain_frequency_trends.png')
    print("Rain frequency trends graph uploaded to S3")

analyze_rain = PythonOperator(
    task_id='analyze_rain_severity',
    python_callable=analyze_rain_severity,
    dag=dag
)

analyze_frequency_trends = PythonOperator(
    task_id='analyze_rain_frequency_trends',
    python_callable=analyze_rain_frequency_trends,
    dag=dag
)

extract >> transform >> load_sql >> [analyze_rain, analyze_frequency_trends]