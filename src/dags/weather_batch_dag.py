from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import sys
sys.path.insert(0, '/opt/airflow/src')
from config import CSV_PATH, PARQUET_PATH  # Import from your config.py
from extract_batch import extract_kaggle_data  # Your extract function

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
    dag=dag
)

load_sql = SnowflakeOperator(
    task_id='load_sql',
    snowflake_conn_id='snowflake_default',
    sql="""
    COPY INTO WEATHER_DB.PUBLIC.weather_table FROM @weather_stage/data/historical.parquet FILE_FORMAT=(TYPE=PARQUET);
    CREATE OR REPLACE VIEW aggregated AS 
    SELECT "State", AVG("Temperature(F)") AS avg_temp, 
           CASE WHEN avg_temp > 90 THEN 'High' ELSE 'Normal' END AS anomaly 
    FROM weather_table GROUP BY "State";
    """,
    dag=dag
)

extract >> transform >> load_sql