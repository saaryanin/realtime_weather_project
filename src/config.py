# config.py in root/src
DATA_DIR = '/opt/airflow/data/'  # Mounted path in container
CSV_FILE = 'WeatherEvents_Jan2016-Dec2022.csv'
PARQUET_FILE = 'historical.parquet'
CSV_PATH = DATA_DIR + CSV_FILE
PARQUET_PATH = DATA_DIR + PARQUET_FILE