import pyarrow.parquet as pq

df = pq.read_table('C:\\Users\\yanin\\Projects\\realtime_weather_project\\data\\historical.parquet').to_pandas()
print(df.shape[0])  # Row count—expect ~8M if full data
print(df.head())  # Sample rows (State, Precipitation(in), Severity)