from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # Add this import for filtering
import sys
import boto3
import os

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("WeatherTransform").master("local[*]").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    cities = ['New York', 'Los Angeles', 'Chicago', 'Philadelphia', 'Houston', 'Phoenix', 'Washington']
    df = df.filter(col("City").isin(cities))  # Filter to only your 7 cities (reduces data volume)
    df.write.mode("overwrite").parquet(output_path)  # Keep all columns
    spark.stop()

    # S3 upload (unchanged)
    s3 = boto3.client('s3')
    bucket_name = 'weather-etl-bucket-yanin'
    s3_prefix = 'data/historical.parquet/'

    # Clear prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    if 'Contents' in response:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in response['Contents']]}
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    print(f"Cleared S3 prefix: {bucket_name}/{s3_prefix}")

    # Upload .parquet only
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if not file.endswith('.parquet'): continue
            local_file = os.path.join(root, file)
            s3_key = s3_prefix + local_file[len(output_path)+1:]
            s3.upload_file(local_file, bucket_name, s3_key)
    print("Uploaded Parquet files to S3: " + bucket_name + "/" + s3_prefix)