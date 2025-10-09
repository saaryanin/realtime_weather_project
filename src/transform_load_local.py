from pyspark.sql import SparkSession
import sys
import boto3
import os

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("WeatherTransform").master("local[*]").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.select("State", "Precipitation(in)", "Severity")  # Keep all for batch
    df.write.mode("overwrite").parquet(output_path)
    spark.stop()

    # AWS S3 upload directory (clear prefix first, then upload only .parquet files)
    s3 = boto3.client('s3')
    bucket_name = 'weather-etl-bucket-yanin'  # Your bucket
    s3_prefix = 'data/historical.parquet/'  # S3 path

    # Clear existing objects in the prefix to avoid accumulation
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    if 'Contents' in response:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in response['Contents']]}
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    print(f"Cleared S3 prefix: {bucket_name}/{s3_prefix}")

    # Upload only .parquet files
    for root, dirs, files in os.walk(output_path):  # Loop over directory
        for file in files:
            if not file.endswith('.parquet'):
                continue  # Skip _SUCCESS and any non-Parquet
            local_file = os.path.join(root, file)
            s3_key = s3_prefix + local_file[len(output_path)+1:]  # Strip base
            s3.upload_file(local_file, bucket_name, s3_key)
    print("Uploaded Parquet files to S3: " + bucket_name + "/" + s3_prefix)