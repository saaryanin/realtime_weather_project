from pyspark.sql import SparkSession
import sys
import boto3
import os

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("WeatherTransform").master("local[*]").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.filter(df["Precipitation(in)"].isNotNull()).select("State", "Precipitation(in)", "Severity")
    df.write.mode("overwrite").parquet(output_path)
    spark.stop()

    # AWS S3 upload directory (hands-on fix for Parquet dir)
    s3 = boto3.client('s3')
    bucket_name = 'weather-etl-bucket-yanin'  # Update to your bucket
    s3_prefix = 'data/historical.parquet/'  # S3 path

    for root, dirs, files in os.walk(output_path):
        for file in files:
            local_file = os.path.join(root, file)
            s3_key = s3_prefix + local_file[len(output_path)+1:]  # Strip base path
            s3.upload_file(local_file, bucket_name, s3_key)
    print("Uploaded Parquet directory to S3: " + bucket_name + "/" + s3_prefix)