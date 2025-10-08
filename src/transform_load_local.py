from pyspark.sql import SparkSession
import sys
import os
import boto3  # Prep for AWS

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("WeatherTransform").master("local[*]").getOrCreate()
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df = df.filter(df["Temperature(F)"].isNotNull()).select("State", "Temperature(F)", "Severity")
        df.write.mode("overwrite").parquet(output_path)
        print(f"Transformed to {output_path}")
    except Exception as e:
        print(f"Spark error: {e}")
    finally:
        spark.stop()