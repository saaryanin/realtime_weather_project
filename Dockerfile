FROM apache/airflow:2.10.1

# Install providers and libs (pinned, no requirements.txt to avoid conflicts)
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon==9.13.0 \
    apache-airflow-providers-snowflake==6.5.3 \
    apache-airflow-providers-apache-spark==5.3.2 \
    apache-airflow-providers-apache-kafka==1.10.3 \
    pyspark==4.0.1 \
    snowflake-connector-python==3.17.3 \
    boto3==1.40.35 \
    pandas==2.3.2 \
    pyarrow==17.0.0