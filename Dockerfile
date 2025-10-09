FROM apache/airflow:2.10.1

# Switch to root for system installs (Spark/Hadoop requires Java)
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER ${AIRFLOW_UID:-50000}

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt