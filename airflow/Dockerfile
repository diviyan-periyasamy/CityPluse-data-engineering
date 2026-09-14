FROM apache/airflow:2.9.3-python3.11

USER root

# Java is required for PySpark (local mode) to run inside this image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Postgres JDBC driver, used by the PySpark job to read/write via JDBC.
RUN mkdir -p /opt/airflow/jars && \
    curl -fL -o /opt/airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
