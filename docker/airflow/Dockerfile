# Build Airflow on top of Spark so the scheduler can launch local Spark jobs
# without needing a separate custom runtime image per task.
FROM apache/spark:3.5.8-python3 AS spark

FROM apache/airflow:3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Reuse the Spark distribution from the builder stage instead of installing it
# separately inside the Airflow image.
COPY --from=spark /opt/spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="/home/airflow/.local/bin:/opt/spark/bin:/opt/spark/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV HOME=/tmp

# The stock Airflow launcher script is replaced so `docker compose exec airflow`
# behaves predictably in this image.
RUN printf '#!/bin/sh\nexec python -m airflow "$@"\n' > /usr/local/bin/airflow \
    && chmod +x /usr/local/bin/airflow

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-snowflake \
    kafka-python \
    pyspark==3.5.8

WORKDIR /opt/project

COPY --chown=airflow:0 pyproject.toml README.md /opt/project/
COPY --chown=airflow:0 historical /opt/project/historical
COPY --chown=airflow:0 ml_model /opt/project/ml_model
COPY --chown=airflow:0 poker_platform /opt/project/poker_platform
COPY --chown=airflow:0 simulation /opt/project/simulation
COPY --chown=airflow:0 sql /opt/project/sql
COPY --chown=airflow:0 sample_data /opt/project/sample_data
COPY --chown=airflow:0 jobs /opt/project/jobs

USER root
RUN pip install --no-cache-dir -e /opt/project[snowflake,kafka]
USER airflow
