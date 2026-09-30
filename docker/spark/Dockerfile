FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=600 --retries=10 \
    pyspark==4.0.0 \
    delta-spark==4.0.0

RUN mkdir -p /opt/spark-jars \
    && curl -fSL -o /opt/spark-jars/delta-spark.jar https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar \
    && curl -fSL -o /opt/spark-jars/delta-storage.jar https://repo.maven.apache.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar \
    && curl -fSL -o /opt/spark-jars/antlr4-runtime.jar https://repo.maven.apache.org/maven2/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar \
    && curl -fSL -o /opt/spark-jars/postgresql.jar https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar

WORKDIR /opt/project
