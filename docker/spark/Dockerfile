FROM eclipse-temurin:17-jre

ENV SPARK_VERSION=3.5.1 \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    FLOWIDS_HOME=/app
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    PYTHONPATH="/app:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

RUN apt-get update && apt-get install -y --no-install-recommends \
        wget curl python3 python3-pip tcpdump \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Spark-Cassandra connector (self-contained assembly jar)
RUN wget -q -P ${SPARK_HOME}/jars/ \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.0/spark-cassandra-connector-assembly_2.12-3.5.0.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY docker/spark/start-master.sh docker/spark/start-worker.sh /
RUN chmod +x /start-master.sh /start-worker.sh
CMD ["bash"]
