FROM apache/airflow:3.0.6

ENV AIRFLOW_HOME=/opt/airflow
ENV HOME=/home/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN curl "https://download.oracle.com/java/17/archive/jdk-17.0.12_linux-x64_bin.tar.gz" -o "${AIRFLOW_HOME}/jdk-17.0.12_linux-x64_bin.tar.gz" \
    && tar -xzf jdk-17.0.12_linux-x64_bin.tar.gz \
    && rm jdk-17.0.12_linux-x64_bin.tar.gz

ENV JAVA_HOME="$AIRFLOW_HOME/jdk-17.0.12"
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN curl "https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz" -o "${AIRFLOW_HOME}/spark-3.5.3-bin-hadoop3.tgz" \
    && tar -xzf spark-3.5.3-bin-hadoop3.tgz \
    && rm spark-3.5.3-bin-hadoop3.tgz

ENV SPARK_HOME="$AIRFLOW_HOME/spark-3.5.3-bin-hadoop3"
ENV PATH="$SPARK_HOME/bin:$PATH"

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
# SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
# RUN apt-get update -qq && apt-get install unzip -y -qq && apt-get install -y python3-pip

# RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
#     && unzip awscliv2.zip \
#     && ./aws/install \
#     && aws --version \
#     && rm -rf aws*

RUN curl "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar" -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
RUN curl "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"
RUN curl "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.104.5/nessie-spark-extensions-3.5_2.12-0.104.5.jar" -o "${SPARK_HOME}/jars/nessie-spark-extensions-3.5_2.12-0.104.5.jar"
RUN curl "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar" -o "${SPARK_HOME}/jars/postgresql-42.7.2.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" -o "${SPARK_HOME}/jars/iceberg-aws-bundle-1.10.1.jar"


USER $AIRFLOW_UID

COPY requirements.txt .
RUN pip install --no-deps -r requirements.txt