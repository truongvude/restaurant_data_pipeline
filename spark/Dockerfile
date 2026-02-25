FROM bitnamilegacy/spark:3.5.3

USER root

RUN apt-get update -qq && apt-get install curl -qq

RUN curl "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" -o "${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" -o "${SPARK_HOME}/jars/kafka-clients-3.4.1.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -o "${SPARK_HOME}/jars/commons-pool2-2.11.1.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" -o "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar" -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar" -o "${SPARK_HOME}/jars/hadoop-common-3.3.4.jar"
RUN curl "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"
RUN curl "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar" -o "${SPARK_HOME}/jars/postgresql-42.7.2.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" -o "${SPARK_HOME}/jars/iceberg-aws-bundle-1.10.1.jar"