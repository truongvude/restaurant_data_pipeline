FROM quay.io/debezium/connect:3.4

RUN curl "https://packages.confluent.io/maven/io/confluent/common-config/7.5.0/common-config-7.5.0.jar" -o "${KAFKA_HOME}/libs/common-config-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/common-utils/7.5.0/common-utils-7.5.0.jar" -o "${KAFKA_HOME}/libs/common-utils-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.5.0/kafka-avro-serializer-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-avro-serializer-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.5.0/kafka-connect-avro-converter-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-connect-avro-converter-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.5.0/kafka-connect-avro-data-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-connect-avro-data-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.5.0/kafka-schema-serializer-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-schema-serializer-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.5.0/kafka-schema-converter-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-schema-converter-7.5.0.jar"
RUN curl "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.5.0/kafka-schema-registry-client-7.5.0.jar" -o "${KAFKA_HOME}/libs/kafka-schema-registry-client-7.5.0.jar"
RUN curl "https://repo1.maven.org/maven2/com/google/guava/guava/32.0.1-jre/guava-32.0.1-jre.jar" -o "${KAFKA_HOME}/libs/guava-32.0.1-jre.jar"
RUN curl "https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar" -o "${KAFKA_HOME}/libs/failureaccess-1.0.1.jar"
RUN curl "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.0/avro-1.11.0.jar" -o "${KAFKA_HOME}/libs/avro-1.11.0.jar"