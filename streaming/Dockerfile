# Spark 3.5 (Python) + Iceberg + AWS 번들 + Kafka 커넥터
FROM apache/spark:3.5.1-python3

USER root

# Spark 잡에서 쓰는 커넥터 JAR을 이미지에 미리 내려받아 둔다.
# (매 실행마다 --packages 로 받지 않아 기동이 빠르고 오프라인에서도 안정적)
ENV IVY_JARS=/opt/spark/jars
ARG ICEBERG_VER=1.5.2
ARG SPARK_KAFKA_VER=3.5.1
ARG SCALA_VER=2.12
RUN set -eux; \
    cd ${IVY_JARS}; \
    BASE=https://repo1.maven.org/maven2; \
    curl -fSL -o iceberg-spark-runtime.jar \
      ${BASE}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VER}/${ICEBERG_VER}/iceberg-spark-runtime-3.5_${SCALA_VER}-${ICEBERG_VER}.jar; \
    curl -fSL -o iceberg-aws-bundle.jar \
      ${BASE}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar; \
    curl -fSL -o spark-sql-kafka.jar \
      ${BASE}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VER}/${SPARK_KAFKA_VER}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_KAFKA_VER}.jar; \
    curl -fSL -o spark-token-provider-kafka.jar \
      ${BASE}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VER}/${SPARK_KAFKA_VER}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_KAFKA_VER}.jar; \
    curl -fSL -o kafka-clients.jar \
      ${BASE}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fSL -o commons-pool2.jar \
      ${BASE}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

WORKDIR /app
