# ---------- Stage 1: fetch Maven runtime deps ----------
FROM maven:3.9-eclipse-temurin-17 AS deps
WORKDIR /build

# Minimal POM: Spark cloud jar + JAXB to silence the Base64 warning on Java 11+
# (jaxb-runtime brings core/impl; api is the javax.* 2.3.x line that works on JDK 11)
RUN printf '%s\n' \
  '<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">' \
  '<modelVersion>4.0.0</modelVersion><groupId>tmp</groupId><artifactId>fetch</artifactId><version>1</version>' \
  '<dependencies>' \
  '  <dependency><groupId>org.apache.spark</groupId><artifactId>spark-hadoop-cloud_2.13</artifactId><version>4.0.0</version></dependency>' \
  '  <dependency><groupId>javax.xml.bind</groupId><artifactId>jaxb-api</artifactId><version>2.3.1</version></dependency>' \
  '  <dependency><groupId>org.glassfish.jaxb</groupId><artifactId>jaxb-runtime</artifactId><version>2.3.3</version></dependency>' \
  '  <dependency><groupId>javax.activation</groupId><artifactId>javax.activation-api</artifactId><version>1.2.0</version></dependency>' \
  '  <dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><version>42.7.4</version></dependency>' \
  '  <dependency><groupId>io.delta</groupId><artifactId>delta-spark_2.13</artifactId><version>4.0.0</version></dependency>' \
  '</dependencies>' \
  '</project>' > pom.xml

RUN mvn -q -B dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/deps

# ---------- Stage 2: pull Hadoop native libs (match Hadoop 3.4.x used by Spark 4 cloud jars) ----------
FROM debian:12-slim AS hadoop_native
ARG HADOOP_VERSION=3.4.0
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl tar && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt
# Use an official Apache mirror; adjust if your build environment requires a specific mirror/proxy
RUN curl -fL "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz && \
    # keep only the native .so libs to keep the image slim
    mkdir -p /opt/hadoop/lib/native && \
    cp -a "/opt/hadoop-${HADOOP_VERSION}/lib/native/." /opt/hadoop/lib/native && \
    rm -rf "/opt/hadoop-${HADOOP_VERSION}"

# ---------- Stage 3: final image ----------
FROM docker.io/bitnamilegacy/spark:4.0.0

USER root

# 1) Add the JARs (spark-hadoop-cloud + JAXB) into Spark's classpath
COPY --from=deps /deps/*.jar ${SPARK_HOME}/jars/

# 2) Add Hadoop native libs and wire up the loader path
RUN mkdir -p /opt/hadoop/lib/native
COPY --from=hadoop_native /opt/hadoop/lib/native /opt/hadoop/lib/native
ENV HADOOP_HOME=/opt/hadoop
ENV LD_LIBRARY_PATH=/opt/hadoop/lib/native:${LD_LIBRARY_PATH}

# 3) Provide a no-op Metrics2 config so Hadoop stops warning about missing files
RUN set -eux; \
    mkdir -p "${SPARK_HOME}/conf"; \
    printf '%s\n' \
      '# Minimal Hadoop Metrics2 config (no sinks configured)' \
      '*.period=10' \
    > "${SPARK_HOME}/conf/hadoop-metrics2.properties"

# 4) Set perms for the non-root user Bitnami runs as (1001)
RUN chown -R 1001:1001 "${SPARK_HOME}/jars" /opt/hadoop "${SPARK_HOME}/conf"

# Add a Postgres client lib for the DDL step from your Python script
# (binary wheel avoids compiler toolchain)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3-pip; \
    pip3 install --no-cache-dir psycopg2-binary==2.9.9; \
    rm -rf /var/lib/apt/lists/*

USER 1001