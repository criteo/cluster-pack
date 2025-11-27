# docker file/docker-compose derived from https://github.com/gettyimages/docker-spark
# https://github.com/gettyimages/docker-spark/blob/master/LICENSE
FROM debian:bookworm

RUN apt-get update

# Java
RUN apt-get install -y curl openjdk-17-jre \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Hadoop
ARG HADOOP_VERSION=3.3.5
ENV HADOOP_VERSION="${HADOOP_VERSION}"
ENV HADOOP_HOME="/usr/hadoop-${HADOOP_VERSION}"
ENV HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
ENV PATH="${PATH}:${HADOOP_HOME}/bin"
COPY scripts/install-hadoop.sh /tmp/
RUN /tmp/install-hadoop.sh && rm /tmp/install-hadoop.sh

# Spark
ARG SPARK_INPUT_VERSION=3.5.7
ENV SPARK_VERSION="${SPARK_INPUT_VERSION}"
ENV SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-without-hadoop"
ENV SPARK_HOME="/usr/spark-${SPARK_VERSION}"
ENV SPARK_DIST_CLASSPATH="${HADOOP_HOME}/etc/hadoop/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/tools/lib/*"
ENV PATH="$PATH:${SPARK_HOME}/bin"
COPY scripts/install-spark.sh /tmp/
RUN /tmp/install-spark.sh && rm /tmp/install-spark.sh

# Python
ARG PYTHON_VERSION=3.9.15
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"
RUN uv python install ${PYTHON_VERSION} --default

WORKDIR /cluster-pack
