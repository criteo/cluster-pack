#!/bin/bash
set -e

CHECKSUM=$(curl -sL --retry 3 "http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" | awk '{print $4}')
echo "Archive SHA256: $CHECKSUM"

curl -sL --retry 3 "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
  | tee >(sha512sum | awk -v expected="$CHECKSUM" '{if ($1 != expected) exit 1}') \
  | gunzip \
  | tar -x -C /usr/

rm -rf "${HADOOP_HOME}"/share/doc
chown -R root:root "${HADOOP_HOME}"