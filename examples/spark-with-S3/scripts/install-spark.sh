#!/bin/bash
set -e

CHECKSUM=$(curl -sL --retry 3 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz.sha512" | awk '{print $1}')
echo "Archive SHA256: $CHECKSUM"

curl -sL --retry 3 https://dlcdn.apache.org/spark/spark-"${SPARK_VERSION}"/"${SPARK_PACKAGE}".tgz \
 | tee >(sha512sum | awk -v expected="$CHECKSUM" '{if ($1 != expected) exit 1}') \
 | gunzip \
 | tar -x -C /usr/

mv "/usr/${SPARK_PACKAGE}" "${SPARK_HOME}"
chown -R root:root "${SPARK_HOME}"
