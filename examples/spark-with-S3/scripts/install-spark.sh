#!/bin/bash
set -euo pipefail

SPARK_ARCHIVE_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"
SPARK_SHA512_URL="${SPARK_ARCHIVE_URL}.sha512"

CHECKSUM="$(curl -fsSL --retry 3 "${SPARK_SHA512_URL}" | awk '{print $1}')"
echo "Archive SHA512: ${CHECKSUM}"

TMP_ARCHIVE="/tmp/${SPARK_PACKAGE}.tgz"
curl -fsSL --retry 3 -o "${TMP_ARCHIVE}" "${SPARK_ARCHIVE_URL}"

# Verify archive integrity (fail hard on mismatch).
echo "${CHECKSUM}  ${TMP_ARCHIVE}" | sha512sum -c -

tar -xzf "${TMP_ARCHIVE}" -C /usr/
rm -f "${TMP_ARCHIVE}"

mv "/usr/${SPARK_PACKAGE}" "${SPARK_HOME}"
chown -R root:root "${SPARK_HOME}"
