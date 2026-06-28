#!/bin/bash
# Download Kafka-Spark connector jars for offline use
# These are the same jars that --packages would pull from Maven Central
# Run this script if your Spark cluster has no internet access to Maven
# Usage: bash download-jars.sh

set -euo pipefail

JARS_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_VERSION="3.5.0"
SCALA_VERSION="2.12"
KAFKA_VERSION="3.5.0"
COMMONS_POOL_VERSION="2.11.1"

declare -A ARTIFACTS=(
    ["spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"]="https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
    ["spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"]="https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
    ["kafka-clients-${KAFKA_VERSION}.jar"]="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"
    ["commons-pool2-${COMMONS_POOL_VERSION}.jar"]="https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar"
)

echo "Downloading Spark-Kafka connector jars to ${JARS_DIR}..."
mkdir -p "${JARS_DIR}"

for jar_name in "${!ARTIFACTS[@]}"; do
    url="${ARTIFACTS[$jar_name]}"
    target="${JARS_DIR}/${jar_name}"

    if [ -f "${target}" ]; then
        echo "  ✅ Already exists: ${jar_name}"
    else
        echo "  ⬇️  Downloading: ${jar_name} ..."
        if curl -fsSL "${url}" -o "${target}"; then
            echo "  ✅ Downloaded: ${jar_name} ($(du -h "${target}" | cut -f1))"
        else
            echo "  ❌ Failed to download: ${jar_name}"
            rm -f "${target}"
        fi
    fi
done

echo ""
echo "Done. Jars downloaded to ${JARS_DIR}"
ls -lh "${JARS_DIR}"/*.jar 2>/dev/null || echo "(no jars found - downloads may have failed)"
