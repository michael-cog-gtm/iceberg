#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Builds the Iceberg jars the e2e stack uses from this checkout and stages them in ./jars:
#   jars/flink-<line>/  iceberg-flink-runtime-<line> + iceberg-aws-bundle
#   jars/spark/         iceberg-spark-runtime-3.5_2.12 + iceberg-aws-bundle
# and builds the REST catalog fixture image iceberg-rest-fixture:local.
#
# Usage: ./build-jars.sh [flink lines, default "1.20,2.2,2.3"]
# Extra Gradle args can be passed with GRADLE_ARGS (e.g. GRADLE_ARGS=-PmockitoVersion=5.24.0).
set -euo pipefail

LINES="${1:-1.20,2.2,2.3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
cd "$ROOT"

# The build needs JDK 17 or 21
if ! "${JAVA_HOME:-/nonexistent}/bin/java" -version 2>&1 | grep -qE 'version "(17|21)[."]'; then
  for j in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-21-openjdk-amd64; do
    [[ -d "$j" ]] && export JAVA_HOME="$j" && break
  done
fi

TASKS=(:iceberg-aws-bundle:shadowJar :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar :iceberg-open-api:shadowJar)
IFS=',' read -ra LINE_ARR <<< "$LINES"
for line in "${LINE_ARR[@]}"; do
  TASKS+=(":iceberg-flink:iceberg-flink-runtime-${line}:shadowJar")
done

./gradlew -DflinkVersions="$LINES" -DsparkVersions=3.5 -DscalaVersion=2.12 -DkafkaVersions= \
  -x javadoc ${GRADLE_ARGS:-} "${TASKS[@]}"

latest() { ls -t $1 2>/dev/null | grep -vE -- '-(sources|javadoc|tests)\.jar$' | head -1; }
AWS_JAR="$(latest "aws-bundle/build/libs/iceberg-aws-bundle-*.jar")"
SPARK_JAR="$(latest "spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-*.jar")"

rm -rf "$HERE/jars/spark" && mkdir -p "$HERE/jars/spark"
cp "$AWS_JAR" "$SPARK_JAR" "$HERE/jars/spark/"
for line in "${LINE_ARR[@]}"; do
  FLINK_JAR="$(latest "flink/v${line}/flink-runtime/build/libs/iceberg-flink-runtime-${line}-*.jar")"
  rm -rf "$HERE/jars/flink-${line}" && mkdir -p "$HERE/jars/flink-${line}"
  cp "$AWS_JAR" "$FLINK_JAR" "$HERE/jars/flink-${line}/"
done

# REST fixture image from this checkout (base image pulled through a mirror when Docker Hub throttles)
docker image inspect azul/zulu-openjdk:17-jre-headless >/dev/null 2>&1 || \
  docker pull azul/zulu-openjdk:17-jre-headless || \
  { docker pull mirror.gcr.io/azul/zulu-openjdk:17-jre-headless && \
    docker tag mirror.gcr.io/azul/zulu-openjdk:17-jre-headless azul/zulu-openjdk:17-jre-headless; }
docker build -q -t iceberg-rest-fixture:local -f docker/iceberg-rest-fixture/Dockerfile .

{
  echo "commit: $(git rev-parse HEAD)"
  echo "built: $(date -u +%FT%TZ)"
  echo "java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
  echo "gradle args: ${GRADLE_ARGS:-}"
  echo "mockito (iceberg-core testRuntimeClasspath): $(./gradlew -q -DflinkVersions= -DsparkVersions= -DkafkaVersions= ${GRADLE_ARGS:-} :iceberg-core:dependencyInsight --configuration testRuntimeClasspath --dependency org.mockito:mockito-core 2>/dev/null | grep -m1 -oE 'org.mockito:mockito-core:[^ ]+' || echo unknown)"
  echo "jars:"
  (cd "$HERE/jars" && sha256sum */*.jar)
  echo "mockito classes in runtime jars: $(for f in "$HERE"/jars/*/*.jar; do unzip -l "$f" | grep -c 'org/mockito/' || true; done | paste -sd+ | bc)"
} | tee "$HERE/jars/BUILD_INFO.txt"
