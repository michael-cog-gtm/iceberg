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

# Same layout as ../Dockerfile, but with locally built Iceberg jars (see build-jars.sh)
# instead of the released downloads.
ARG FLINK_IMAGE=apache/flink:2.3.0-java21

FROM ${FLINK_IMAGE}

SHELL ["/bin/bash", "-c"]

ARG FLINK_LINE=2.3
ARG HADOOP_VERSION=3.4.2
ARG MAVEN_REPO=https://maven-central.storage-download.googleapis.com/maven2

USER flink
WORKDIR /opt/flink

COPY --chown=flink:flink jars/flink-${FLINK_LINE}/ ./lib/iceberg/

RUN mkdir -p ./lib/hadoop && pushd ./lib/hadoop && \
    curl -fsSO ${MAVEN_REPO}/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar && \
    curl -fsSO ${MAVEN_REPO}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar && \
    popd && ls -l ./lib/iceberg ./lib/hadoop
