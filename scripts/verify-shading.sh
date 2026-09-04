#!/bin/sh

# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

JAR_FILE="$1"

for REQUIRED_CLASS in \
  io/opentelemetry/api/GlobalOpenTelemetry.class \
  io/opentelemetry/context/Context.class \
  io/opentelemetry/common/ComponentLoader.class
do
  if ! jar tf "${JAR_FILE}" | grep -Fxq "${REQUIRED_CLASS}"; then
    echo "Missing standard OpenTelemetry class: ${REQUIRED_CLASS}"
    exit 1
  fi
done

if jar tf "${JAR_FILE}" | \
  grep -q '^com/google/cloud/spark/bigquery/repackaged/io/opentelemetry/'; then
  echo "Found relocated OpenTelemetry classes"
  exit 1
fi

if ! unzip -p "${JAR_FILE}" \
  com/google/cloud/bigquery/connector/common/BigQueryClientFactory.class | \
  strings | grep -q 'io/opentelemetry/api/GlobalOpenTelemetry'; then
  echo "Connector bytecode does not reference standard OpenTelemetry packages"
  exit 1
fi

UNSHADED_CLASSES=$(jar tf "${JAR_FILE}" | \
  grep -v META-INF | \
  grep -E "\.class$" | \
  grep -v com/google/cloud/spark/bigquery | \
  grep -v com/google/cloud/bigquery/connector/common | \
  grep -vE 'org/apache/spark/sql/.*SparkSqlUtils.class' | \
  grep -vE '^io/opentelemetry/(api|context|common)/')

if [ -n "${UNSHADED_CLASSES}" ]; then
  echo "${UNSHADED_CLASSES}"
  echo "Found unshaded classes, please fix above findings"
  exit 1
fi

echo "No unexpected unshaded classes found"
