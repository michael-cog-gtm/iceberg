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

# Flink streaming end-to-end scenario on the quickstart stack with locally built jars.
#
# Usage: ./run-scenario.sh <flink line: 1.20|2.2|2.3>     run the scenario (stack left up)
#        ./run-scenario.sh down                           stop the stack
#
# Needs ./build-jars.sh first. Writes raw outputs and summary.txt to results/flink-<line>/.
# The Flink dashboard is on http://localhost:8081 while it runs. Tunables (defaults):
#   ROWS=6000 RPS=100                   append stream (about ROWS/RPS seconds)
#   UPSERT_ROWS=2000 UPSERT_KEYS=100 UPSERT_RPS=50
#   CHECKPOINT_INTERVAL=10s  MIN_CHECKPOINTS=3
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
export COMPOSE_PROJECT_NAME=iceberg-flink-e2e

if [[ "${1:-}" == "down" ]]; then
  FLINK_IMAGE=x FLINK_LINE=x docker compose down -v --remove-orphans
  exit 0
fi

export FLINK_LINE="${1:?usage: $0 <1.20|2.2|2.3>}"
case "$FLINK_LINE" in
  1.20) export FLINK_IMAGE="${FLINK_IMAGE:-apache/flink:1.20.1-java17}" ;;
  2.2) export FLINK_IMAGE="${FLINK_IMAGE:-apache/flink:2.2.0-java21}" ;;
  2.3) export FLINK_IMAGE="${FLINK_IMAGE:-apache/flink:2.3.0-java21}" ;;
  *) echo "Unknown Flink line: $FLINK_LINE" >&2; exit 2 ;;
esac
export SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.7-java17}"
export ROWS="${ROWS:-6000}" RPS="${RPS:-100}"
export UPSERT_ROWS="${UPSERT_ROWS:-2000}" UPSERT_KEYS="${UPSERT_KEYS:-100}" UPSERT_RPS="${UPSERT_RPS:-50}"
export CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-10s}"
MIN_CHECKPOINTS="${MIN_CHECKPOINTS:-3}"
FLINK_REST=http://localhost:8081
# Only these are substituted, so metadata table names like `t$snapshots` stay intact
SQL_VARS='${ROWS} ${RPS} ${UPSERT_ROWS} ${UPSERT_KEYS} ${UPSERT_RPS} ${CHECKPOINT_INTERVAL}'

[[ -d "jars/flink-$FLINK_LINE" && -d jars/spark ]] || { echo "Run ./build-jars.sh $FLINK_LINE first" >&2; exit 2; }
(( ROWS % 5 == 0 )) || { echo "ROWS must be a multiple of 5" >&2; exit 2; }

OUT="results/flink-$FLINK_LINE"
rm -rf "$OUT" && mkdir -p "$OUT"
exec > >(tee "$OUT/run.log") 2>&1

log() { echo "[$(date -u +%T)] $*"; }

pull() {
  local img="$1" ns_img="$1"
  docker image inspect "$img" >/dev/null 2>&1 && return 0
  [[ "$img" == */* ]] || ns_img="library/$img"
  docker pull -q "$img" 2>/dev/null || { docker pull -q "mirror.gcr.io/${ns_img#apache/flink*}" 2>/dev/null; } || true
  docker image inspect "$img" >/dev/null 2>&1 && return 0
  # Docker Hub library/flink and apache/flink are the same images
  local mirror="mirror.gcr.io/$ns_img"
  [[ "$img" == apache/flink:* ]] && mirror="mirror.gcr.io/library/flink:${img#apache/flink:}"
  docker pull -q "$mirror" && docker tag "$mirror" "$img"
}

sql() {
  local name="$1" rendered
  rendered="$(mktemp)" && chmod 644 "$rendered"
  envsubst "$SQL_VARS" < "sql/$name" > "$rendered"
  docker cp -q "$rendered" "jobmanager:/tmp/$name"
  envsubst "$SQL_VARS" < sql/init.sql > "$rendered"
  docker cp -q "$rendered" jobmanager:/tmp/init.sql
  rm -f "$rendered"
  docker exec jobmanager ./bin/sql-client.sh -i /tmp/init.sql -f "/tmp/$name"
}

job_id() {
  curl -fs "$FLINK_REST/jobs/overview" |
    jq -r --arg n "$1" '[.jobs[] | select(.name == $n)] | sort_by(."start-time") | last | .jid // empty'
}

job_state() { curl -fs "$FLINK_REST/jobs/$1" | jq -r .state; }

wait_for_job() {
  local name="$1" jid="" t=0
  while [[ -z "$jid" ]]; do
    (( t++ > 120 )) && { log "Job $name never appeared"; return 1; }
    sleep 1; jid="$(job_id "$name")"
  done
  echo "$jid"
}

wait_state() {
  local jid="$1" want="$2" timeout="$3" t=0 state
  while :; do
    state="$(job_state "$jid")"
    [[ "$state" == "$want" ]] && return 0
    case "$state" in FAILED|CANCELED) [[ "$want" != "$state" ]] && { log "Job $jid is $state"; return 1; } ;; esac
    (( t++ > timeout )) && { log "Timed out waiting for $jid to be $want (is $state)"; return 1; }
    sleep 1
  done
}

save_job() {
  local jid="$1" tag="$2"
  curl -fs "$FLINK_REST/jobs/$jid" > "$OUT/job-$tag.json"
  curl -fs "$FLINK_REST/jobs/$jid/checkpoints" > "$OUT/checkpoints-$tag.json"
  log "$tag: state=$(jq -r .state "$OUT/job-$tag.json") checkpoints completed=$(jq -r .counts.completed "$OUT/checkpoints-$tag.json") failed=$(jq -r .counts.failed "$OUT/checkpoints-$tag.json")"
}

log "Flink line $FLINK_LINE, image $FLINK_IMAGE, jars: $(ls "jars/flink-$FLINK_LINE" | paste -sd' ')"
cp jars/BUILD_INFO.txt "$OUT/" 2>/dev/null || true
pull "$FLINK_IMAGE"; pull "$SPARK_IMAGE"; pull rustfs/rustfs:1.0.0
docker image inspect iceberg-rest-fixture:local >/dev/null

log "Starting stack"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build
for _ in $(seq 120); do curl -fs "$FLINK_REST/overview" >/dev/null 2>&1 && break; sleep 2; done
for _ in $(seq 60); do
  [[ "$(curl -fs "$FLINK_REST/overview" | jq -r '."slots-total"')" -gt 0 ]] && break; sleep 2
done
curl -fs "$FLINK_REST/config" | jq -c . > "$OUT/flink-config.json"
log "Flink $(jq -r '."flink-version"' "$OUT/flink-config.json") up: $(curl -fs "$FLINK_REST/overview" | jq -c .)"
docker exec jobmanager ls -l /opt/flink/lib/iceberg > "$OUT/flink-lib-iceberg.txt"

log "1. Create tables"
sql 01-create-tables.sql > "$OUT/01-create-tables.out"

log "2. Start continuous streaming read events -> events_stream_copy"
sql 02-streaming-read.sql > "$OUT/02-streaming-read.out"
READ_JID="$(wait_for_job e2e-streaming-read)"
wait_state "$READ_JID" RUNNING 120

log "3. Streaming insert from datagen: $ROWS rows at $RPS rows/s"
sql 03-streaming-insert.sql > "$OUT/03-streaming-insert.out"
INSERT_JID="$(wait_for_job e2e-datagen-streaming-insert)"
wait_state "$INSERT_JID" RUNNING 120
log "Insert job $INSERT_JID running"
wait_state "$INSERT_JID" FINISHED $(( ROWS / RPS + 300 ))
save_job "$INSERT_JID" streaming-insert

log "Waiting for the streaming read to catch up"
copy=0
for _ in $(seq 40); do
  copy="$(sql probe-copy.sql | grep -oE 'PROBE\|copy_count\|[0-9]+' | tail -1 | cut -d'|' -f3)"
  log "events_stream_copy rows: ${copy:-?} / $ROWS"
  [[ "${copy:-0}" -ge "$ROWS" ]] && break
  sleep 5
done
sleep "${CHECKPOINT_INTERVAL%s}"
save_job "$READ_JID" streaming-read
log "Cancelling unbounded streaming read job $READ_JID"
curl -fs -X PATCH "$FLINK_REST/jobs/$READ_JID?mode=cancel" >/dev/null
wait_state "$READ_JID" CANCELED 60

log "4. Upsert into format-v2 table: $UPSERT_ROWS changes over $UPSERT_KEYS keys at $UPSERT_RPS rows/s"
sql 04-upsert.sql > "$OUT/04-upsert.out"
UPSERT_JID="$(wait_for_job e2e-upsert-format-v2)"
wait_state "$UPSERT_JID" FINISHED $(( UPSERT_ROWS / UPSERT_RPS + 300 ))
save_job "$UPSERT_JID" upsert

log "5. Flink batch reads"
sql 05-batch-checks.sql > "$OUT/05-batch-checks.out"
grep -oE 'CHECK\|[a-z_]+\|[0-9]+' "$OUT/05-batch-checks.out" | sort -u > "$OUT/flink-checks.txt"

log "6. Spark cross-engine reads"
SPARK_JARS="$(docker exec spark sh -c 'ls /opt/iceberg-jars/*.jar | paste -sd,')"
docker exec spark /opt/spark/bin/spark-sql --master 'local[2]' -S \
  --jars "$SPARK_JARS" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.type=rest \
  --conf spark.sql.catalog.ice.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.ice.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.ice.s3.endpoint=http://object-store:9000 \
  --conf spark.sql.catalog.ice.s3.path-style-access=true \
  --conf spark.sql.catalog.ice.s3.access-key-id=admin \
  --conf spark.sql.catalog.ice.s3.secret-access-key=password \
  --conf spark.sql.catalog.ice.client.region=us-east-1 \
  -f /opt/e2e-sql/spark-checks.sql > "$OUT/06-spark-checks.out" 2> "$OUT/06-spark-checks.err" || {
    log "spark-sql failed"; tail -40 "$OUT/06-spark-checks.err"; }
{
  grep -oE 'CHECK\|[a-z_]+\|[0-9]+' "$OUT/06-spark-checks.out" || true
  awk -F'\t' '$1 == "format-version" { print "CHECK|upsert_format_version|" $2 }' "$OUT/06-spark-checks.out"
} | sort -u > "$OUT/spark-checks.txt"

log "7. Compare with the generated input"
declare -A EXPECT=(
  [events_count]="== $ROWS"
  [events_distinct_ids]="== $ROWS"
  [events_sum_ids]="== $(( ROWS * (ROWS + 1) / 2 ))"
  [events_category_min]="== $(( ROWS / 5 ))"
  [events_category_max]="== $(( ROWS / 5 ))"
  [events_snapshots]=">= $MIN_CHECKPOINTS"
  [copy_count]="== $ROWS"
  [copy_distinct_ids]="== $ROWS"
  [copy_sum_ids]="== $(( ROWS * (ROWS + 1) / 2 ))"
  [upsert_count]="== $UPSERT_KEYS"
  [upsert_distinct_keys]="== $UPSERT_KEYS"
  [upsert_sum_seq]="== $(( (2 * UPSERT_ROWS - UPSERT_KEYS + 1) * UPSERT_KEYS / 2 ))"
  [upsert_min_seq]="== $(( UPSERT_ROWS - UPSERT_KEYS + 1 ))"
  [upsert_snapshots]=">= $MIN_CHECKPOINTS"
  [upsert_eq_delete_files]=">= 1"
  [upsert_format_version]="== 2"
)
ORDER=(events_count events_distinct_ids events_sum_ids events_category_min events_category_max events_snapshots
  copy_count copy_distinct_ids copy_sum_ids upsert_count upsert_distinct_keys upsert_sum_seq
  upsert_min_seq upsert_snapshots upsert_eq_delete_files upsert_format_version)

fails=0
check() {
  local engine="$1" name="$2" actual="$3" op expected
  read -r op expected <<< "${EXPECT[$name]}"
  local ok=FAIL
  if [[ "$op" == "==" && "$actual" == "$expected" ]]; then ok=PASS
  elif [[ "$op" == ">=" && "$actual" =~ ^[0-9]+$ && "$actual" -ge "$expected" ]]; then ok=PASS
  fi
  [[ "$ok" == PASS ]] || fails=$((fails + 1))
  printf '%-6s %-6s %-27s expected %-2s %-14s actual %s\n' "$ok" "$engine" "$name" "$op" "$expected" "${actual:-<missing>}"
}
value() { grep -E "^CHECK\|$2\|" "$1" | head -1 | cut -d'|' -f3- ; }

{
  echo "Flink $FLINK_LINE ($(jq -r '."flink-version"' "$OUT/flink-config.json")) streaming e2e — $(date -u +%FT%TZ)"
  sed -n '1,4p' "$OUT/BUILD_INFO.txt" 2>/dev/null || true
  echo "input: events ROWS=$ROWS at RPS=$RPS; upsert UPSERT_ROWS=$UPSERT_ROWS over UPSERT_KEYS=$UPSERT_KEYS at UPSERT_RPS=$UPSERT_RPS; checkpoint interval $CHECKPOINT_INTERVAL"
  echo
  for tag in streaming-insert streaming-read upsert; do
    c="$(jq -r .counts.completed "$OUT/checkpoints-$tag.json")"
    f="$(jq -r .counts.failed "$OUT/checkpoints-$tag.json")"
    s="$(jq -r .state "$OUT/job-$tag.json")"
    exp_state=FINISHED; [[ "$tag" == streaming-read ]] && exp_state=RUNNING
    ok=PASS; { [[ "$c" -ge "$MIN_CHECKPOINTS" && "$s" == "$exp_state" ]]; } || { ok=FAIL; fails=$((fails + 1)); }
    printf '%-6s %-6s %-27s state %s (expected %s), completed checkpoints %s (expected >= %s), failed %s\n' "$ok" flink "job:$tag" "$s" "$exp_state" "$c" "$MIN_CHECKPOINTS" "$f"
  done
  # Flink SQL has no way to show the format version; the equality delete files show v2 instead
  for name in "${ORDER[@]}"; do
    [[ "$name" == upsert_format_version ]] || check flink "$name" "$(value "$OUT/flink-checks.txt" "$name")"
  done
  for name in "${ORDER[@]}"; do check spark "$name" "$(value "$OUT/spark-checks.txt" "$name")"; done
  echo
  if (( fails == 0 )); then echo "RESULT: PASS"; else echo "RESULT: FAIL ($fails checks failed)"; fi
} > "$OUT/summary.txt"
cat "$OUT/summary.txt"
log "Stack left up for the dashboard ($FLINK_REST); stop it with: $0 down"
(( fails == 0 ))
