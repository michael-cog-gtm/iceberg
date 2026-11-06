<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Flink streaming end-to-end check with locally built jars

The quickstart stack (Flink, REST catalog fixture, RustFS) with the Iceberg jars built from this
checkout instead of released downloads, plus a Spark container for cross-engine reads.

```sh
./build-jars.sh 2.3        # or 1.20 / 2.2 / "1.20,2.2,2.3"; needs JDK 17 or 21 and Docker
./run-scenario.sh 2.3      # dashboard: http://localhost:8081
./run-scenario.sh down
```

Scenario, all through the Flink SQL client (`sql/`):

1. Create `e2e.events` (append), `e2e.events_stream_copy`, and the format-v2 upsert table
   `e2e.users_upsert` with identifier field `user_id`.
2. Start an unbounded streaming read `events -> events_stream_copy`.
3. Stream `ROWS` datagen rows into `events` at `RPS` rows/s, committing on each checkpoint.
4. Wait until the streaming read has copied every row, then cancel it.
5. Upsert `UPSERT_ROWS` changes over `UPSERT_KEYS` keys into `users_upsert`.
6. Batch-read every table in Flink and in Spark 3.5 and compare counts, sums and snapshot and
   delete-file counts with the generated input.

`results/flink-<line>/summary.txt` has the PASS/FAIL table; raw SQL client output, Spark output,
and Flink job and checkpoint JSON are next to it.
