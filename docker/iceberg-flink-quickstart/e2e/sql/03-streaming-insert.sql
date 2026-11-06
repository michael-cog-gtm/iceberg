-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- 3. Streaming insert from a rate-limited datagen source: ${ROWS} rows at ${RPS} rows/s,
--    committed to Iceberg on every checkpoint.
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'e2e-datagen-streaming-insert';

CREATE TEMPORARY TABLE default_catalog.default_database.gen_events (
  id BIGINT
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '${RPS}',
  'fields.id.kind'  = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end'   = '${ROWS}'
);

INSERT INTO iceberg_catalog.e2e.events
SELECT
  id,
  CONCAT('c', CAST(MOD(id, 5) AS STRING)) AS category,
  CAST(MOD(id, 1000) AS DOUBLE) / 10 AS amount,
  CAST(LOCALTIMESTAMP AS TIMESTAMP(6)) AS event_ts
FROM default_catalog.default_database.gen_events;
