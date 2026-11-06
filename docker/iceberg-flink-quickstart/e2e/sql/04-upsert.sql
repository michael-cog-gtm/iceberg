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

-- 4. Streaming upsert into the format-v2 table: ${UPSERT_ROWS} changes over ${UPSERT_KEYS}
--    keys (user_id = seq mod keys). Parallelism 1 keeps per-key order deterministic, so
--    each key must end with its highest update_seq.
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'e2e-upsert-format-v2';

CREATE TEMPORARY TABLE default_catalog.default_database.gen_users (
  seq BIGINT
) WITH (
  'connector'        = 'datagen',
  'rows-per-second'  = '${UPSERT_RPS}',
  'fields.seq.kind'  = 'sequence',
  'fields.seq.start' = '1',
  'fields.seq.end'   = '${UPSERT_ROWS}'
);

INSERT INTO iceberg_catalog.e2e.users_upsert
SELECT
  MOD(seq, ${UPSERT_KEYS}) AS user_id,
  CONCAT('user-', CAST(MOD(seq, ${UPSERT_KEYS}) AS STRING)) AS name,
  seq AS update_seq,
  CAST(MOD(seq, 97) AS INT) AS score
FROM default_catalog.default_database.gen_users;
