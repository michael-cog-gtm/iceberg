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

-- 5. Batch reads. Each row is "CHECK|<name>|<value>"; run-scenario.sh compares the
--    values with the generated input.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.display.max-column-width' = '500';

SELECT CONCAT('CHECK|events_count|', CAST(COUNT(*) AS STRING)) FROM iceberg_catalog.e2e.events;
SELECT CONCAT('CHECK|events_distinct_ids|', CAST(COUNT(DISTINCT id) AS STRING)) FROM iceberg_catalog.e2e.events;
SELECT CONCAT('CHECK|events_sum_ids|', CAST(SUM(id) AS STRING)) FROM iceberg_catalog.e2e.events;
SELECT CONCAT('CHECK|events_category_min|', CAST(MIN(c) AS STRING))
FROM (SELECT category, COUNT(*) AS c FROM iceberg_catalog.e2e.events GROUP BY category);
SELECT CONCAT('CHECK|events_category_max|', CAST(MAX(c) AS STRING))
FROM (SELECT category, COUNT(*) AS c FROM iceberg_catalog.e2e.events GROUP BY category);
SELECT CONCAT('CHECK|events_snapshots|', CAST(COUNT(*) AS STRING)) FROM iceberg_catalog.e2e.`events$snapshots`;

SELECT CONCAT('CHECK|copy_count|', CAST(COUNT(*) AS STRING)) FROM iceberg_catalog.e2e.events_stream_copy;
SELECT CONCAT('CHECK|copy_distinct_ids|', CAST(COUNT(DISTINCT id) AS STRING)) FROM iceberg_catalog.e2e.events_stream_copy;
SELECT CONCAT('CHECK|copy_sum_ids|', CAST(SUM(id) AS STRING)) FROM iceberg_catalog.e2e.events_stream_copy;

SELECT CONCAT('CHECK|upsert_count|', CAST(COUNT(*) AS STRING)) FROM iceberg_catalog.e2e.users_upsert;
SELECT CONCAT('CHECK|upsert_distinct_keys|', CAST(COUNT(DISTINCT user_id) AS STRING)) FROM iceberg_catalog.e2e.users_upsert;
SELECT CONCAT('CHECK|upsert_sum_seq|', CAST(SUM(update_seq) AS STRING)) FROM iceberg_catalog.e2e.users_upsert;
SELECT CONCAT('CHECK|upsert_min_seq|', CAST(MIN(update_seq) AS STRING)) FROM iceberg_catalog.e2e.users_upsert;
SELECT CONCAT('CHECK|upsert_snapshots|', CAST(COUNT(*) AS STRING)) FROM iceberg_catalog.e2e.`users_upsert$snapshots`;
SELECT CONCAT('CHECK|upsert_eq_delete_files|', CAST(COUNT(*) AS STRING))
FROM iceberg_catalog.e2e.`users_upsert$files` WHERE content = 2;

SELECT * FROM iceberg_catalog.e2e.users_upsert ORDER BY user_id LIMIT 5;
SELECT snapshot_id, operation, summary['added-records'] AS added_records, summary['added-equality-deletes'] AS added_eq_deletes
FROM iceberg_catalog.e2e.`users_upsert$snapshots`;
