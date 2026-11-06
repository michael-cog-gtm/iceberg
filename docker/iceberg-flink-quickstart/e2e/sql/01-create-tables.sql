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

-- 1. Create the tables: an append table, a copy filled by a streaming read, and a
--    format-v2 upsert table with an identifier field (primary key).
CREATE DATABASE IF NOT EXISTS iceberg_catalog.e2e;

CREATE TABLE iceberg_catalog.e2e.events (
  id       BIGINT,
  category STRING,
  amount   DOUBLE,
  event_ts TIMESTAMP(6)
);

CREATE TABLE iceberg_catalog.e2e.events_stream_copy (
  id       BIGINT,
  category STRING,
  amount   DOUBLE,
  event_ts TIMESTAMP(6)
);

CREATE TABLE iceberg_catalog.e2e.users_upsert (
  user_id    BIGINT,
  name       STRING,
  update_seq BIGINT,
  score      INT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.upsert.enabled'  = 'true'
);

SHOW TABLES IN iceberg_catalog.e2e;
