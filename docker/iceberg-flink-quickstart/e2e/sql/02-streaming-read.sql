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

-- 2. Continuous streaming read of events into events_stream_copy (unbounded job,
--    started before any data exists; cancelled by run-scenario.sh once caught up).
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'e2e-streaming-read';

INSERT INTO iceberg_catalog.e2e.events_stream_copy
SELECT id, category, amount, event_ts
FROM iceberg_catalog.e2e.events /*+ OPTIONS('streaming'='true', 'monitor-interval'='2s') */;
