CREATE TABLE IF NOT EXISTS Raw._ingest_queue (
  queue_id STRING,
  file_path STRING,
  schema_name STRING,
  table_name STRING,
  status STRING,
  try_count INT,
  enqueued_at TIMESTAMP,
  updated_at TIMESTAMP,
  message STRING
) USING delta;

CREATE TABLE IF NOT EXISTS Raw._table_lock (
  schema_name STRING,
  table_name STRING,
  locked_at TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS Raw._ingest_quality (
  queue_id STRING,
  schema_name STRING,
  table_name STRING,
  file_path STRING,
  ingested_at TIMESTAMP,
  status STRING,
  try_count INT,
  duration_sec DOUBLE,
  total_rows BIGINT,
  good_rows BIGINT,
  bad_rows BIGINT,
  encoding STRING,
  bad_record_path STRING,
  message STRING
) USING delta;
