-- A1: 取り込みエラー検知
CREATE OR REPLACE VIEW Raw.v_alert_ingest_error AS
SELECT
  count(*) AS error_count
FROM Raw._ingest_quality
WHERE status = 'ERROR'
  AND ingested_at >= current_timestamp() - INTERVAL 10 MINUTES;


-- A2: 不正行発生検知
CREATE OR REPLACE VIEW Raw.v_alert_bad_records AS
SELECT
  sum(bad_rows) AS bad_rows
FROM Raw._ingest_quality
WHERE ingested_at >= current_timestamp() - INTERVAL 10 MINUTES;


-- A3: 不正行率 SLA 違反
CREATE OR REPLACE VIEW Raw.v_alert_bad_ratio AS
SELECT
  CASE
    WHEN sum(total_rows) = 0 THEN 0
    ELSE sum(bad_rows) / sum(total_rows)
  END AS bad_ratio
FROM Raw._ingest_quality
WHERE ingested_at >= current_timestamp() - INTERVAL 1 HOUR;


-- A4: 処理時間劣化
CREATE OR REPLACE VIEW Raw.v_alert_slow_ingest AS
SELECT
  max(duration_sec) AS max_duration
FROM Raw._ingest_quality
WHERE ingested_at >= current_timestamp() - INTERVAL 30 MINUTES;
