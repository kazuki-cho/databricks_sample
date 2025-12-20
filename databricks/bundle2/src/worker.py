import time
from datetime import datetime
from lib_csv import *

MAX_IDLE = 60
SLEEP = 5
idle = 0

while idle < MAX_IDLE:
    spark.sql("""
    UPDATE Raw._ingest_queue
    SET status='RUNNING', try_count=try_count+1, updated_at=current_timestamp()
    WHERE queue_id = (
      SELECT queue_id FROM Raw._ingest_queue
      WHERE status='NEW'
      ORDER BY enqueued_at LIMIT 1
    )
    """)

    q = spark.sql("""
    SELECT * FROM Raw._ingest_queue
    WHERE status='RUNNING'
      AND updated_at >= current_timestamp() - INTERVAL 5 SECONDS
    ORDER BY updated_at DESC LIMIT 1
    """)

    if q.count() == 0:
        idle += SLEEP
        time.sleep(SLEEP)
        continue

    r = q.collect()[0]
    start = time.time()

    try:
        spark.sql(f"""
        INSERT INTO Raw._table_lock
        SELECT '{r.schema_name}', '{r.table_name}', current_timestamp()
        WHERE NOT EXISTS (
          SELECT 1 FROM Raw._table_lock
          WHERE schema_name='{r.schema_name}'
            AND table_name='{r.table_name}'
        )
        """)

        file_size = get_file_size_bytes(r.file_path)
        parts = calc_partitions(file_size)

        bad_path = f"abfss://<container>/bad_records/{r.schema_name}/{r.table_name}/{uuid.uuid4()}"
        df, enc = read_csv_with_fallback(r.file_path, bad_path=bad_path)

        good_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        bad_df  = df.filter(F.col("_corrupt_record").isNotNull())

        good_df = normalize_columns(good_df).repartition(parts)

        spark.sql(f"DROP TABLE IF EXISTS Raw.{r.schema_name}.{r.table_name}")

        good_df.write.format("delta").mode("overwrite").saveAsTable(
            f"Raw.{r.schema_name}.{r.table_name}"
        )

        duration = time.time() - start

        spark.sql(f"""
        INSERT INTO Raw._ingest_quality
        VALUES (
          '{r.queue_id}', '{r.schema_name}', '{r.table_name}', '{r.file_path}',
          current_timestamp(), 'DONE', {r.try_count},
          {duration},
          {good_df.count()+bad_df.count()},
          {good_df.count()},
          {bad_df.count()},
          '{enc}', '{bad_path}', NULL
        )
        """)

        spark.sql(f"UPDATE Raw._ingest_queue SET status='DONE' WHERE queue_id='{r.queue_id}'")

    except Exception as e:
        spark.sql(f"""
        UPDATE Raw._ingest_queue
        SET status='ERROR', message='{str(e)}'
        WHERE queue_id='{r.queue_id}'
        """)

    finally:
        spark.sql(f"""
        DELETE FROM Raw._table_lock
        WHERE schema_name='{r.schema_name}'
          AND table_name='{r.table_name}'
        """)
        idle = 0
