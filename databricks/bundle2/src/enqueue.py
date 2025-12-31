import uuid

file_path = dbutils.widgets.get("file_path")

parts = file_path.strip("/").split("/")
schema_name = parts[-4]
table_name  = parts[-3]

spark.sql(f"""
INSERT INTO Raw._ingest_queue
VALUES (
  '{uuid.uuid4()}',
  '{file_path}',
  '{schema_name}',
  '{table_name}',
  'NEW',
  0,
  current_timestamp(),
  current_timestamp(),
  NULL
)
""")
