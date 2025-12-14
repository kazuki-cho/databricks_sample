# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS Auto Loader - CSV.GZ Ingestion
# MAGIC 
# MAGIC このノートブックは以下の処理を行います：
# MAGIC 1. トリガーされたファイルからschema_name/table_nameを抽出
# MAGIC 2. ファイルパターン（YYYY-mm-dd_HH:MM:SS.csv.gz）に一致するか検証
# MAGIC 3. Auto Loaderでファイルを読み込み
# MAGIC 4. カラム名を正規化（空白などを_に置換）
# MAGIC 5. 型推論を実行
# MAGIC 6. 対応するDeltaテーブルに書き込み

# COMMAND ----------

# パラメータ定義
dbutils.widgets.text("triggered_file", "")
dbutils.widgets.text("base_path", "")
dbutils.widgets.text("checkpoint_base_path", "")
dbutils.widgets.text("catalog_name", "dev_catalog")

triggered_file = dbutils.widgets.get("triggered_file")
base_path = dbutils.widgets.get("base_path")
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path")
catalog_name = dbutils.widgets.get("catalog_name")

print(f"Triggered file: {triggered_file}")
print(f"Base path: {base_path}")
print(f"Checkpoint base: {checkpoint_base_path}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

import re
import os
from datetime import datetime
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## ファイルパス解析

# COMMAND ----------

def parse_file_path(file_path, base_path):
    """
    ファイルパスを解析してschema_name, table_name, file_nameを抽出
    
    期待されるパターン:
    abfss://container@storage.dfs.core.windows.net/schema_name/table_name/tmp/file_name_YYYY-mm-dd_HH:MM:SS.csv.gz
    
    Returns:
        dict: {
            'schema_name': str,
            'table_name': str,
            'file_name': str,
            'is_valid': bool,
            'error': str or None
        }
    """
    try:
        # ベースパスを除去
        relative_path = file_path.replace(base_path, "").lstrip("/")
        
        print(f"Relative path: {relative_path}")
        
        # パス分割: schema_name/table_name/tmp/file_name_YYYY-mm-dd_HH:MM:SS.csv.gz
        parts = relative_path.split("/")
        
        if len(parts) < 4:
            return {
                'is_valid': False,
                'error': f"Invalid path structure. Expected: schema_name/table_name/tmp/filename, got: {relative_path}"
            }
        
        schema_name = parts[0]
        table_name = parts[1]
        tmp_dir = parts[2]
        file_name = parts[3]
        
        # tmpディレクトリの検証
        if tmp_dir != "tmp":
            return {
                'is_valid': False,
                'error': f"Expected 'tmp' directory, got: {tmp_dir}"
            }
        
        # ファイルパターンの検証: file_name_YYYY-mm-dd_HH:MM:SS.csv.gz
        # 例: sales_data_2024-12-14_10:30:45.csv.gz
        pattern = r'^.+_\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.csv\.gz$'
        
        if not re.match(pattern, file_name):
            return {
                'is_valid': False,
                'error': f"File name does not match expected pattern (name_YYYY-mm-dd_HH:MM:SS.csv.gz): {file_name}"
            }
        
        return {
            'is_valid': True,
            'schema_name': schema_name,
            'table_name': table_name,
            'file_name': file_name,
            'error': None
        }
        
    except Exception as e:
        return {
            'is_valid': False,
            'error': f"Error parsing file path: {str(e)}"
        }

# ファイルパスを解析
parsed = parse_file_path(triggered_file, base_path)

if not parsed['is_valid']:
    print(f"✗ Invalid file: {parsed['error']}")
    dbutils.notebook.exit(f"SKIPPED: {parsed['error']}")

schema_name = parsed['schema_name']
table_name = parsed['table_name']
file_name = parsed['file_name']

print(f"✓ Valid file detected")
print(f"  Schema: {schema_name}")
print(f"  Table: {table_name}")
print(f"  File: {file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## カラム名正規化関数

# COMMAND ----------

def normalize_column_name(col_name):
    """
    カラム名を正規化
    - 前後の空白を削除
    - スペース、タブ、改行を_に置換
    - 特殊文字（カッコ、ハイフン、ドット等）を_に置換
    - 連続する_を1つに圧縮
    - 小文字に変換
    - 先頭が数字の場合は_を追加
    
    Args:
        col_name: 元のカラム名
    
    Returns:
        str: 正規化されたカラム名
    """
    # 前後の空白を削除
    normalized = col_name.strip()
    
    # 空白文字（スペース、タブ、改行等）を_に置換
    normalized = re.sub(r'\s+', '_', normalized)
    
    # 特殊文字を_に置換（英数字とアンダースコア以外）
    normalized = re.sub(r'[^a-zA-Z0-9_]', '_', normalized)
    
    # 連続する_を1つに圧縮
    normalized = re.sub(r'_+', '_', normalized)
    
    # 前後の_を削除
    normalized = normalized.strip('_')
    
    # 小文字に変換
    normalized = normalized.lower()
    
    # 先頭が数字の場合は_を追加
    if normalized and normalized[0].isdigit():
        normalized = '_' + normalized
    
    # 空文字列の場合はデフォルト名
    if not normalized:
        normalized = 'unnamed_column'
    
    return normalized

def normalize_dataframe_columns(df):
    """
    DataFrameの全カラム名を正規化
    重複するカラム名がある場合は連番を付与
    
    Args:
        df: Spark DataFrame
    
    Returns:
        DataFrame: カラム名が正規化されたDataFrame
    """
    original_cols = df.columns
    normalized_cols = [normalize_column_name(c) for c in original_cols]
    
    # 重複チェックと連番付与
    seen = {}
    final_cols = []
    
    for orig, norm in zip(original_cols, normalized_cols):
        if norm in seen:
            seen[norm] += 1
            final_name = f"{norm}_{seen[norm]}"
        else:
            seen[norm] = 0
            final_name = norm
        
        final_cols.append(final_name)
        
        if orig != final_name:
            print(f"  Column renamed: '{orig}' -> '{final_name}'")
    
    # カラム名を変更
    for old_name, new_name in zip(original_cols, final_cols):
        df = df.withColumnRenamed(old_name, new_name)
    
    return df

# テスト
test_cols = [
    "Customer Name",
    "Order Date",
    "Product (ID)",
    "Price $",
    "Quantity #",
    "Total\tAmount",
    "Email-Address",
    "Phone.Number",
    "  Leading Space",
    "Trailing Space  ",
    "Multiple   Spaces",
    "日本語カラム",
    "123StartWithNumber",
    "",
    "Duplicate",
    "Duplicate"
]

print("\nカラム名正規化テスト:")
for col in test_cols:
    print(f"  '{col}' -> '{normalize_column_name(col)}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader設定とデータ読み込み

# COMMAND ----------

# ソースディレクトリ（schema_name/table_name/tmp/）
source_dir = f"{base_path}{schema_name}/{table_name}/tmp/"

# チェックポイントパス
checkpoint_path = f"{checkpoint_base_path}{schema_name}/{table_name}/"

# スキーマ保存場所
schema_location = f"{checkpoint_path}schema/"

print(f"Source directory: {source_dir}")
print(f"Checkpoint path: {checkpoint_path}")
print(f"Schema location: {schema_location}")

# COMMAND ----------

# Auto Loaderでストリーミング読み込み
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    
    # スキーマ推論設定
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")  # 型推論を有効化
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # スキーマ進化を許可
    
    # ファイルパターン（.csv.gzファイルのみ）
    .option("pathGlobFilter", "*.csv.gz")
    
    # 既存ファイルは処理しない（新規ファイルのみ）
    .option("cloudFiles.includeExistingFiles", "false")
    
    # CSV固有設定
    .option("header", "true")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("multiLine", "false")
    .option("escape", '"')
    .option("encoding", "UTF-8")
    
    # gzip圧縮ファイルの処理
    .option("compression", "gzip")
    
    # 不正な行の処理
    .option("mode", "PERMISSIVE")  # 不正な行は_corruptedカラムに格納
    .option("columnNameOfCorruptRecord", "_corrupted_record")
    
    .load(source_dir)
)

print("✓ Auto Loader stream configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## カラム名の正規化とメタデータ追加

# COMMAND ----------

# マイクロバッチ処理用の関数
def process_micro_batch(df_batch, batch_id):
    """
    各マイクロバッチの処理
    1. カラム名を正規化
    2. メタデータ列を追加
    3. Deltaテーブルに書き込み
    """
    if df_batch.isEmpty():
        print(f"Batch {batch_id}: No data to process")
        return
    
    print(f"Batch {batch_id}: Processing {df_batch.count()} rows")
    
    # カラム名を正規化
    df_normalized = normalize_dataframe_columns(df_batch)
    
    # メタデータ列を追加
    df_enriched = (df_normalized
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_schema_name", lit(schema_name))
        .withColumn("_table_name", lit(table_name))
        .withColumn("_batch_id", lit(batch_id))
    )
    
    # 破損レコードがあれば警告
    if "_corrupted_record" in df_enriched.columns:
        corrupted_count = df_enriched.filter(col("_corrupted_record").isNotNull()).count()
        if corrupted_count > 0:
            print(f"⚠️  Warning: {corrupted_count} corrupted records found in batch {batch_id}")
    
    # テーブル名（カタログ.スキーマ.テーブル）
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    # Deltaテーブルに書き込み
    (df_enriched.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # スキーマの自動マージ
        .saveAsTable(full_table_name)
    )
    
    print(f"✓ Batch {batch_id}: Written to {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ストリーミングクエリの実行

# COMMAND ----------

# foreachBatchを使用してマイクロバッチ処理
query = (df_raw.writeStream
    .foreachBatch(process_micro_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # すべての新規ファイルを処理して終了
    .start()
)

# ストリーミングジョブの完了を待つ
query.awaitTermination()

print("✓ Auto Loader processing completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 処理結果のサマリー

# COMMAND ----------

# テーブル名
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# 処理結果を確認
try:
    result = spark.sql(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT _source_file) as files_processed,
            MIN(_ingestion_time) as first_ingestion,
            MAX(_ingestion_time) as last_ingestion,
            COUNT(DISTINCT _batch_id) as batches_processed
        FROM {full_table_name}
        WHERE _ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
    """)
    
    result.display()
    
    # 結果を取得
    row = result.collect()[0]
    
    print("\n" + "="*60)
    print("処理サマリー:")
    print("="*60)
    print(f"テーブル: {full_table_name}")
    print(f"処理行数: {row['total_rows']:,}")
    print(f"処理ファイル数: {row['files_processed']}")
    print(f"バッチ数: {row['batches_processed']}")
    print(f"開始時刻: {row['first_ingestion']}")
    print(f"終了時刻: {row['last_ingestion']}")
    print("="*60)
    
    # 破損レコードの確認
    corrupted_query = f"""
        SELECT COUNT(*) as corrupted_count
        FROM {full_table_name}
        WHERE _corrupted_record IS NOT NULL
        AND _ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
    """
    
    corrupted_result = spark.sql(corrupted_query).collect()[0]
    
    if corrupted_result['corrupted_count'] > 0:
        print(f"\n⚠️  Warning: {corrupted_result['corrupted_count']} corrupted records detected")
        
        # 破損レコードのサンプルを表示
        spark.sql(f"""
            SELECT _source_file, _corrupted_record
            FROM {full_table_name}
            WHERE _corrupted_record IS NOT NULL
            AND _ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
            LIMIT 10
        """).display()
    else:
        print("\n✓ No corrupted records")
    
    # テーブルスキーマを表示
    print("\nテーブルスキーマ:")
    spark.sql(f"DESCRIBE {full_table_name}").display()
    
except Exception as e:
    print(f"Error querying results: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 処理完了

# COMMAND ----------

print(f"""
✓ Processing completed successfully

Details:
- Schema: {schema_name}
- Table: {table_name}
- Source file: {file_name}
- Target table: {full_table_name}
- Checkpoint: {checkpoint_path}
""")

dbutils.notebook.exit("SUCCESS")
