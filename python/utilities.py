# Databricks notebook source
# MAGIC %md
# MAGIC # ユーティリティ関数
# MAGIC 
# MAGIC チェックポイント管理、テーブル確認、リカバリ用の関数

# COMMAND ----------

import re
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## チェックポイント管理

# COMMAND ----------

def list_checkpoints(checkpoint_base_path):
    """
    すべてのチェックポイントを一覧表示
    """
    try:
        schemas = dbutils.fs.ls(checkpoint_base_path)
        
        checkpoints = []
        for schema in schemas:
            if schema.isDir():
                schema_name = schema.name.rstrip("/")
                tables = dbutils.fs.ls(schema.path)
                
                for table in tables:
                    if table.isDir():
                        table_name = table.name.rstrip("/")
                        checkpoint_path = table.path
                        
                        # チェックポイントの情報取得
                        try:
                            commits = dbutils.fs.ls(f"{checkpoint_path}commits/")
                            last_commit = max(commits, key=lambda x: x.name) if commits else None
                            
                            checkpoints.append({
                                'schema': schema_name,
                                'table': table_name,
                                'checkpoint_path': checkpoint_path,
                                'last_commit': last_commit.name if last_commit else None
                            })
                        except:
                            checkpoints.append({
                                'schema': schema_name,
                                'table': table_name,
                                'checkpoint_path': checkpoint_path,
                                'last_commit': 'No commits'
                            })
        
        return checkpoints
    except Exception as e:
        print(f"Error listing checkpoints: {str(e)}")
        return []

def display_checkpoints(checkpoint_base_path):
    """
    チェックポイント一覧を表形式で表示
    """
    checkpoints = list_checkpoints(checkpoint_base_path)
    
    if not checkpoints:
        print("No checkpoints found")
        return
    
    # DataFrameに変換して表示
    df = spark.createDataFrame(checkpoints)
    df.display()

# COMMAND ----------

def reset_checkpoint(checkpoint_path, confirm=False):
    """
    チェックポイントをリセット（削除）
    
    Args:
        checkpoint_path: チェックポイントのパス
        confirm: Trueの場合のみ実際に削除
    """
    if not confirm:
        print("⚠️  WARNING: This will delete the checkpoint!")
        print(f"Path: {checkpoint_path}")
        print("\nTo actually delete, call with confirm=True:")
        print(f"reset_checkpoint('{checkpoint_path}', confirm=True)")
        return False
    
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"✓ Checkpoint deleted: {checkpoint_path}")
        return True
    except Exception as e:
        print(f"✗ Error deleting checkpoint: {str(e)}")
        return False

def reset_table_checkpoint(checkpoint_base_path, schema_name, table_name, confirm=False):
    """
    特定のテーブルのチェックポイントをリセット
    """
    checkpoint_path = f"{checkpoint_base_path}{schema_name}/{table_name}/"
    return reset_checkpoint(checkpoint_path, confirm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## テーブル管理

# COMMAND ----------

def list_tables_with_metadata(catalog_name):
    """
    カタログ内のすべてのテーブルとメタデータを表示
    """
    try:
        result = spark.sql(f"""
            SELECT 
                table_catalog,
                table_schema,
                table_name,
                table_type,
                created
            FROM {catalog_name}.information_schema.tables
            WHERE table_schema != 'information_schema'
            ORDER BY table_schema, table_name
        """)
        
        result.display()
        return result
    except Exception as e:
        print(f"Error listing tables: {str(e)}")
        return None

def get_table_stats(catalog_name, schema_name, table_name):
    """
    テーブルの統計情報を取得
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    try:
        # 行数とファイル数
        stats = spark.sql(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT _source_file) as total_files,
                MIN(_ingestion_time) as first_ingestion,
                MAX(_ingestion_time) as last_ingestion
            FROM {full_table_name}
        """)
        
        print(f"\n{'='*60}")
        print(f"Table: {full_table_name}")
        print(f"{'='*60}")
        
        stats.display()
        
        # 最近の取り込み状況（日別）
        daily_stats = spark.sql(f"""
            SELECT 
                DATE(_ingestion_time) as ingestion_date,
                COUNT(*) as rows,
                COUNT(DISTINCT _source_file) as files
            FROM {full_table_name}
            WHERE _ingestion_time >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY DATE(_ingestion_time)
            ORDER BY ingestion_date DESC
        """)
        
        print("\nDaily ingestion (last 7 days):")
        daily_stats.display()
        
        # スキーマ情報
        print("\nTable schema:")
        spark.sql(f"DESCRIBE {full_table_name}").display()
        
    except Exception as e:
        print(f"Error getting table stats: {str(e)}")

def drop_table(catalog_name, schema_name, table_name, confirm=False):
    """
    テーブルを削除
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    if not confirm:
        print("⚠️  WARNING: This will drop the table!")
        print(f"Table: {full_table_name}")
        print("\nTo actually drop, call with confirm=True:")
        print(f"drop_table('{catalog_name}', '{schema_name}', '{table_name}', confirm=True)")
        return False
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print(f"✓ Table dropped: {full_table_name}")
        return True
    except Exception as e:
        print(f"✗ Error dropping table: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ品質チェック

# COMMAND ----------

def check_data_quality(catalog_name, schema_name, table_name):
    """
    データ品質のチェック
    - NULL値の割合
    - 破損レコード
    - 重複行
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    try:
        # 総行数
        total_rows = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']
        
        print(f"\n{'='*60}")
        print(f"Data Quality Report: {full_table_name}")
        print(f"{'='*60}")
        print(f"Total rows: {total_rows:,}")
        
        # 破損レコード
        corrupted = spark.sql(f"""
            SELECT COUNT(*) as cnt 
            FROM {full_table_name}
            WHERE _corrupted_record IS NOT NULL
        """).collect()[0]['cnt']
        
        if corrupted > 0:
            print(f"⚠️  Corrupted records: {corrupted:,} ({corrupted/total_rows*100:.2f}%)")
        else:
            print("✓ No corrupted records")
        
        # 各カラムのNULL率
        columns = [c.name for c in spark.table(full_table_name).schema 
                  if not c.name.startswith('_')]
        
        print(f"\nNULL percentage by column:")
        null_checks = []
        
        for col_name in columns[:20]:  # 最初の20カラムのみ
            null_count = spark.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM {full_table_name}
                WHERE `{col_name}` IS NULL
            """).collect()[0]['cnt']
            
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            null_checks.append((col_name, null_count, null_pct))
        
        # DataFrameで表示
        null_df = spark.createDataFrame(null_checks, ['column', 'null_count', 'null_percentage'])
        null_df.orderBy('null_percentage', ascending=False).display()
        
        # 完全重複行のチェック
        print("\nChecking for duplicate rows...")
        dup_check = spark.sql(f"""
            WITH row_counts AS (
                SELECT *, COUNT(*) OVER (PARTITION BY *) as dup_count
                FROM {full_table_name}
            )
            SELECT COUNT(*) as duplicate_rows
            FROM row_counts
            WHERE dup_count > 1
        """)
        
        dup_count = dup_check.collect()[0]['duplicate_rows']
        if dup_count > 0:
            print(f"⚠️  Duplicate rows: {dup_count:,} ({dup_count/total_rows*100:.2f}%)")
        else:
            print("✓ No duplicate rows")
        
    except Exception as e:
        print(f"Error checking data quality: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用例

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # チェックポイント一覧を表示
# MAGIC display_checkpoints("abfss://container@storage.dfs.core.windows.net/checkpoints/")
# MAGIC 
# MAGIC # テーブル統計を表示
# MAGIC get_table_stats("dev_catalog", "sales", "transactions")
# MAGIC 
# MAGIC # データ品質チェック
# MAGIC check_data_quality("dev_catalog", "sales", "transactions")
# MAGIC 
# MAGIC # チェックポイントをリセット（再処理したい場合）
# MAGIC reset_table_checkpoint(
# MAGIC     "abfss://container@storage.dfs.core.windows.net/checkpoints/",
# MAGIC     "sales",
# MAGIC     "transactions",
# MAGIC     confirm=True
# MAGIC )
# MAGIC 
# MAGIC # テーブルを削除
# MAGIC drop_table("dev_catalog", "sales", "transactions", confirm=True)
# MAGIC ```
