# Databricks notebook source
# Lake Flow Ingestion Job - CSV/Excel対応版
# ビルトインExcelサポート使用（DBR 17.1+）

import yaml
import re
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, current_timestamp, lit, monotonically_increasing_id
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------
# 設定ファイル管理

def find_matching_config(file_path, config_dir="/Workspace/configs/tables"):
    """
    到着ファイルパスから該当する設定ファイルを検索（ディレクトリベース）
    
    Args:
        file_path: 到着ファイルのフルパス
        config_dir: 設定ファイルが格納されているディレクトリ
    
    Returns:
        tuple: (設定辞書, 設定ファイル名)
    
    Raises:
        ValueError: マッチする設定が見つからない場合
    """
    config_files = list(Path(config_dir).glob("*.yaml")) + list(Path(config_dir).glob("*.yml"))
    
    for config_file in config_files:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # ディレクトリパスでマッチング
        source_dir = config['source']['directory']
        if source_dir in file_path:
            print(f"  Matched config file: {config_file.name}")
            return config, config_file.stem
    
    raise ValueError(f"No matching config found for file path: {file_path}")

# COMMAND ----------
# ファイル読み込み処理

def read_source_file(spark, file_path, config):
    """
    設定に基づいてCSVまたはExcelファイルを読み込み
    
    Args:
        spark: SparkSession
        file_path: 読み込むファイルのパス
        config: YAML設定辞書
    
    Returns:
        DataFrame: 読み込んだ生データ
    """
    file_type = config['source']['file_type'].lower()
    
    print(f"  File type: {file_type}")
    
    if file_type == 'csv':
        return read_csv_file(spark, file_path, config)
    elif file_type == 'excel':
        return read_excel_file(spark, file_path, config)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def read_csv_file(spark, file_path, config):
    """
    CSV(圧縮含む)ファイルを読み込み
    
    Args:
        spark: SparkSession
        file_path: CSVファイルのパス
        config: YAML設定辞書
    
    Returns:
        DataFrame: 読み込んだCSVデータ
    """
    csv_opts = config['source']['csv_options']
    
    reader = spark.read.format("csv")
    
    # 基本オプション
    reader = reader \
        .option("encoding", csv_opts.get('encoding', 'utf-8')) \
        .option("delimiter", csv_opts.get('delimiter', ',')) \
        .option("header", str(csv_opts.get('header', True)).lower()) \
        .option("inferSchema", "false")  # 明示的にfalse（設定ファイルで型管理）
    
    # 圧縮形式
    compression = csv_opts.get('compression', 'none')
    if compression == 'gzip':
        reader = reader.option("compression", "gzip")
    
    # ファイル読み込み
    df = reader.load(file_path)
    
    # スキップ行数の処理
    skip_rows = csv_opts.get('skip_rows', 0)
    if skip_rows > 0:
        print(f"  Skipping first {skip_rows} rows")
        df = df.withColumn("_row_num", monotonically_increasing_id())
        df = df.filter(col("_row_num") >= skip_rows).drop("_row_num")
    
    return df


def read_excel_file(spark, file_path, config):
    """
    Excelファイルを読み込み（ビルトイン機能使用 - DBR 17.1+）
    
    Args:
        spark: SparkSession
        file_path: Excelファイルのパス
        config: YAML設定辞書
    
    Returns:
        DataFrame: 読み込んだExcelデータ
    """
    excel_opts = config['source']['excel_options']
    
    reader = spark.read.format("excel")
    
    # ヘッダー行数
    has_header = excel_opts.get('header', True)
    header_rows = 1 if has_header else 0
    reader = reader.option("headerRows", header_rows)
    
    # シート名またはインデックスからデータアドレスを構築
    sheet = excel_opts.get('sheet_name', 0)
    if isinstance(sheet, str):
        # シート名指定の場合
        data_address = f"'{sheet}'!"
        print(f"  Reading sheet: {sheet}")
    else:
        # シートインデックスの場合（0始まり）
        # Note: Excelのシート名は通常 Sheet1, Sheet2... なので +1
        data_address = f"Sheet{sheet + 1}!"
        print(f"  Reading sheet index: {sheet}")
    
    # セル範囲の指定（オプション）
    if excel_opts.get('cell_range'):
        data_address += excel_opts['cell_range']
        print(f"  Cell range: {excel_opts['cell_range']}")
    
    reader = reader.option("dataAddress", data_address)
    
    # スキーマ推論（後で明示的にキャストするが、初期読み込みで型を推測）
    reader = reader.option("inferSchema", "true")
    
    # スキップ行数の処理（ヘッダー前の行をスキップ）
    skip_rows = excel_opts.get('skip_rows', 0)
    if skip_rows > 0:
        # dataAddressで開始行を調整
        print(f"  Skipping first {skip_rows} rows")
        # 例: A1:Z100 -> A{skip_rows+1}:Z100 に変更
        # 簡易実装: skip_rows分をDataFrame読み込み後に除外
        pass  # ビルトイン機能ではdataAddressで調整するのが望ましい
    
    df = reader.load(file_path)
    
    # スキップ行数の後処理（必要な場合）
    if skip_rows > 0 and not excel_opts.get('cell_range'):
        df = df.withColumn("_row_num", monotonically_increasing_id())
        df = df.filter(col("_row_num") >= skip_rows).drop("_row_num")
    
    return df

# COMMAND ----------
# データ変換処理

def build_column_selection(df, config):
    """
    設定に基づいてカラム選択とキャストの式を構築
    
    Args:
        df: 元のDataFrame
        config: YAML設定辞書
    
    Returns:
        list: selectExpr用の文字列リスト
    """
    file_type = config['source']['file_type'].lower()
    
    if file_type == 'csv':
        has_header = config['source']['csv_options'].get('header', True)
    else:  # excel
        has_header = config['source']['excel_options'].get('header', True)
    
    select_exprs = []
    
    for col_config in config['columns']:
        col_name = col_config['name']
        col_type = col_config['type']
        
        # ソースカラムの特定
        if 'source_column_index' in col_config:
            # 列番号指定の場合
            col_idx = col_config['source_column_index']
            
            if col_idx >= len(df.columns):
                raise ValueError(
                    f"Column index {col_idx} is out of range. "
                    f"DataFrame has {len(df.columns)} columns: {df.columns}"
                )
            
            # DataFrameの実際のカラム名を取得
            source_col_name = df.columns[col_idx]
            print(f"  Mapping column index {col_idx} -> '{source_col_name}' as '{col_name}'")
        else:
            # カラム名指定の場合
            source_col_name = col_config.get('source_column', col_name)
            
            if source_col_name not in df.columns:
                raise ValueError(
                    f"Source column '{source_col_name}' not found in DataFrame. "
                    f"Available columns: {df.columns}"
                )
            
            print(f"  Mapping column '{source_col_name}' as '{col_name}'")
        
        # 型変換式の構築
        expr = build_cast_expression(source_col_name, col_type, col_config)
        select_exprs.append(f"{expr} as {col_name}")
    
    return select_exprs


def build_cast_expression(source_col, target_type, col_config):
    """
    型変換の式を構築
    
    Args:
        source_col: ソースカラム名
        target_type: ターゲット型（STRING, DATE, DECIMAL等）
        col_config: カラム設定辞書
    
    Returns:
        str: キャスト式の文字列
    """
    source_col_escaped = f"`{source_col}`"
    target_type_upper = target_type.upper()
    
    # DATE型
    if target_type_upper.startswith('DATE'):
        date_format = col_config.get('date_format', 'yyyy-MM-dd')
        return f"to_date({source_col_escaped}, '{date_format}')"
    
    # TIMESTAMP型
    elif target_type_upper.startswith('TIMESTAMP'):
        ts_format = col_config.get('timestamp_format', 'yyyy-MM-dd HH:mm:ss')
        return f"to_timestamp({source_col_escaped}, '{ts_format}')"
    
    # 数値型
    elif target_type_upper in ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'INT', 'LONG']:
        return f"CAST({source_col_escaped} AS {target_type})"
    
    # DECIMAL型
    elif target_type_upper.startswith('DECIMAL'):
        return f"CAST({source_col_escaped} AS {target_type})"
    
    # STRING型
    elif target_type_upper == 'STRING':
        return f"CAST({source_col_escaped} AS STRING)"
    
    # BOOLEAN型
    elif target_type_upper in ['BOOLEAN', 'BOOL']:
        return f"CAST({source_col_escaped} AS BOOLEAN)"
    
    # その他はそのまま
    else:
        return source_col_escaped

# COMMAND ----------
# データ品質チェック

def validate_data_quality(df, config):
    """
    データ品質チェックを実行
    
    Args:
        df: チェック対象のDataFrame
        config: YAML設定辞書
    
    Raises:
        ValueError: 品質チェックに失敗した場合
    """
    if config['quality'].get('skip_validation', False):
        print("  Quality validation skipped (skip_validation=true)")
        return
    
    print("  Running quality checks...")
    
    # NOT NULLチェック
    not_null_columns = config['quality'].get('not_null_columns', [])
    for col_name in not_null_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Quality check failed: {null_count} null values found in column '{col_name}' "
                f"(NOT NULL constraint violated)"
            )
        print(f"    ✓ NOT NULL check passed for '{col_name}'")
    
    # ユニークチェック
    unique_columns = config['quality'].get('unique_columns', [])
    for col_name in unique_columns:
        total_count = df.count()
        distinct_count = df.select(col_name).distinct().count()
        if total_count != distinct_count:
            duplicates = total_count - distinct_count
            raise ValueError(
                f"Quality check failed: {duplicates} duplicate values found in column '{col_name}' "
                f"(UNIQUE constraint violated)"
            )
        print(f"    ✓ UNIQUE check passed for '{col_name}'")
    
    print("  ✓ All quality checks passed")

# COMMAND ----------
# Delta Lake書き込み

def write_to_delta(df, config, file_path, config_name):
    """
    Delta Lakeテーブルに書き込み
    
    Args:
        df: 書き込むDataFrame
        config: YAML設定辞書
        file_path: ソースファイルパス
        config_name: 使用した設定ファイル名
    
    Returns:
        tuple: (書き込んだ行数, テーブル名)
    """
    table_config = config['table']
    catalog = table_config.get('catalog', 'main')
    schema = table_config.get('schema', 'bronze')
    table_name_simple = table_config['name']
    table_name_full = f"{catalog}.{schema}.{table_name_simple}"
    
    print(f"  Writing to Delta table: {table_name_full}")
    
    # メタデータカラム追加
    df_with_metadata = df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", lit(file_path)) \
        .withColumn("_config_name", lit(config_name))
    
    # 書き込みモード
    write_mode = table_config.get('write_mode', 'overwrite')
    print(f"  Write mode: {write_mode}")
    
    # 書き込み実行
    write_builder = df_with_metadata.write \
        .format("delta") \
        .mode(write_mode)
    
    # パーティショニング
    partition_by = table_config.get('partition_by', [])
    if partition_by:
        print(f"  Partitioning by: {', '.join(partition_by)}")
        write_builder = write_builder.partitionBy(*partition_by)
    
    # テーブル書き込み
    write_builder.saveAsTable(table_name_full)
    
    row_count = df_with_metadata.count()
    
    return row_count, table_name_full

# COMMAND ----------
# メイン処理

def main():
    """メイン処理フロー"""
    
    print("=" * 80)
    print("Lake Flow Ingestion Job - Started")
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Widgetから到着ファイルパスを取得
    dbutils.widgets.text("file_path", "")
    file_path = dbutils.widgets.get("file_path")
    
    if not file_path:
        raise ValueError("file_path parameter is required")
    
    print(f"\n[1/6] Processing file: {file_path}")
    
    try:
        # 1. 設定ファイルを検索
        print("\n[2/6] Finding matching configuration...")
        config, config_name = find_matching_config(file_path)
        print(f"  ✓ Config loaded: {config_name}")
        
        # 2. ソースファイル読み込み
        print("\n[3/6] Reading source file...")
        df_raw = read_source_file(spark, file_path, config)
        raw_row_count = df_raw.count()
        raw_col_count = len(df_raw.columns)
        print(f"  ✓ Raw data loaded: {raw_row_count:,} rows, {raw_col_count} columns")
        print(f"  Columns: {df_raw.columns}")
        
        # 3. カラム選択と型変換
        print("\n[4/6] Transforming data (column mapping & type casting)...")
        select_exprs = build_column_selection(df_raw, config)
        df_transformed = df_raw.selectExpr(*select_exprs)
        print(f"  ✓ Data transformed: {df_transformed.count():,} rows, {len(df_transformed.columns)} columns")
        
        # スキーマ表示
        print("  Target schema:")
        df_transformed.printSchema()
        
        # 4. データ品質チェック
        print("\n[5/6] Validating data quality...")
        validate_data_quality(df_transformed, config)
        
        # 5. Delta Lake書き込み
        print("\n[6/6] Writing to Delta Lake...")
        row_count, table_name = write_to_delta(df_transformed, config, file_path, config_name)
        
        # 成功メッセージ
        print("\n" + "=" * 80)
        print("✓ SUCCESS - Ingestion completed successfully")
        print("=" * 80)
        print(f"  Rows written:    {row_count:,}")
        print(f"  Target table:    {table_name}")
        print(f"  Config used:     {config_name}")
        print(f"  Source file:     {file_path}")
        print(f"  Completed at:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
    except Exception as e:
        # エラーメッセージ
        print("\n" + "=" * 80)
        print("✗ FAILED - Ingestion failed with error")
        print("=" * 80)
        print(f"  Error type:      {type(e).__name__}")
        print(f"  Error message:   {str(e)}")
        print(f"  Source file:     {file_path}")
        print(f"  Failed at:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # エラーを再スロー
        raise

# COMMAND ----------
# 実行

if __name__ == "__main__":
    main()
