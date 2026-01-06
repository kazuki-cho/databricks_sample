"""
Databricks Lakeflow用テストデータ生成ジョブ（大量データ最適化版）
分散処理を活用して大量データを効率的に生成します
"""

import yaml
import random
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, rand, lit, row_number, concat_ws
from pyspark.sql.window import Window
import string

class OptimizedTestDataGenerator:
    """大量データ生成最適化クラス"""
    
    def __init__(self, config_path):
        """
        初期化
        Args:
            config_path: 設定ファイルのパス
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = SparkSession.builder \
            .appName("OptimizedTestDataGenerator") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
    
    def create_base_dataframe(self, record_count):
        """
        ベースとなるDataFrameを作成（分散処理用）
        Args:
            record_count: レコード数
        Returns:
            DataFrame: ベースDataFrame
        """
        # パーティション数を計算（1パーティションあたり100万件程度）
        num_partitions = max(1, record_count // 1000000)
        
        # range()を使用して分散処理可能なDataFrameを作成
        df = self.spark.range(0, record_count, numPartitions=num_partitions)
        
        return df
    
    def add_columns(self, df):
        """
        カラムを追加
        Args:
            df: ベースDataFrame
        Returns:
            DataFrame: カラム追加済みDataFrame
        """
        result_df = df
        
        for col_def in self.config['columns']:
            col_name = col_def['name']
            col_type = col_def['type'].upper()
            value_type = col_def.get('value_type', '')
            nullable = col_def.get('nullable', True)
            duplicatable = col_def.get('duplicatable', True)
            
            # カラム式を生成
            if col_type == 'INT':
                if not duplicatable:
                    # 重複不可の場合は連番ベース
                    min_val = col_def.get('value_min', 1)
                    expr_str = f"id + {min_val}"
                else:
                    min_val = col_def.get('value_min', 1)
                    max_val = col_def.get('value_max', 100000)
                    expr_str = f"cast(rand() * ({max_val} - {min_val}) + {min_val} as int)"
            
            elif col_type == 'STRING':
                expr_str = self._get_string_expression(col_def)
            
            elif col_type == 'DATE':
                min_date = col_def.get('value_min', '1970-01-01')
                max_date = col_def.get('value_max', '2023-12-31')
                min_days = (datetime.strptime(min_date, '%Y-%m-%d') - datetime(1970, 1, 1)).days
                max_days = (datetime.strptime(max_date, '%Y-%m-%d') - datetime(1970, 1, 1)).days
                expr_str = f"date_add('1970-01-01', cast(rand() * ({max_days} - {min_days}) + {min_days} as int))"
            
            elif col_type == 'TIMESTAMP':
                min_date = col_def.get('value_min', '1970-01-01')
                max_date = col_def.get('value_max', '2023-12-31')
                min_ts = int(datetime.strptime(min_date, '%Y-%m-%d').timestamp())
                max_ts = int(datetime.strptime(max_date, '%Y-%m-%d').timestamp())
                expr_str = f"cast(from_unixtime(cast(rand() * ({max_ts} - {min_ts}) + {min_ts} as bigint)) as timestamp)"
            
            elif col_type == 'DOUBLE':
                min_val = col_def.get('value_min', 0.0)
                max_val = col_def.get('value_max', 1000.0)
                expr_str = f"round(rand() * ({max_val} - {min_val}) + {min_val}, 2)"
            
            else:
                expr_str = "null"
            
            # nullable処理
            if nullable:
                expr_str = f"case when rand() < 0.1 then null else {expr_str} end"
            
            result_df = result_df.withColumn(col_name, expr(expr_str))
        
        # id列を削除
        result_df = result_df.drop('id')
        
        return result_df
    
    def _get_string_expression(self, col_def):
        """
        STRING型のカラム式を生成
        Args:
            col_def: カラム定義
        Returns:
            str: カラム式
        """
        value_type = col_def.get('value_type', '')
        
        if value_type == 'first_name':
            names = ["太郎", "花子", "一郎", "美咲", "健太", "さくら", "翔太", "結衣",
                    "John", "Mary", "Michael", "Sarah", "David", "Emma", "James", "Olivia"]
            return self._create_array_random_expr(names)
        
        elif value_type == 'last_name':
            names = ["佐藤", "鈴木", "高橋", "田中", "渡辺", "伊藤", "山本", "中村",
                    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
            return self._create_array_random_expr(names)
        
        elif value_type == 'country_code':
            codes = ["JP", "US", "GB", "DE", "FR", "CA", "AU", "CN", "KR", "IN"]
            return self._create_array_random_expr(codes)
        
        elif value_type == 'state_code':
            codes = ["東京", "大阪", "神奈川", "愛知", "CA", "NY", "TX", "FL", "IL"]
            return self._create_array_random_expr(codes)
        
        elif value_type == 'city_name':
            cities = ["Tokyo", "Osaka", "Yokohama", "Nagoya", "New York", "Los Angeles",
                     "Chicago", "Houston", "Phoenix", "London", "Paris", "Berlin"]
            return self._create_array_random_expr(cities)
        
        elif value_type == 'zip_code':
            return "concat(lpad(cast(cast(rand() * 900 + 100 as int) as string), 3, '0'), '-', lpad(cast(cast(rand() * 9000 + 1000 as int) as string), 4, '0'))"
        
        elif value_type == 'phone_number':
            return "concat(lpad(cast(cast(rand() * 900 + 100 as int) as string), 3, '0'), '-', lpad(cast(cast(rand() * 9000 + 1000 as int) as string), 4, '0'), '-', lpad(cast(cast(rand() * 9000 + 1000 as int) as string), 4, '0'))"
        
        elif value_type == 're':
            # 正規表現パターンの簡易対応
            pattern = col_def.get('value_pattern', '')
            return self._pattern_to_expr(pattern)
        
        else:
            length = col_def.get('value_length', 10)
            return f"substring(md5(cast(rand() as string)), 1, {length})"
    
    def _create_array_random_expr(self, values):
        """
        配列からランダムに選択する式を生成
        Args:
            values: 値のリスト
        Returns:
            str: SQL式
        """
        array_str = ", ".join([f"'{v}'" for v in values])
        return f"array({array_str})[cast(rand() * {len(values)} as int)]"
    
    def _pattern_to_expr(self, pattern):
        """
        正規表現パターンをSQL式に変換（簡易版）
        Args:
            pattern: 正規表現パターン
        Returns:
            str: SQL式
        """
        # \d{4}-\d{4}-\d{4} のようなパターンに対応
        pattern = pattern.replace('^', '').replace('$', '').replace('¥d', '\\d')
        
        # \d{n}を数字生成式に変換
        parts = []
        remaining = pattern
        
        while remaining:
            match = re.search(r'\\d\{(\d+)\}', remaining)
            if match:
                # マッチ前の固定文字列
                if match.start() > 0:
                    parts.append(f"'{remaining[:match.start()]}'")
                
                # 数字生成
                count = int(match.group(1))
                max_val = 10 ** count - 1
                parts.append(f"lpad(cast(cast(rand() * {max_val} as int) as string), {count}, '0')")
                
                remaining = remaining[match.end():]
            else:
                if remaining:
                    parts.append(f"'{remaining}'")
                break
        
        return "concat(" + ", ".join(parts) + ")"
    
    def write_data(self, df):
        """
        データを指定された形式で書き込み
        Args:
            df: 書き込むDataFrame
        """
        output_config = self.config['output']
        path = output_config['path']
        file_type = output_config['file_type'].lower()
        
        writer = df.write.mode('overwrite')
        
        if file_type == 'csv':
            csv_options = output_config.get('csv_options', {})
            writer = writer.options(
                compression=csv_options.get('compression', 'none'),
                encoding=csv_options.get('encoding', 'utf-8'),
                delimiter=csv_options.get('delimiter', ','),
                header=str(csv_options.get('header', True)).lower()
            )
            writer.csv(path)
        
        elif file_type == 'parquet':
            parquet_options = output_config.get('parquet_options', {})
            writer = writer.options(
                compression=parquet_options.get('compression', 'snappy')
            )
            writer.parquet(path)
        
        elif file_type == 'delta':
            writer.format('delta').save(path)
        
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        print(f"Data written to {path}")
    
    def run(self):
        """ジョブを実行"""
        print("Starting optimized test data generation...")
        record_count = self.config['output']['record_count']
        print(f"Target records: {record_count:,}")
        
        # ベースDataFrame作成
        df = self.create_base_dataframe(record_count)
        
        # カラム追加
        df = self.add_columns(df)
        
        print("Schema:")
        df.printSchema()
        
        print("Sample data:")
        df.show(10, truncate=False)
        
        # データ書き込み
        self.write_data(df)
        
        print("Test data generation completed!")


def main():
    """メイン関数"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python optimized_test_data_generator.py <config_file_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    generator = OptimizedTestDataGenerator(config_path)
    generator.run()


if __name__ == "__main__":
    main()
