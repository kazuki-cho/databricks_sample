# Lake Flow Ingestion Platform

Azure Databricks上でCSV/Excelファイルを自動的にDelta Lakeに取り込むためのデータ基盤プラットフォーム。

## 🎯 特徴

- **CSV/Excel両対応**: CSV.gzipとExcelファイルの両方をサポート（Databricks Runtime 17.1+のビルトイン機能使用）
- **柔軟な設定管理**: YAMLベースの設定ファイルでテーブルごとにスキーマを管理
- **カラムマッピング**: ソースファイルのヘッダー名とテーブルカラム名を自由にマッピング
- **列番号指定対応**: ヘッダーなしファイルも列番号（インデックス）で指定可能
- **ファイル到着トリガー**: ADLS Gen2へのファイル到着で自動実行
- **データ品質チェック**: NOT NULL、UNIQUE制約などの検証機能
- **Gitベース管理**: Databricks Asset Bundleで設定とコードをバージョン管理
- **dbt統合**: Bronze→Silver→Goldのデータ変換パイプライン

## 📋 システム要件

- **Databricks Runtime**: 17.1.x以降（Excelビルトインサポート必須）
- **Azure Storage**: ADLS Gen2
- **Python**: 3.10以降
- **Databricks CLI**: 最新版

## 🏗️ アーキテクチャ

```
ADLS Gen2 (ファイル到着)
    ↓ トリガー
┌─────────────────────────────────────┐
│ Lake Flow Ingestion Job             │
│                                     │
│ 1. ファイルパス解析                   │
│ 2. 設定ファイル検索                   │
│ 3. CSV/Excel読み込み                 │
│ 4. カラムマッピング & 型変換          │
│ 5. データ品質チェック                 │
│ 6. Delta Lake書き込み (Bronze)       │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ dbt Transformation                  │
│                                     │
│ 1. Bronze → Silver (クレンジング)    │
│ 2. Silver → Gold (集計・結合)        │
└─────────────────────────────────────┘
```

## 🚀 クイックスタート

### 1. リポジトリのクローン

```bash
git clone <repository-url>
cd lake-flow-ingestion
```

### 2. Databricks CLI設定

```bash
pip install databricks-cli
databricks configure --token
```

### 3. 環境変数の設定

`databricks.yml` を編集：

```yaml
targets:
  dev:
    workspace:
      host: https://adb-YOUR-WORKSPACE-ID.azuredatabricks.net
    variables:
      storage_account: YOUR-STORAGE-ACCOUNT
      container: raw-data
```

### 4. デプロイ

```bash
# 検証
databricks bundle validate -t dev

# デプロイ
databricks bundle deploy -t dev
```

### 5. テスト実行

テストファイルをADLS Gen2にアップロード：

```bash
az storage blob upload \
  --account-name YOUR-STORAGE-ACCOUNT \
  --container-name raw-data \
  --name sales/daily/sales_20241229.csv.gz \
  --file test_data.csv.gz
```

ジョブが自動実行されることを確認。

## 📝 設定ファイルの書き方

### CSV設定例（ヘッダーあり）

```yaml
table:
  name: sales_data
  catalog: main
  schema: bronze
  write_mode: overwrite

source:
  container: raw-data
  directory: "sales/daily/"
  file_type: csv
  
  csv_options:
    compression: gzip
    encoding: utf-8
    delimiter: ","
    header: true

columns:
  - name: sale_id
    type: STRING
    nullable: false
    source_column: "SaleID"
    
  - name: amount
    type: DECIMAL(10,2)
    nullable: false
    source_column: "Amount"

quality:
  skip_validation: false
  not_null_columns:
    - sale_id
```

### Excel設定例（シート名指定）

```yaml
table:
  name: inventory_data
  catalog: main
  schema: bronze
  write_mode: overwrite

source:
  container: raw-data
  directory: "inventory/monthly/"
  file_type: excel
  
  excel_options:
    sheet_name: "在庫マスタ"
    header: true

columns:
  - name: product_code
    type: STRING
    nullable: false
    source_column: "商品コード"

quality:
  skip_validation: false
  not_null_columns:
    - product_code
```

### ヘッダーなし（列番号指定）

```yaml
columns:
  - name: transaction_id
    type: STRING
    nullable: false
    source_column_index: 0  # 1列目（0始まり）
    
  - name: amount
    type: DECIMAL(15,2)
    nullable: false
    source_column_index: 3  # 4列目
```

詳細は `設定ファイルサンプル集` を参照。

## 🔧 サポートされるデータ型

| 設定ファイルの型 | Sparkのデータ型 | 備考 |
|---------------|---------------|------|
| STRING | StringType | 文字列 |
| INTEGER | IntegerType | 32bit整数 |
| BIGINT | LongType | 64bit整数 |
| DECIMAL(p,s) | DecimalType | 固定小数点 |
| DOUBLE | DoubleType | 浮動小数点 |
| DATE | DateType | 日付（date_format指定可） |
| TIMESTAMP | TimestampType | タイムスタンプ（timestamp_format指定可） |
| BOOLEAN | BooleanType | 真偽値 |

## 📊 データフロー

1. **ファイル到着**: ADLS Gen2にファイルがアップロードされる
2. **トリガー発火**: ファイル到着トリガーがジョブを起動
3. **設定マッチング**: ファイルパスから該当する設定ファイルを検索
4. **データ読み込み**: CSV/Excelファイルを読み込み
5. **変換**: カラムマッピング、型変換を実行
6. **検証**: データ品質チェックを実行
7. **書き込み**: Delta Lake（Bronze層）に書き込み
8. **dbt実行**: Bronze→Silver→Goldの変換を実行

## 🔍 トラブルシューティング

### Excel読み込みエラー

**エラー**: `Excel format not supported`

**解決策**: Databricks Runtime 17.1以降を使用していることを確認

```yaml
spark_version: "17.1.x-scala2.12"
```

### 設定ファイルが見つからない

**エラー**: `No matching config found`

**解決策**: 設定ファイルの `source.directory` がファイルパスに含まれているか確認

### カラムが見つからない

**エラー**: `Source column 'XXX' not found`

**解決策**: 
- ヘッダー名の綴りを確認
- ヘッダーなしの場合は `source_column_index` を使用

## 📈 モニタリング

### 取り込み履歴の確認

```sql
SELECT 
  _config_name,
  _source_file,
  _ingestion_timestamp,
  COUNT(*) as row_count
FROM main.bronze.sales_data
GROUP BY _config_name, _source_file, _ingestion_timestamp
ORDER BY _ingestion_timestamp DESC
LIMIT 10;
```

### ジョブ実行状況

Databricks UI → Workflows → Jobs → ジョブ名 から確認

## 🤝 コントリビューション

1. Featureブランチを作成
2. 変更をコミット
3. Pull Requestを作成
4. レビュー後にマージ

## 📚 ドキュメント

- [デプロイメントガイド](./docs/deployment-guide.md)
- [設定ファイルリファレンス](./docs/config-reference.md)
- [トラブルシューティング](./docs/troubleshooting.md)

## 📄 ライセンス

[ライセンスを記載]

## 👥 メンテナー

- データエンジニアリングチーム

## 📞 サポート

問題が発生した場合:
1. [Issues](https://github.com/your-org/lake-flow-ingestion/issues)で既存の問題を検索
2. 見つからない場合は新しいIssueを作成
3. 緊急の場合はSlackの #data-engineering チャンネルへ
