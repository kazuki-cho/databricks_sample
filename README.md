# ADLS Auto Loader - CSV.GZ Ingestion

ADLS上のCSV.GZファイルを自動的にDeltaテーブルに取り込むDatabricks Asset Bundle構成です。

## ファイル構造

```
bundle/
├── databricks.yml                    # メイン設定ファイル
├── resources/
│   └── jobs/
│       └── autoloader_ingestion.yml  # ジョブ定義
└── notebooks/
    ├── autoloader_ingest.py          # メイン処理ノートブック
    └── utilities.py                  # ユーティリティ関数
```

## 前提条件

### 1. ADLS接続の設定

Databricksワークスペースから、ADLS Gen2へのアクセスを設定してください。

#### 方法A: サービスプリンシパル（推奨）

```python
# Databricks Secretsに保存
spark.conf.set(
    "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get("azure", "client-id")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get("azure", "client-secret")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)
```

#### 方法B: アクセスキー

```python
spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-key")
)
```

### 2. カタログとスキーマの作成

```sql
-- カタログの作成
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE CATALOG IF NOT EXISTS prod_catalog;

-- スキーマは自動作成されますが、事前に作成することも可能
CREATE SCHEMA IF NOT EXISTS dev_catalog.sales;
CREATE SCHEMA IF NOT EXISTS dev_catalog.inventory;
```

## セットアップ

### 1. 設定ファイルの編集

`databricks.yml`を環境に合わせて編集：

```yaml
targets:
  dev:
    variables:
      storage_account: "your-dev-storage"
      container_name: "your-dev-container"
      checkpoint_base_path: "abfss://your-dev-container@your-dev-storage.dfs.core.windows.net/checkpoints/"
  
  prod:
    variables:
      storage_account: "your-prod-storage"
      container_name: "your-prod-container"
      checkpoint_base_path: "abfss://your-prod-container@your-prod-storage.dfs.core.windows.net/checkpoints/"
```

### 2. バリデーション

```bash
databricks bundle validate -t dev
```

### 3. デプロイ

```bash
# 開発環境
databricks bundle deploy -t dev

# 本番環境
databricks bundle deploy -t prod
```

## ファイル配置ルール

### ディレクトリ構造

```
container/
├── schema_name_1/
│   ├── table_name_1/
│   │   └── tmp/
│   │       ├── data_2024-12-14_10:30:45.csv.gz
│   │       └── data_2024-12-14_11:00:00.csv.gz
│   └── table_name_2/
│       └── tmp/
│           └── export_2024-12-14_09:15:30.csv.gz
└── schema_name_2/
    └── table_name_3/
        └── tmp/
            └── records_2024-12-14_08:00:00.csv.gz
```

### ファイル命名規則

- **パターン**: `{prefix}_YYYY-mm-dd_HH:MM:SS.csv.gz`
- **例**:
  - `sales_data_2024-12-14_10:30:45.csv.gz`
  - `customer_records_2024-12-14_14:00:00.csv.gz`
  - `inventory_2024-12-14_09:00:00.csv.gz`

### CSVフォーマット

```csv
Customer Name,Order Date,Product (ID),Price $,Quantity #,Total	Amount
John Doe,2024-12-14,PROD-001,100.50,2,201.00
Jane Smith,2024-12-14,PROD-002,50.00,5,250.00
```

**注意点:**
- ヘッダー行が必須
- カラム名に特殊文字（スペース、記号等）が含まれていても自動で正規化されます
- gzip圧縮されたCSVファイル（.csv.gz）

## カラム名の正規化

以下のルールでカラム名が自動的に正規化されます：

| 元のカラム名 | 正規化後 |
|------------|---------|
| `Customer Name` | `customer_name` |
| `Order Date` | `order_date` |
| `Product (ID)` | `product_id` |
| `Price $` | `price_` |
| `Total	Amount` | `total_amount` |
| `Email-Address` | `email_address` |
| `  Leading Space` | `leading_space` |
| `123StartWithNumber` | `_123startwithnumber` |

**正規化ルール:**
1. 前後の空白を削除
2. 空白文字（スペース、タブ等）を`_`に置換
3. 特殊文字を`_`に置換
4. 連続する`_`を1つに圧縮
5. 小文字に変換
6. 先頭が数字の場合は`_`を追加

## 自動追加されるメタデータカラム

各レコードに以下のメタデータが自動追加されます：

| カラム名 | 説明 |
|---------|------|
| `_ingestion_time` | 取り込み日時 |
| `_source_file` | ソースファイルのフルパス |
| `_schema_name` | スキーマ名 |
| `_table_name` | テーブル名 |
| `_batch_id` | バッチID |
| `_corrupted_record` | 破損レコード（エラー時のみ） |

## テーブルの確認

### 1. デプロイされたリソースの確認

```bash
databricks bundle summary -t dev
```

### 2. テーブルの確認

```sql
-- テーブル一覧
SHOW TABLES IN dev_catalog.sales;

-- テーブル内容の確認
SELECT * FROM dev_catalog.sales.transactions LIMIT 10;

-- 最近取り込まれたデータ
SELECT 
    DATE(_ingestion_time) as date,
    COUNT(*) as rows,
    COUNT(DISTINCT _source_file) as files
FROM dev_catalog.sales.transactions
WHERE _ingestion_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY DATE(_ingestion_time)
ORDER BY date DESC;
```

### 3. ユーティリティ関数を使った確認

```python
# notebooks/utilities.pyをインポート
%run ./utilities

# テーブル統計を表示
get_table_stats("dev_catalog", "sales", "transactions")

# データ品質チェック
check_data_quality("dev_catalog", "sales", "transactions")

# チェックポイント一覧
display_checkpoints("abfss://dev-container@devstorage.dfs.core.windows.net/checkpoints/")
```

## トラブルシューティング

### ファイルが処理されない

1. **ファイルパターンの確認**
   ```python
   # ファイル名が正規表現に一致するか確認
   import re
   pattern = r'^.+_\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.csv\.gz$'
   file_name = "your_file_name.csv.gz"
   print(re.match(pattern, file_name))
   ```

2. **ディレクトリ構造の確認**
   ```python
   # 期待される構造: schema_name/table_name/tmp/file.csv.gz
   dbutils.fs.ls("abfss://container@storage.dfs.core.windows.net/sales/transactions/tmp/")
   ```

3. **ジョブログの確認**
   - Databricks UIでジョブの実行履歴を確認
   - エラーメッセージを確認

### チェックポイントのリセット

処理を最初からやり直したい場合：

```python
%run ./utilities

# 特定のテーブルのチェックポイントをリセット
reset_table_checkpoint(
    "abfss://container@storage.dfs.core.windows.net/checkpoints/",
    "sales",
    "transactions",
    confirm=True
)
```

### テーブルの再作成

```python
%run ./utilities

# テーブルを削除
drop_table("dev_catalog", "sales", "transactions", confirm=True)

# チェックポイントも削除
reset_table_checkpoint(
    "abfss://container@storage.dfs.core.windows.net/checkpoints/",
    "sales",
    "transactions",
    confirm=True
)

# 次回のファイル到達時に自動的に再作成されます
```

### 破損レコードの確認

```sql
-- 破損レコードを表示
SELECT 
    _source_file,
    _corrupted_record,
    _ingestion_time
FROM dev_catalog.sales.transactions
WHERE _corrupted_record IS NOT NULL
ORDER BY _ingestion_time DESC
LIMIT 100;
```

## 運用

### モニタリング

1. **Databricks Jobs UI**
   - ジョブの実行履歴
   - 成功/失敗の確認
   - 実行時間の監視

2. **メール通知**
   - 失敗時に自動通知（`databricks.yml`に設定済み）

3. **データ品質監視**
   ```python
   %run ./utilities
   
   # 定期的にデータ品質をチェック
   check_data_quality("dev_catalog", "sales", "transactions")
   ```

### バックアップとリカバリ

1. **Deltaテーブルのタイムトラベル**
   ```sql
   -- 1日前の状態に戻す
   RESTORE TABLE dev_catalog.sales.transactions TO VERSION AS OF 1 DAY AGO;
   
   -- 特定のバージョンに戻す
   RESTORE TABLE dev_catalog.sales.transactions TO VERSION AS OF 42;
   ```

2. **チェックポイントのバックアップ**
   ```bash
   # 定期的にチェックポイントをバックアップ
   az storage blob copy start-batch \
     --source-container checkpoints \
     --destination-container checkpoints-backup
   ```

## コスト最適化

1. **クラスタのオートスケール**
   - 既に設定済み（2-8ワーカー）

2. **Delta最適化**
   ```sql
   -- 定期的に実行
   OPTIMIZE dev_catalog.sales.transactions
   ZORDER BY (_ingestion_time);
   
   -- 古いファイルのクリーンアップ
   VACUUM dev_catalog.sales.transactions RETAIN 168 HOURS;
   ```

3. **ジョブの並列実行制御**
   - `max_concurrent_runs: 5`で制限済み

## セキュリティ

1. **サービスプリンシパルの使用**
   - 本番環境では必ずサービスプリンシパルを使用

2. **Secretsの管理**
   ```bash
   # Azure Key Vaultと統合
   databricks secrets create-scope --scope azure
   databricks secrets put --scope azure --key client-id
   databricks secrets put --scope azure --key client-secret
   ```

3. **アクセス制御**
   ```sql
   -- テーブルへのアクセス権限を設定
   GRANT SELECT ON TABLE dev_catalog.sales.transactions TO `data-analysts`;
   GRANT ALL PRIVILEGES ON SCHEMA dev_catalog.sales TO `data-engineers`;
   ```

## サポート

問題が発生した場合：

1. ジョブログを確認
2. `utilities.py`の診断機能を使用
3. チェックポイントとテーブルの状態を確認
4. 必要に応じてチェックポイントをリセット

