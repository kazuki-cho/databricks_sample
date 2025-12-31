# config/tables/inventory_excel.yaml
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
    sheet_name: "在庫マスタ"  # またはインデックス: 0
    header: true
    cell_range: "A1:D100"  # オプション: セル範囲を限定

columns:
  - name: product_code
    type: STRING
    nullable: false
    source_column: "商品コード"  # ヘッダー名
    
  - name: product_name
    type: STRING
    nullable: false
    source_column: "商品名"

```
**ビルトインExcelの制約:**
パスワード保護ファイルは非サポート、ヘッダー行は1行のみサポート、マージセルは左上のセルのみ値が入り他はNULL 

### **オプション2: Excelを事前にCSVに変換**

ビルトイン機能が使えない、またはより複雑な処理が必要な場合:

1. **Azure Data Factory/Synapse Pipelineで前処理**
   - Excelファイル到着時に自動でCSV変換
   - 変換後のCSVをADLS Gen2に配置
   - Databricksは変換後のCSVを処理

2. **Python処理用の中間レイヤー**
   - Serverlessとは別に通常のジョブクラスターを使用
   - `pandas` + `openpyxl`でExcelをCSV変換
   - 変換後はServerless Lakeflowで処理

## 実装の判断基準
```
├─ DBR 17.1以降 + シンプルなExcel
│   → ビルトインExcelサポート使用（最も簡単）
│
├─ DBR 17.1未満 or 複雑なExcel処理が必要
│   → 前処理でCSV変換（ADFまたは通常クラスター）
│
└─ Serverlessの制約が厳しすぎる場合
    → Lake Flowジョブ自体を通常のジョブクラスターで実行
       （Serverlessは諦める）

