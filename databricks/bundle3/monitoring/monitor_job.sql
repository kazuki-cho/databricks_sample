SELECT 
  _config_name,
  _source_file,
  _ingestion_timestamp,
  COUNT(*) as row_count
FROM main.bronze.sales_data
GROUP BY _config_name, _source_file, _ingestion_timestamp
ORDER BY _ingestion_timestamp DESC
LIMIT 10;
