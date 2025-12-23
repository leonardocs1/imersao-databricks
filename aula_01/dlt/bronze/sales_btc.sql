CREATE OR REFRESH LIVE TABLE lakehouse.bronze.sales_btc
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM postgres_coin.public.sales_btc;