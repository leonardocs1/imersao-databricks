CREATE OR REFRESH LIVE TABLE lakehouse.bronze.customers
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM postgres_coin.public.customers;