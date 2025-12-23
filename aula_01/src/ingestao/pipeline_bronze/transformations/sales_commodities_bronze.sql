CREATE OR REFRESH LIVE TABLE lakehouse.bronze.sales_commodities
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM postgres_coin.public.sales_commodities;
