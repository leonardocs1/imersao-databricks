CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.yfinance
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/raw_yfinance/commodities/latest_prices/',
  'json'
);