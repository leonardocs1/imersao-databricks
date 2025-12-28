CREATE OR REFRESH STREAMING TABLE lakehouse.silver.fact_quotation_assets
(
  CONSTRAINT preco_positivo EXPECT (preco > 0) ON VIOLATION DROP ROW,
  CONSTRAINT horario_coleta_valido EXPECT (timestamp_cotacao <= current_timestamp()) ON VIOLATION DROP ROW,
  CONSTRAINT ativo_nao_vazio EXPECT (ativo_original IS NOT NULL AND ativo_original != '') ON VIOLATION DROP ROW,
  CONSTRAINT moeda_usd EXPECT (moeda = 'USD') ON VIOLATION DROP ROW
)
AS SELECT 
  CASE 
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GOLD','GC=F')   THEN 'GOLD'
    WHEN UPPER(ativo) IN ('OIL','CL=F')    THEN 'OIL'
    WHEN UPPER(ativo) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  ativo as ativo_original,
  preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) as timestamp_cotacao,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  current_timestamp() as processed_at
FROM (
  SELECT 
    ativo,
    preco,
    moeda,
    horario_coleta
  FROM STREAM(lakehouse.bronze.quotation_btc)
  
  UNION ALL
  
  SELECT 
    ativo,
    preco,
    moeda,
    horario_coleta
  FROM STREAM(lakehouse.bronze.quotation_yfinance)
)