-- =====================================================
-- Tabela Bronze: quotation_yfinance
-- Descrição: Ingestão de cotações yFinance do volume CSV
-- Fonte: /Volumes/lakehouse/raw_public/quotation_yfinance
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.bronze.quotation_yfinance
AS SELECT 
  ativo,
  preco,
  moeda,
  horario_coleta,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/quotation_yfinance",
  "csv",
  map("header", "true", "inferSchema", "true")
)