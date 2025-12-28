-- =====================================================
-- Tabela Bronze: quotation_btc
-- Descrição: Ingestão de cotações Bitcoin do volume CSV
-- Fonte: /Volumes/lakehouse/raw_public/quotation_btc
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.bronze.quotation_btc
AS SELECT 
  ativo,
  preco,
  moeda,
  horario_coleta,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/quotation_btc",
  "csv",
  map("header", "true", "inferSchema", "true")
)