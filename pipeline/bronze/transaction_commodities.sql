-- =====================================================
-- Tabela Bronze: transaction_commodities
-- Descrição: Ingestão de transações de commodities do volume CSV
-- Fonte: /Volumes/lakehouse/raw_public/transaction_commodities
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.bronze.transaction_commodities
AS SELECT 
  transaction_id,
  data_hora,
  commodity_code,
  quantidade,
  tipo_operacao,
  unidade,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/transaction_commodities",
  "csv",
  map("header", "true", "inferSchema", "true")
)