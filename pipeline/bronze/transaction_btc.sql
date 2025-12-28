-- =====================================================
-- Tabela Bronze: transaction_btc
-- Descrição: Ingestão de transações Bitcoin do volume CSV
-- Fonte: /Volumes/lakehouse/raw_public/transacation_btc
-- =====================================================

CREATE OR REFRESH STREAMING TABLE bronze.transaction_btc
AS SELECT 
  transaction_id,
  data_hora,
  ativo,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/transaction_btc",
  "csv",
  map("header", "true", "inferSchema", "true")
)