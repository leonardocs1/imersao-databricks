-- =====================================================
-- Tabela Bronze: customers
-- Descrição: Ingestão de dados de clientes do volume CSV
-- Fonte: /Volumes/lakehouse/raw_public/customers
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.bronze.customers
AS SELECT 
  customer_id,
  customer_name,
  documento,
  segmento,
  pais,
  estado,
  cidade,
  created_at,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/customers",
  "csv",
  map("header", "true", "inferSchema", "true")
)