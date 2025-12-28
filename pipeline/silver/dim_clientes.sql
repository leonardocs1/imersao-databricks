-- =====================================================
-- Tabela Silver: dim_clientes
-- Descrição: Dimensão de clientes com anonimização e validações
-- Fonte: bronze.customers
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.silver.dim_clientes
(
  CONSTRAINT customer_id_nao_nulo EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT segmento_valido EXPECT (segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')) ON VIOLATION DROP ROW,
  CONSTRAINT pais_valido EXPECT (pais IN ('Brasil', 'Alemanha', 'Estados Unidos')) ON VIOLATION DROP ROW,
  CONSTRAINT estado_brasil_valido EXPECT (
    CASE 
      WHEN pais = 'Brasil' THEN estado IN ('SE','RS', 'PA', 'AL', 'MT', 'SP', 'RJ', 'MG', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE', 'MA', 'PB', 'AM', 'ES', 'RN', 'AC', 'AP', 'RO', 'RR', 'TO', 'MS', 'DF')
      ELSE true
    END
  ) ON VIOLATION DROP ROW
)
AS SELECT 
  customer_id,
  customer_name,
  -- Anonimização do documento usando SHA2
  SHA2(documento, 256) as documento_hash,
  segmento,
  pais,
  estado,
  cidade,
  CAST(created_at AS TIMESTAMP) as created_at,
  current_timestamp() as processed_at
FROM STREAM(lakehouse.bronze.customers)