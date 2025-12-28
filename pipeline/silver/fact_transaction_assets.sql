-- =====================================================
-- Tabela Silver: fact_transaction_assets
-- Descrição: União de transações Bitcoin e commodities com símbolos padronizados
-- Fonte: bronze.transaction_btc + bronze.transaction_commodities
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.silver.fact_transaction_assets
(
  CONSTRAINT quantidade_positiva EXPECT (quantidade > 0) ON VIOLATION DROP ROW,
  CONSTRAINT data_hora_valida EXPECT (data_hora IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT tipo_operacao_valido EXPECT (tipo_operacao IN ('COMPRA','VENDA')) ON VIOLATION DROP ROW,
  CONSTRAINT asset_symbol_valido EXPECT (asset_symbol IN ('BTC','GOLD','OIL','SILVER')) ON VIOLATION DROP ROW
)
AS SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP) as data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) as data_hora_aproximada,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  -- Mapeamento de símbolos padronizado
  CASE 
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('GOLD','GC=F')   THEN 'GOLD'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('OIL','CL=F')    THEN 'OIL'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  current_timestamp() as processed_at
FROM (
  -- Transações Bitcoin
  SELECT 
    transaction_id,
    data_hora,
    ativo,
    NULL as commodity_code,
    quantidade,
    tipo_operacao,
    moeda,
    cliente_id,
    canal,
    mercado,
    arquivo_origem,
    importado_em
  FROM STREAM(bronze.transaction_btc)
  
  UNION ALL
  
  -- Transações Commodities
  SELECT 
    transaction_id,
    data_hora,
    NULL as ativo,
    commodity_code,
    quantidade,
    tipo_operacao,
    moeda,
    cliente_id,
    canal,
    mercado,
    arquivo_origem,
    importado_em
  FROM STREAM(lakehouse.bronze.transaction_commodities)
)