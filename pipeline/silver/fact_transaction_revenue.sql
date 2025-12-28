-- =====================================================
-- Tabela Silver: fact_transaction_revenue
-- Descrição: Join transações + cotações + clientes com cálculos financeiros
-- Fonte: silver.fact_transaction_assets + silver.fact_quotation_assets + silver.dim_clientes
-- =====================================================

CREATE OR REFRESH STREAMING TABLE lakehouse.silver.fact_transaction_revenue
(
  CONSTRAINT gross_value_positivo EXPECT (gross_value > 0) ON VIOLATION DROP ROW,
  CONSTRAINT fee_revenue_positivo EXPECT (fee_revenue > 0) ON VIOLATION DROP ROW,
  CONSTRAINT customer_sk_nao_nulo EXPECT (customer_sk IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT preco_cotacao_positivo EXPECT (preco_cotacao > 0 AND timestamp_cotacao <= data_hora) ON VIOLATION DROP ROW
)
AS SELECT 
  t.transaction_id,
  t.data_hora,
  t.data_hora_aproximada,
  t.asset_symbol,
  t.quantidade,
  t.tipo_operacao,
  t.moeda,
  t.cliente_id,
  t.canal,
  t.mercado,
  t.arquivo_origem,
  t.importado_em,
  
  -- Dados da cotação
  q.preco as preco_cotacao,
  q.timestamp_cotacao,
  q.ativo_original,
  
  -- Dados do cliente
  c.customer_id as customer_sk,
  c.customer_name,
  c.segmento,
  c.pais,
  c.estado,
  c.cidade,
  
  -- Cálculos financeiros
  (t.quantidade * q.preco) as gross_value,
  
  -- Lógica do sinal: VENDA(+) / COMPRA(-)
  CASE 
    WHEN t.tipo_operacao = 'VENDA' THEN (t.quantidade * q.preco)
    WHEN t.tipo_operacao = 'COMPRA' THEN -(t.quantidade * q.preco)
    ELSE 0
  END as gross_value_sinal,
  
  -- Receita de taxa: 0.25% sobre valor total
  (t.quantidade * q.preco * 0.0025) as fee_revenue,
  
  current_timestamp() as processed_at

FROM STREAM(lakehouse.silver.fact_transaction_assets) t
INNER JOIN STREAM(lakehouse.silver.fact_quotation_assets) q
  ON t.asset_symbol = q.asset_symbol 
  AND t.data_hora_aproximada = q.data_hora_aproximada
INNER JOIN STREAM(lakehouse.silver.dim_clientes) c
  ON t.cliente_id = c.customer_id