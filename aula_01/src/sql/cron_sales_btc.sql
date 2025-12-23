WITH RECURSIVE
params AS (
  SELECT (5 + floor(random()*5))::int AS n
),
series(gs) AS (
  SELECT 1
  UNION ALL
  SELECT gs + 1 FROM series, params WHERE gs < params.n
)
INSERT INTO sales_btc (
  transaction_id, data_hora, ativo, quantidade, tipo_operacao, moeda,
  cliente_id, canal, mercado, arquivo_origem, importado_em
)
SELECT
  format('BTCX-%08s', (extract(epoch FROM clock_timestamp())::bigint)*100000 + gs),
  now() - (random() * interval '5 minutes'),
  'BTC',
  round((0.01 + random()*0.49)::numeric, 2),
  CASE WHEN random() < 0.5 THEN 'COMPRA' ELSE 'VENDA' END,
  'USD',
  (SELECT customer_id FROM customers ORDER BY random() LIMIT 1),
  (ARRAY['ONLINE','DISTRIB','RETAIL'])[ceil(random()*3)::int],
  (ARRAY['BR','US','EU'])[ceil(random()*3)::int],
  'btc_cron_job',
  now()
FROM series;