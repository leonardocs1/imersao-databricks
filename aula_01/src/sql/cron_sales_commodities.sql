WITH RECURSIVE
params2 AS (
  SELECT (5 + floor(random()*5))::int AS n
),
series2(gs) AS (
  SELECT 1
  UNION ALL
  SELECT gs + 1 FROM series2, params2 WHERE gs < params2.n
),
choice AS (
  SELECT
    gs,
    (ARRAY['GOLD','OIL','SILVER'])[ceil(random()*3)::int] AS code
  FROM series2
)
INSERT INTO sales_commodities (
  transaction_id, data_hora, commodity_code, quantidade, tipo_operacao, unidade, moeda,
  cliente_id, canal, mercado, arquivo_origem, importado_em
)
SELECT
  format('COM-%08s', (extract(epoch FROM clock_timestamp())::bigint)*100000 + gs),
  now() - (random() * interval '5 minutes'),
  code,
  (10 + floor(random()*40))::int,
  CASE WHEN random() < 0.5 THEN 'COMPRA' ELSE 'VENDA' END,
  CASE code WHEN 'OIL' THEN 'bbl' ELSE 'oz' END,
  'USD',
  (SELECT customer_id FROM customers ORDER BY random() LIMIT 1),
  (ARRAY['ONLINE','DISTRIB','RETAIL'])[ceil(random()*3)::int],
  (ARRAY['BR','US','EU'])[ceil(random()*3)::int],
  'commodities_cron_job',
  now()
FROM choice;