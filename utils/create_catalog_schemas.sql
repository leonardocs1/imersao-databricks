-- Databricks notebook source
-- 1 Cria o catálogo principal (onde toda a plataforma de dados será organizada)
CREATE CATALOG IF NOT EXISTS lakehouse
COMMENT "Catálogo principal do projeto, com camadas de dados governadas via Unity Catalog";

-- 2 Cria as quatro camadas da arquitetura de dados
CREATE SCHEMA IF NOT EXISTS lakehouse.raw_public
COMMENT "Camada RAW - dados brutos, como chegam das fontes (SQL, API, planilhas)";
CREATE VOLUME IF NOT EXISTS lakehouse.raw_public.coinbase;
CREATE VOLUME IF NOT EXISTS lakehouse.raw_public.yfinance;

CREATE SCHEMA IF NOT EXISTS lakehouse.bronze
COMMENT "Camada BRONZE - dados padronizados, com metadados e controle de ingestão";

CREATE SCHEMA IF NOT EXISTS lakehouse.silver
COMMENT "Camada SILVER - dados tratados, com regras de negócio aplicadas";

CREATE SCHEMA IF NOT EXISTS lakehouse.gold
COMMENT "Camada GOLD - dados analíticos e métricas finais para BI e IA";

-- 3 (opcional) Verifica se tudo foi criado corretamente
SHOW SCHEMAS IN lakehouse;

-- COMMAND ----------


-- 2) Permissões (um GRANT por principal)

-- Catálogo
GRANT USE CATALOG ON CATALOG lakehouse TO data_engineers;
GRANT USE CATALOG ON CATALOG lakehouse TO data_analysts;
GRANT USE CATALOG ON CATALOG lakehouse TO data_scientists;
GRANT USE CATALOG ON CATALOG lakehouse TO business_users;

-- RAW (restrito a engenheiros)
GRANT USE SCHEMA ON SCHEMA lakehouse.raw_public TO data_engineers;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.raw TO data_engineers;

-- BRONZE (eng: criar/modificar; analistas: leitura)
GRANT USE SCHEMA ON SCHEMA lakehouse.bronze TO data_engineers;
GRANT USE SCHEMA ON SCHEMA lakehouse.bronze TO data_analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.bronze TO data_analysts;

-- SILVER (leitura p/ times técnicos)
GRANT USE SCHEMA ON SCHEMA lakehouse.silver TO data_engineers;
GRANT USE SCHEMA ON SCHEMA lakehouse.silver TO data_analysts;
GRANT USE SCHEMA ON SCHEMA lakehouse.silver TO data_scientists;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.silver TO data_engineers;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.silver TO data_analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.silver TO data_scientists;

-- GOLD (leitura ampla, incluindo negócio)
GRANT USE SCHEMA ON SCHEMA lakehouse.gold TO data_engineers;
GRANT USE SCHEMA ON SCHEMA lakehouse.gold TO data_analysts;
GRANT USE SCHEMA ON SCHEMA lakehouse.gold TO data_scientists;
GRANT USE SCHEMA ON SCHEMA lakehouse.gold TO business_users;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.gold TO data_engineers;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.gold TO data_analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.gold TO data_scientists;
-- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse.gold TO business_users;

