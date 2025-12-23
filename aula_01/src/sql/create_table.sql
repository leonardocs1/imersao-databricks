-- Schema público (ERP simulado)
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name TEXT,
    documento TEXT,
    segmento TEXT,
    pais TEXT,
    estado TEXT,
    cidade TEXT,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sales_btc (
    transaction_id VARCHAR PRIMARY KEY,
    data_hora TIMESTAMPTZ,
    ativo TEXT,
    quantidade NUMERIC(10,4),
    tipo_operacao TEXT,
    moeda TEXT,
    cliente_id VARCHAR REFERENCES customers(customer_id),
    canal TEXT,
    mercado TEXT,
    arquivo_origem TEXT,
    importado_em TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sales_commodities (
    transaction_id VARCHAR PRIMARY KEY,
    data_hora TIMESTAMPTZ,
    commodity_code TEXT,
    quantidade NUMERIC(10,4),
    tipo_operacao TEXT,
    unidade TEXT,
    moeda TEXT,
    cliente_id VARCHAR REFERENCES customers(customer_id),
    canal TEXT,
    mercado TEXT,
    arquivo_origem TEXT,
    importado_em TIMESTAMPTZ
);
