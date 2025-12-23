# Databricks notebook source
# MAGIC %sql
# MAGIC -- Volume para zona raw (arquivos JSON)
# MAGIC CREATE VOLUME IF NOT EXISTS lakehouse.raw_public.raw_yfinance

# COMMAND ----------

%pip install yfinance

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


import yfinance as yf
import pandas as pd
from datetime import datetime, UTC

# Caminho do Volume UC (Raw Zone)
RAW_BASE_PATH = "/Volumes/lakehouse/raw_public/raw_yfinance/commodities/latest_prices"

# Garante que o diretório existe (Databricks)
dbutils.fs.mkdirs(RAW_BASE_PATH)

def get_commodities_df() -> pd.DataFrame:
    """
    Retorna as últimas cotações (1 minuto) de Ouro, Petróleo e Prata via Yahoo Finance.
    """
    symbols = ["GC=F", "CL=F", "SI=F"]  # Ouro, Petróleo, Prata
    dfs = []

    for sym in symbols:
        try:
            # Histórico de 1 dia com intervalo de 1 minuto, pega o último
            ultimo_df = yf.Ticker(sym).history(period="1d", interval="1m")[["Close"]].tail(1)
            if ultimo_df.empty:
                continue

            ultimo_df = ultimo_df.rename(columns={"Close": "preco"})
            ultimo_df["ativo"] = sym
            ultimo_df["moeda"] = "USD"
            ultimo_df["horario_coleta"] = datetime.now(UTC).isoformat()
            dfs.append(ultimo_df[["ativo", "preco", "moeda", "horario_coleta"]])

        except Exception as e:
            print(f"⚠️ Erro ao buscar {sym}: {e}")

    if not dfs:
        raise ValueError("Nenhuma cotação retornada pelo Yahoo Finance.")

    df = pd.concat(dfs, ignore_index=True)
    df["source_system"] = "yfinance"
    df["source_endpoint"] = "https://finance.yahoo.com"
    df["ingestion_ts_utc"] = datetime.now(UTC).isoformat()

    return df


# ===== 1️⃣ Coleta =====
df = get_commodities_df()

# ===== 2️⃣ Caminho de salvamento =====
file_name = f"{RAW_BASE_PATH}/yfinance_commodities_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"

# ===== 3️⃣ Escrita simples em JSON =====
df.to_json(file_name, orient="records", lines=True, force_ascii=False)

print(f"✅ JSON salvo em: {file_name}")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED lakehouse.bronze.sales_btc;