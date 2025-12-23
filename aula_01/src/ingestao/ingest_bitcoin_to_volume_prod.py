# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lakehouse;
# MAGIC CREATE SCHEMA  IF NOT EXISTS lakehouse.raw;
# MAGIC CREATE SCHEMA  IF NOT EXISTS lakehouse.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volume para zona raw (arquivos JSON)
# MAGIC CREATE VOLUME IF NOT EXISTS lakehouse.raw.raw_coinbase;

# COMMAND ----------

# Databricks notebook source
# COMMAND ----------
import requests
import pandas as pd
from datetime import datetime, UTC  # ou timezone.utc se versão <3.11
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

RAW_BASE_PATH = "/Volumes/lakehouse/raw/raw_coinbase/coinbase/bitcoin_spot"

def get_bitcoin_df() -> pd.DataFrame:
    url = "https://api.coinbase.com/v2/prices/spot?currency=USD"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()

    preco = float(data["data"]["amount"])
    ativo = data["data"]["base"]
    moeda = data["data"]["currency"]
    horario_coleta = datetime.now(UTC)   # horário UTC correto

    return pd.DataFrame([{
        "ativo": ativo,
        "preco": preco,
        "moeda": moeda,
        "horario_coleta": horario_coleta,
    }])

# Coleta
pdf = get_bitcoin_df()

# Schema explícito (opcional, deixa claro os tipos)
schema = StructType([
    StructField("ativo", StringType(), False),
    StructField("preco", DoubleType(), False),
    StructField("moeda", StringType(), False),
    StructField("horario_coleta", TimestampType(), False),
])

df = (
    spark.createDataFrame(pdf, schema=schema)
        .withColumn("ingestion_ts_utc", F.current_timestamp())
        .withColumn("source_system", F.lit("coinbase"))
        .withColumn("source_endpoint", F.lit("https://api.coinbase.com/v2/prices/spot?currency=USD"))
        .withColumn("ingestion_date", F.to_date(F.col("ingestion_ts_utc")))
)

# Grava os arquivos JSON particionados por data de ingestão
(
    df.write
      .mode("append")
      .partitionBy("ingestion_date")
      .json(RAW_BASE_PATH)
)

print("✅ JSON salvo em:", RAW_BASE_PATH)
