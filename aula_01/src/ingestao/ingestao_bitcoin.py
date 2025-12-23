# Databricks notebook source
import requests
import pandas as pd
from datetime import datetime, UTC

RAW_BASE_PATH = "/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot"
URL = "https://api.coinbase.com/v2/prices/spot?currency=USD"

# 1️⃣ Garante que o diretório existe
dbutils.fs.mkdirs(RAW_BASE_PATH)

# 2️⃣ Coleta
response = requests.get(URL, timeout=15)
response.raise_for_status()
data = response.json()["data"]

# 3️⃣ Monta DataFrame
df = pd.DataFrame([{
    "ativo": data["base"],
    "preco": float(data["amount"]),
    "moeda": data["currency"],
    "horario_coleta": datetime.now(UTC).isoformat(),
    "source_system": "coinbase",
    "source_endpoint": URL,
    "ingestion_ts_utc": datetime.now(UTC).isoformat(),
}])

# 4️⃣ Salva JSON simples
file_name = f"{RAW_BASE_PATH}/coinbase_btc_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
df.to_json(file_name, orient="records", lines=True, force_ascii=False)

print(f"✅ JSON salvo em: {file_name}")
