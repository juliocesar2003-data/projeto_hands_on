# Databricks notebook source
# MAGIC %md
# MAGIC # BACEN Series Explorer
# MAGIC
# MAGIC Quick exploration of BACEN SGS series using the `dados/ultimos/10` endpoint to fetch the last 10 records from each series.

# COMMAND ----------

import requests
import pandas as pd

# BACEN SGS series to explore
series = {
    432: "Selic target set by Copom",
    12: "Daily CDI",
    226: "Daily TR",
    433: "IPCA - monthly % variation",
}

headers = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data exploration)"}

for code, name in series.items():
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/10?formato=json"
    print(f"\n{'='*60}")
    print(f"Code {code}: {name}")
    print(f"{'='*60}")

    response = requests.get(url, headers=headers, timeout=60)
    if response.status_code == 404:
        print("  -> 404: No data found for this series")
        continue
    response.raise_for_status()
    data = response.json()

    if not data:
        print("  -> Empty response")
        continue

    df = pd.DataFrame(data)
    display(df)
