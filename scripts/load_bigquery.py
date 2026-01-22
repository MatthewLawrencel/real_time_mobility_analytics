from google.cloud import bigquery
import pandas as pd
import os
import time
from yaml import safe_load

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

cfg = safe_load(open(CONFIG_PATH))

project_id = cfg["project_id"]
dataset_id = cfg["bq_dataset"]
table_id = cfg["bq_table"]

parquet_path = os.path.join(BASE_DIR, "data", "processed", "fact_mobility.parquet")

if not os.path.exists(parquet_path):
    raise SystemExit("Missing fact_mobility.parquet. Run transform_clean.py first.")

client = bigquery.Client(project=project_id)

df = pd.read_parquet(parquet_path)

full_table_id = f"{project_id}.{dataset_id}.{table_id}"

schema = [
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("temp_c", "FLOAT"),
    bigquery.SchemaField("humidity", "INTEGER"),
    bigquery.SchemaField("wind_speed", "FLOAT"),
    bigquery.SchemaField("precip_mm", "FLOAT"),
    bigquery.SchemaField("weather", "STRING"),
    bigquery.SchemaField("congestion_index", "FLOAT"),
    bigquery.SchemaField("is_rainy", "INTEGER"),
    bigquery.SchemaField("is_humid", "INTEGER"),
    bigquery.SchemaField("is_windy", "INTEGER"),
    bigquery.SchemaField("mobility_stress_score", "INTEGER"),
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_APPEND"
)

# ---- RETRY LOGIC FOR 503 ERRORS ----
max_retries = 3
for attempt in range(1, max_retries + 1):
    try:
        job = client.load_table_from_dataframe(
            df,
            full_table_id,
            job_config=job_config
        )
        job.result()
        print("Loaded into BigQuery table:", full_table_id)
        print("Rows loaded:", len(df))
        break
    except Exception as e:
        print(f"⚠ Attempt {attempt} failed:", e)
        if attempt == max_retries:
            raise
        print("Retrying in 10 seconds...")
        time.sleep(10)
