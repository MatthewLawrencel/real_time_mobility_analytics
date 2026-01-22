import pandas as pd
import pyodbc
import os
from yaml import safe_load

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

cfg = safe_load(open(CONFIG_PATH))
conn_str = cfg["mssql_conn"]

parquet_path = os.path.join(BASE_DIR, "data", "processed", "fact_mobility.parquet")

if not os.path.exists(parquet_path):
    raise SystemExit("Missing fact_mobility.parquet. Run transform_clean.py first.")

df = pd.read_parquet(parquet_path)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

insert_sql = """
INSERT INTO fact_mobility (
    city, [timestamp], temp_c, humidity, wind_speed, precip_mm,
    weather, congestion_index, is_rainy, is_humid, is_windy, mobility_stress_score
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

rows_inserted = 0

for _, r in df.iterrows():
    cursor.execute(
        insert_sql,
        r.city,
        pd.to_datetime(r.timestamp),
        float(r.temp_c),
        int(r.humidity),
        float(r.wind_speed),
        float(r.precip_mm),
        r.weather if pd.notna(r.weather) else None,
        float(r.congestion_index),
        int(r.is_rainy),
        int(r.is_humid),
        int(r.is_windy),
        int(r.mobility_stress_score),
    )
    rows_inserted += 1

conn.commit()
cursor.close()
conn.close()

print("Loaded into MS SQL Server: mobility_dw.fact_mobility")
print("Rows inserted:", rows_inserted)
