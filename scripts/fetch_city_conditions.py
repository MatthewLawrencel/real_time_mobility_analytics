import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from yaml import safe_load
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

cfg = safe_load(open(CONFIG_PATH))
cities = cfg["cities"]

records = []

# ---- CONFIG ----
DAYS_BACK = 10        # 10 days × 24 hrs × 7 cities ≈ 1680 rows
SLEEP_BETWEEN_CALLS = 1.2
MAX_RETRIES = 3

end_date = datetime.utcnow().date()
start_date = end_date - timedelta(days=DAYS_BACK)

date_range = [
    start_date + timedelta(days=i)
    for i in range((end_date - start_date).days + 1)
]

print(f"⏳ Backfilling {len(date_range)} days for {len(cities)} cities...")

session = requests.Session()

for city in cities:
    name = city["name"]
    lat = city["lat"]
    lon = city["lon"]

    print(f"\n Fetching previous data for {name}...")

    for d in date_range:
        date_str = d.strftime("%Y-%m-%d")

        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
            f"&start_date={date_str}&end_date={date_str}"
            "&timezone=UTC"
        )

        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, timeout=25)
                data = resp.json()

                if "hourly" not in data:
                    raise ValueError("Missing hourly block")

                hourly = data["hourly"]

                for i in range(len(hourly["time"])):
                    ts_hour = datetime.fromisoformat(hourly["time"][i])

                    congestion_index = round(
                        0.4 * (hourly["precipitation"][i] > 0) +
                        0.3 * (hourly["wind_speed_10m"][i] > 20) +
                        0.3 * (hourly["relative_humidity_2m"][i] > 80),
                        2
                    )

                    records.append({
                        "city": name,
                        "temp_c": hourly["temperature_2m"][i],
                        "humidity": hourly["relative_humidity_2m"][i],
                        "wind_speed": hourly["wind_speed_10m"][i],
                        "precip_mm": hourly["precipitation"][i],
                        "congestion_index": congestion_index,
                        "timestamp": ts_hour.isoformat()
                    })

                success = True
                break

            except Exception as e:
                print(f"⚠ {name} {date_str} attempt {attempt} failed:", e)
                time.sleep(2)

        if not success:
            print(f"Skipping {name} on {date_str} after {MAX_RETRIES} failures")

        time.sleep(SLEEP_BETWEEN_CALLS)

df = pd.DataFrame(records)

output_path = os.path.join(BASE_DIR, "data", "raw", "city_conditions.csv")
df.to_csv(output_path, index=False)

print("\n Historical city conditions generated")
print("Rows:", len(df))
print("Saved to:", output_path)
