import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

weather_path = os.path.join(BASE_DIR, "data", "raw", "weather.csv")
city_path = os.path.join(BASE_DIR, "data", "raw", "city_conditions.csv")

if not os.path.exists(weather_path):
    raise SystemExit("❌ Missing weather.csv. Run fetch_weather.py first.")

if not os.path.exists(city_path):
    raise SystemExit("Missing city_conditions.csv. Run fetch_city_conditions.py first.")

weather = pd.read_csv(weather_path)
cities = pd.read_csv(city_path)

# ---- Standardize column names ----
weather.columns = weather.columns.str.lower()
cities.columns = cities.columns.str.lower()

# ---- Normalize timestamps to minute grain ----
weather["timestamp"] = pd.to_datetime(weather["timestamp"]).dt.floor("min")
cities["timestamp"] = pd.to_datetime(cities["timestamp"]).dt.floor("min")

# ---- Merge single-city detailed weather into multi-city feed ----
fact = cities.merge(
    weather[["city", "temp_c", "humidity", "weather", "timestamp"]],
    on=["city", "timestamp"],
    how="left",
    suffixes=("", "_detail")
)

# ---- Fill missing detailed weather with city feed values ----
fact["temp_c"] = fact["temp_c"].fillna(fact["temp_c"])
fact["humidity"] = fact["humidity"].fillna(fact["humidity"])

# ---- Feature engineering ----
fact["is_rainy"] = (fact["precip_mm"] > 0).astype(int)
fact["is_humid"] = (fact["humidity"] > 75).astype(int)
fact["is_windy"] = (fact["wind_speed"] > 20).astype(int)

# Final KPI: mobility stress score (0–100)
fact["mobility_stress_score"] = (
    fact["congestion_index"] * 40 +
    fact["is_rainy"] * 25 +
    fact["is_humid"] * 20 +
    fact["is_windy"] * 15
).round(0)

# ---- Reorder columns ----
fact = fact[[
    "city",
    "timestamp",
    "temp_c",
    "humidity",
    "wind_speed",
    "precip_mm",
    "weather",
    "congestion_index",
    "is_rainy",
    "is_humid",
    "is_windy",
    "mobility_stress_score"
]]

# ---- Write outputs ----
processed_csv = os.path.join(BASE_DIR, "data", "processed", "fact_mobility.csv")
processed_parquet = os.path.join(BASE_DIR, "data", "processed", "fact_mobility.parquet")

fact.to_csv(processed_csv, index=False)
fact.to_parquet(processed_parquet, index=False)

print("Fact table created")
print("Rows:", len(fact))
print("Saved CSV:", processed_csv)
print("Saved Parquet:", processed_parquet)
