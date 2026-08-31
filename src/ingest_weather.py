import json
from datetime import datetime, timezone
from pathlib import Path

import requests

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
BLR_LAT, BLR_LON = 12.9716, 77.5946

def main():
    params = {
        "latitude": BLR_LAT,
        "longitude": BLR_LON,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m",
        "past_days": 2, # Get weather data for the last 2 days
        "forecast_days": 1, # Get weather data for the next 1 day
        "timezone": "Asia/Kolkata", # timestamps are IST, NOT UTC — Silver must convert before any time join
    }
    resp = requests.get(WEATHER_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"openmeteo_blr_weather_{ts}.json"
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Saved weather ({len(data['hourly']['time'])} hours) to {out_path}")

if __name__ == "__main__":
    main()
