import os
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

OCM_URL = "https://api.openchargemap.io/v3/poi/"

def main():
    api_key = os.environ["OCM_API_KEY"]
    headers = {"x-api-key": api_key}
    params = {
    "output": "json",
    "latitude": "12.9716",     # Bangalore center
    "longitude": "77.5946",
    "distance": "30",          # km — covers the bbox corners
    "distanceunit": "KM",
    "maxresults": 200,
    "compact": "true",
    "verbose": "false",
}

    resp = requests.get(OCM_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    LAT_MIN, LAT_MAX = 12.83, 13.14
    LON_MIN, LON_MAX = 77.35, 77.75

    stations = [
        p for p in resp.json()
        if LAT_MIN <= p["AddressInfo"]["Latitude"] <= LAT_MAX
        and LON_MIN <= p["AddressInfo"]["Longitude"] <= LON_MAX
    ]


    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"ocm_blr_stations_{ts}.json"
    out_path.write_text(json.dumps(stations, indent=2), encoding="utf-8")

    print(f"Saved {len(stations)} stations to {out_path}")

if __name__ == "__main__":
    main()
