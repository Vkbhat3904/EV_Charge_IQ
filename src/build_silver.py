import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

def latest_snapshot_path() -> Path:
    snapshots = sorted(BRONZE_DIR.glob("ocm_blr_stations_*.json"))
    if not snapshots:
        raise FileNotFoundError("No bronze snapshots found — run ingest_ocm.py first")
    return snapshots[-1]

def build_stations(raw: list[dict], source_file: str) -> pd.DataFrame:
    df = pd.json_normalize(raw)
    stations = pd.DataFrame({
        "station_id": df["ID"],
        "title": df["AddressInfo.Title"],
        "latitude": df["AddressInfo.Latitude"],
        "longitude": df["AddressInfo.Longitude"],
        "town": df["AddressInfo.Town"],
        "operator_id": df["OperatorID"],
        "status_type_id": df["StatusTypeID"],
        "date_last_status_update": pd.to_datetime(df["DateLastStatusUpdate"], utc=True),
    })
    # dedup rule from the design note: latest DateLastStatusUpdate wins
    stations = (stations
                .sort_values("date_last_status_update")
                .drop_duplicates("station_id", keep="last"))
    return stations

def build_connections(raw: list[dict], source_file: str) -> pd.DataFrame:
    rows = []
    for p in raw:
        for c in (p.get("Connections") or []):
            rows.append({
                "connection_id": c.get("ID"),
                "station_id": p["ID"],
                "connection_type_id": c.get("ConnectionTypeID"),
                "power_kw": c.get("PowerKW"),
                "quantity": c.get("Quantity"),
                "status_type_id": c.get("StatusTypeID"),
            })
    return pd.DataFrame(rows)

def main():
    snap = latest_snapshot_path()
    raw = json.loads(snap.read_text(encoding="utf-8"))
    ref = json.loads((BRONZE_DIR / "ocm_reference_data.json").read_text(encoding="utf-8"))

    stations = build_stations(raw, snap.name)
    conns = build_connections(raw, snap.name)

    # derived: n_connectors, max_power_kw (from facts, before dedup would be wrong — after)
    conn_agg = (conns.groupby("station_id")
                      .agg(n_connectors=("connection_id", "size"),
                           max_power_kw=("power_kw", "max")))
    stations = stations.merge(conn_agg, on="station_id", how="left")

    # operator name join (reference data)
    ops = pd.DataFrame(ref["Operators"])[["ID", "Title"]]
    stations = stations.merge(ops, left_on="operator_id", right_on="ID", how="left")
    stations = stations.rename(columns={"Title": "operator_name"}).drop(columns=["ID"])

    stations["operator_id"] = stations["operator_id"].astype("Int64")
    stations["n_connectors"] = stations["n_connectors"].astype("Int64")


    # provenance
    now = datetime.now(timezone.utc)
    stations["source_file"] = snap.name
    stations["loaded_at_utc"] = now
    conns["source_file"] = snap.name
    conns["loaded_at_utc"] = now

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    stations.to_parquet(SILVER_DIR / "stations.parquet", index=False)
    conns.to_parquet(SILVER_DIR / "connections.parquet", index=False)
    print(f"stations: {len(stations)} rows, connections: {len(conns)} rows -> {SILVER_DIR}")

if __name__ == "__main__":
    main()


