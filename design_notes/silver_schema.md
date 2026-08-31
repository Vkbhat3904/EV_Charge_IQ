Two tables, one-to-many, mirroring reality:

silver.stations
  station_id      (int, from OCM ID — primary key)
  title           (text)
  latitude, longitude  (float, bbox-verified)
  town            (text, nullable — kept for display only)
  operator_id     (int, nullable)
  operator_name   (text, nullable — from reference data join)
  status_type_id  (int, nullable)
  n_connectors    (int, derived: len(Connections))
  max_power_kw    (float, nullable — derived: max of connection PowerKW, null if none)
  date_last_status_update  (timestamp, nullable)

silver.connections
  connection_id   (int, from OCM Connection ID — primary key)
  station_id      (int, foreign key → stations)
  connection_type_id (int, nullable)
  power_kw        (float, nullable)
  quantity        (int, nullable)
  status_type_id  (int, nullable)

Why this shape:

Two tables, not one: a station has 1–11 connectors. Flattening into one row per station loses connectors; one row per connector loses the station. The one-to-many split is the honest model.
max_power_kw derived on stations: the single most useful analytics field ("is this a fast charger?") — derived once, used everywhere downstream.
Nullables marked explicitly: missing PowerKW stays null. Filling with 0 would corrupt every average; filling with the mean would be fabrication.
operator_name via join: reference data gives ID→name. We fetch it once, cache it in Bronze too (it's small), and join in Silver.

Can you answer "how many DC fast chargers (≥50 kW) are in Bangalore?" from these two tables alone? (Yes: filter connections or stations by power)
Does any station appear twice? (We'll dedupe on station_id — OCM IDs are stable)
What happens to the zero-connector station? (It survives in stations with n_connectors=0, max_power_kw=null — present but honest)

1. Why max_power_kw on stations AND power_kw on connections — isn't that redundant?
Yes, it's deliberately redundant — that's the point. It's called denormalization for a known access pattern.

power_kw on connections is the fact: each connector's true rating (3.3, 22, 60, 120 kW...). This is the source of truth.
max_power_kw on stations is a derived convenience: the fastest experience a driver can have at that station.
Why both exist: the most common downstream question is per-station, not per-connector — "show me fast-charging stations near Indiranagar", "what % of Bangalore stations offer ≥50 kW?". If we only stored per-connector power, every one of those queries needs a join + GROUP BY + MAX() — repeated forever, in the DS dashboard, the ML features, the AI tool. Computing max_power_kw once, at Silver build time, and storing it, trades a few bytes of storage for simplicity in every consumer downstream.

The rule that makes it safe: derived fields are fine if documented as derived. Anyone must be able to recompute it from the raw facts (MAX(power_kw) GROUP BY station_id). The moment the derivation is ambiguous, redundancy becomes corruption.

2. Why is town "display only" and not used for geography?
Because we have direct evidence it's unreliable, and direct evidence lat/lon is reliable.

From our own survey of your Bronze data:

Town takes 7+ different values for the same city: Bengaluru (140), Bangalore (6), missing (2), Chikkanayakanahalli (with a leading space), Bandapura, Arasinakunte — and one station literally says Bowie (a US city — a data provider's template that was never updated)
Meanwhile the coordinates of that same "Bowie" station are 12.968, 77.649 — correctly in central Bangalore, and every record passed our bbox filter
The principle: geography comes from coordinates (numeric, filterable, verifiable against the bbox) — text fields like Town are display decoration. When we later do ward-level analysis (joining BBMP ward boundaries), the join key will be a spatial join on lat/lon (point-in-polygon), never a string match on town names. String-matching geography is how you get "Charging deserts in Bowie, Maryland" in a Bangalore report.

3. Why Parquet over CSV, in one sentence?
Parquet stores a real schema — typed, nullable columns — while CSV stores only text, so every null becomes an empty string and every int/float becomes a string the moment you save, silently destroying the typing discipline the whole Silver layer exists to enforce.

## Provenance
Both tables carry:
  source_file     (text) — bronze filename the row was built from
  loaded_at_utc   (timestamp) — when the Silver build ran

## Output format
Parquet files in data/silver/:
  data/silver/stations.parquet
  data/silver/connections.parquet
Rationale: preserves typed, nullable columns; readable by DuckDB,
pandas, dbt, Spark — the lakehouse standard.

## Dedup rule
If the same station_id appears more than once in an input snapshot,
keep the row with the latest DateLastStatusUpdate.

