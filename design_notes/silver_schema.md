# Silver Layer — Schema Design

> Design note for the Silver layer of the Bangalore EV Charging Data Platform.
> Written before implementation; every rule below is encoded in `src/build_silver.py`.

---

## 1. Schema

Two tables, one-to-many, mirroring reality: a station has 1–11 connectors.

### `silver.stations`

| Column | Type | Notes |
|---|---|---|
| `station_id` | int | From OCM `ID` — primary key |
| `title` | text | |
| `latitude` | float | bbox-verified |
| `longitude` | float | bbox-verified |
| `town` | text, nullable | Display only — never used for geography (see §3.2) |
| `operator_id` | int, nullable | Nullable `Int64` |
| `operator_name` | text, nullable | From reference-data join |
| `status_type_id` | int, nullable | |
| `n_connectors` | int, derived | `len(Connections)`; nullable `Int64` |
| `max_power_kw` | float, nullable, derived | Max of connection `PowerKW`; null if none |
| `date_last_status_update` | timestamp, nullable | UTC |
| `source_file` | text | Provenance — bronze filename (see §4) |
| `loaded_at_utc` | timestamp | Provenance — Silver build time (see §4) |

### `silver.connections`

| Column | Type | Notes |
|---|---|---|
| `connection_id` | int | From OCM Connection `ID` — primary key |
| `station_id` | int | Foreign key → `stations` |
| `connection_type_id` | int, nullable | |
| `power_kw` | float, nullable | The fact — each connector's true rating |
| `quantity` | int, nullable | |
| `status_type_id` | int, nullable | |
| `source_file` | text | Provenance |
| `loaded_at_utc` | timestamp | Provenance |

---

## 2. Design Rationale

### 2.1 Why two tables, not one

A station has 1–11 connectors. Flattening into one row per station loses
connectors; one row per connector loses the station. The one-to-many split is
the honest model.

### 2.2 Why `max_power_kw` on stations *and* `power_kw` on connections

Deliberately redundant — **denormalization for a known access pattern**.

- `power_kw` on connections is the **fact**: each connector's true rating
  (3.3, 22, 60, 120 kW...). The source of truth.
- `max_power_kw` on stations is a **derived convenience**: the fastest
  experience a driver can have at that station.

The most common downstream question is per-station, not per-connector —
"show me fast-charging stations near Indiranagar", "what % of Bangalore
stations offer ≥50 kW?". Storing per-connector power only would force a
join + `GROUP BY` + `MAX()` into every consumer — the DS dashboard, the ML
features, the AI tool. Computing `max_power_kw` once, at Silver build time,
trades a few bytes for simplicity everywhere downstream.

**The rule that makes it safe**: derived fields are fine *if documented as
derived*. Anyone must be able to recompute it from the raw facts
(`MAX(power_kw) GROUP BY station_id`). The moment the derivation is
ambiguous, redundancy becomes corruption.

### 2.3 Why `town` is display-only

Direct evidence it's unreliable, direct evidence lat/lon is reliable.
From our survey of the actual Bronze data:

- `Town` takes 7+ values for the same city: `Bengaluru` (140), `Bangalore` (6),
  missing (2), ` Chikkanayakanahalli` (leading space), `Bandapura`,
  `Arasinakunte` — and one station literally says **`Bowie`** (a US city; a
  data provider's template never updated)
- The coordinates of that same "Bowie" station are 12.968, 77.649 — correctly
  in central Bangalore, and every record passed the bbox filter

**Principle**: geography comes from coordinates (numeric, filterable,
verifiable against the bbox); text fields are display decoration. Ward-level
analysis later will use a spatial join on lat/lon (point-in-polygon), never a
string match on town names. String-matching geography is how you get
"charging deserts in Bowie, Maryland" in a Bangalore report.

### 2.4 Why nulls are preserved, never faked

Missing `PowerKW` stays null. Filling with 0 would corrupt every average;
filling with the mean would be fabrication. Nullable `Int64` (pandas) is used
for integer columns that can hold missing values, avoiding silent
float-promotion (`3779.0`).

### 2.5 Why Parquet over CSV

Parquet stores a real schema — typed, nullable columns — while CSV stores
only text, so every null becomes an empty string and every int/float becomes
a string the moment you save, silently destroying the typing discipline the
whole Silver layer exists to enforce.

---

## 3. Provenance

Both tables carry:

| Column | Meaning |
|---|---|
| `source_file` | Bronze filename the row was built from |
| `loaded_at_utc` | When the Silver build ran |

When a number looks wrong in Gold weeks later, provenance answers
"which raw snapshot produced this?" — the audit trail that makes the
medallion architecture defensible.

---

## 4. Output Format

Parquet files in `data/silver/`:

```text
data/silver/stations.parquet
data/silver/connections.parquet
```

Rationale: preserves typed, nullable columns; readable by DuckDB, pandas,
dbt, Spark — the lakehouse standard.

---

## 5. Dedup Rule

If the same `station_id` appears more than once in an input snapshot,
keep the row with the latest `DateLastStatusUpdate`.

---

## 6. Verification Checklist

Questions the design must survive (all answered by the built tables):

1. **"How many DC fast chargers (≥50 kW) are in Bangalore?"** — answerable
   from these two tables alone: filter stations by `max_power_kw >= 50`.
2. **"Does any station appear twice?"** — no: dedup on `station_id`
   (OCM IDs are stable). Verified: 0 duplicates in output.
3. **"What happens to the zero-connector station?"** — it survives in
   `stations` with `n_connectors = NA`, `max_power_kw = NA`:
   present but honest. Verified: exactly 1 such row.
