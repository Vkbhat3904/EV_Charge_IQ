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

---

## 7. Weather Table (added with Open-Meteo source)

> Source: Open-Meteo `/v1/forecast` — hourly Bangalore weather
> (lat 12.9716, lon 77.5946), no API key required.
> Verified by probe call: `hourly` dict of parallel arrays
> (`time`, `temperature_2m` °C, `precipitation` mm, `relative_humidity_2m` %),
> 72 hourly steps per fetch (`past_days=2, forecast_days=1`).

### 7.1 Schema

| Column | Type | Notes |
|---|---|---|
| `time_utc` | timestamp | **Converted from IST to UTC** — see §7.3 |
| `temperature_c` | float, nullable | |
| `precipitation_mm` | float, nullable | |
| `humidity_pct` | float, nullable | |
| `source_file` | text | Provenance — bronze weather filename |
| `loaded_at_utc` | timestamp | Provenance — Silver build time |

### 7.2 Primary key and dedup rule

Weather is a **time series**, not a snapshot — the primary key is
`time_utc` (one row per hour). New fetches overlap old ones (today's
fetch re-includes yesterday's hours), so dedup is required.

**Rule: when two Bronze snapshots contain the same hour, the LATEST
snapshot wins.**

Justification: Open-Meteo can revise past observations (quality
corrections, late-arriving station data). The most recent fetch carries
the API's most corrected view of any given hour. The alternative —
earliest-wins, "closest to the observation" — would freeze any
initial errors into Silver forever. We accept that a revision can
change a historical row; provenance (`source_file`) records which
snapshot each row came from, so any change is traceable.

### 7.3 Timezone decision

The API is queried with `timezone=Asia/Kolkata`, so Bronze timestamps
are IST. Silver stores `time_utc` — converted to UTC — because:

- Every other timestamp in the platform is UTC (`loaded_at_utc`,
  OCM `DateLastStatusUpdate`)
- Mixing timezones in joins silently corrupts results (an IST hour
  joined to a UTC hour is 5.5 hours wrong — enough to turn a
  charging peak into a trough)
- UTC is the storage standard; IST is a *presentation* concern,
  handled at the dashboard/API layer later

Conversion: subtract 5 hours 30 minutes (IST = UTC+5:30, no DST).

### 7.4 Implementation notes

- Bronze stores *parallel arrays* (`data["hourly"]`), not rows.
  `pd.DataFrame(data["hourly"])` converts them to one row per hour.
- Merge across snapshots: concatenate all Bronze weather files, then
  apply the dedup rule (sort by `loaded_at_utc`, keep last per
  `time_utc`).
- Row count check: each snapshot contributes 72 hours; after dedup,
  total rows = distinct hours covered across all snapshots.

---

## 8. Weather Daily Grain (Gold layer)

> Gold table `weather_daily` — daily aggregates of the Silver weather
> series, built in `src/build_gold.py`.

### 8.1 Grain

One row per **IST calendar day**. Hourly data serves forecasting
regressors; daily aggregates serve dashboards and any future
daily-grain demand analysis. Grains are never mixed silently.

### 8.2 Day-definition decision: IST days, not UTC days

`time_utc` is stored in UTC, but a "day" in this analysis means a
Bangalore day. Grouping by UTC date would split Bangalore evenings
(IST 18:00–23:59 = UTC 12:30–18:29) across two meaningless UTC
"days". Grouping by IST date makes each row a real Bangalore day.

The conversion happens inside the grouping expression:
`CAST(time_utc + INTERVAL 5 HOUR + INTERVAL 30 MINUTE AS DATE)`.
Storage stays UTC (§7.3); analysis converts at the boundary — the
standard store-UTC / analyze-local pattern.

### 8.3 Aggregation choices

- `SUM(precipitation_mm)` — precipitation adds up (total mm fallen)
- `AVG(temperature_c)`, `AVG(humidity_pct)` — these average
- `MAX(temperature_c)` — the day's peak, useful for heat-stress signals
- `COUNT(*) AS n_hours` — the honesty column: a complete day has 24;
  partial days (window edges) show fewer, so consumers can see which
  daily averages are trustworthy

### 8.4 Verification

- First/last days of the window show `n_hours < 24`; middle days 24
- One day's `total_precipitation_mm` hand-checked against the sum of
  its hourly Silver rows — must match exactly
