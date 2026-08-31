from pathlib import Path

import duckdb

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

def main():
    con = duckdb.connect()          # in-memory database — nothing persisted unless we write it
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Gold table 1: fast-charger coverage by operator
    fast = con.sql(f"""
        SELECT
            operator_name,
            COUNT(*)                        AS n_stations,
            COUNT(max_power_kw)             AS n_with_power_data,
            SUM(CASE WHEN max_power_kw >= 50 THEN 1 ELSE 0 END) AS n_fast_stations
        FROM '{SILVER_DIR / "stations.parquet"}'
        GROUP BY operator_name
        ORDER BY n_fast_stations DESC
    """)
    fast.write_parquet(str(GOLD_DIR / "fast_charger_coverage.parquet"))

    fast.show()

    # Gold table 2: power tier distribution (stations)
    tiers = con.sql("""
        SELECT
            CASE
                WHEN max_power_kw IS NULL      THEN 'unknown'
                WHEN max_power_kw < 11         THEN 'slow (<11 kW)'
                WHEN max_power_kw < 50         THEN 'standard (11-49 kW)'
                ELSE 'fast (>=50 kW)'
            END AS power_tier,
            COUNT(*) AS n_stations,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_network
        FROM 'data/silver/stations.parquet'
        GROUP BY power_tier
        ORDER BY n_stations DESC
    """)
    tiers.write_parquet(str(GOLD_DIR / "power_tier_distribution.parquet"))
    tiers.show()

    # Gold table 3: connector mix (connectors with readable names)
    mix = con.sql("""
        WITH conn_types AS (
            SELECT unnest(ConnectionTypes) AS ct
            FROM read_json_auto('data/bronze/ocm_reference_data.json')
        )
        SELECT
            ct.Title AS connector_type,
            COUNT(*) AS n_connectors
        FROM 'data/silver/connections.parquet' c
        LEFT JOIN conn_types ON c.connection_type_id = ct.ID
        GROUP BY ct.Title
        ORDER BY n_connectors DESC
    """)
    mix.write_parquet(str(GOLD_DIR / "connector_mix.parquet"))
    mix.show()

        # Gold table 4: daily weather aggregates (IST days — see design note §8)
    con.execute("SET TimeZone = 'Asia/Kolkata'")   # explicit, machine-independent
    weather_daily = con.sql("""
        SELECT
            CAST(time_utc AS DATE) AS date_ist,
            COUNT(*) AS n_hours,
            ROUND(AVG(temperature_c), 1) AS mean_temp_c,
            ROUND(MAX(temperature_c), 1) AS max_temp_c,
            ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
            ROUND(AVG(humidity_pct), 1) AS mean_humidity_pct
        FROM 'data/silver/weather.parquet'
        GROUP BY date_ist
        ORDER BY date_ist
    """)

    weather_daily.write_parquet(str(GOLD_DIR / "weather_daily.parquet"))
    weather_daily.show()

if __name__ == "__main__":
    main()
