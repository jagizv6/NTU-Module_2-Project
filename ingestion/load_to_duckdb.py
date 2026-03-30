"""
Step 1: Ingest CSV files into DuckDB (local data source).

Loads two CSVs into DuckDB:
  - oil_geopolitics_dataset_2010_2026.csv  → table: raw_oil_prices
  - geopolitical_events_timeline.csv       → table: raw_geo_events
"""

import os
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/oil_pipeline.duckdb")
CSV_OIL_PRICES = os.getenv(
    "CSV_OIL_PRICES",
    r"C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Oil Price CSV/oil_geopolitics_dataset_2010_2026.csv",
)
CSV_GEO_EVENTS = os.getenv(
    "CSV_GEO_EVENTS",
    r"C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Oil Price CSV/geopolitical_events_timeline.csv",
)


def load_csv_to_duckdb(conn: duckdb.DuckDBPyConnection, csv_path: str, table_name: str) -> None:
    print(f"  Loading {csv_path} → {table_name} ...")
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=true)
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  ✓ {table_name}: {count:,} rows loaded")


def main():
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

    print(f"\nConnecting to DuckDB at: {DUCKDB_PATH}")
    conn = duckdb.connect(DUCKDB_PATH)

    load_csv_to_duckdb(conn, CSV_OIL_PRICES, "raw_oil_prices")
    load_csv_to_duckdb(conn, CSV_GEO_EVENTS, "raw_geo_events")

    print("\nSchema preview — raw_oil_prices:")
    print(conn.execute("DESCRIBE raw_oil_prices").df().to_string(index=False))

    print("\nSchema preview — raw_geo_events:")
    print(conn.execute("DESCRIBE raw_geo_events").df().to_string(index=False))

    conn.close()
    print("\nDuckDB ingestion complete.")


if __name__ == "__main__":
    main()
