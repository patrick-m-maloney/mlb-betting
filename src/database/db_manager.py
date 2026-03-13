import duckdb
import pandas as pd
from datetime import datetime, timezone
from config.settings import DB_PATH   # ← matches your ducked_test.ipynb + data/db/mlb_betting.duckdb

def get_connection():
    return duckdb.connect(str(DB_PATH))

def append_to_table(df: pd.DataFrame, table_name: str):
    """Final version — try/except on every ALTER + type-safe fetch_timestamp."""
    if df is None or df.empty:
        print(f"⚠️  No data to append to {table_name}")
        return

    df = df.copy()
    con = get_connection()

    # Create table on first run
    table_exists = con.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    ).fetchone() is not None

    if not table_exists:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df LIMIT 0")

    # Get existing columns
    existing = [row[0] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]

    # Safely add fetch_timestamp (TIMESTAMP for time-series queries)
    if "fetch_timestamp" not in existing:
        try:
            con.execute(f'ALTER TABLE {table_name} ADD COLUMN fetch_timestamp TIMESTAMP')
            print(f"   → Added fetch_timestamp column to {table_name}")
        except Exception as e:
            print(f"   ⚠️  fetch_timestamp already exists (skipped): {e}")

    # Add any other new columns as VARCHAR
    for col in df.columns:
        if col not in existing and col != "fetch_timestamp":
            try:
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" VARCHAR')
                print(f"   → Added new column '{col}' to {table_name}")
            except:
                pass

    # Add timestamp + insert (BY NAME = safe even if columns differ)
    df["fetch_timestamp"] = datetime.now(timezone.utc)
    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM temp_df")

    print(f"✅ Appended {len(df)} rows to table '{table_name}' in {DB_PATH.name}")
    con.close()