# src/database/db_manager.py
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/mlb_betting.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_connection():
    return duckdb.connect(str(DB_PATH))

def append_to_table(df: pd.DataFrame, table_name: str):
    if df is None or df.empty:
        return
    df["fetch_timestamp"] = datetime.now(timezone.utc)  # ensure we always have it
    con = get_connection()
    # Create table if not exists + append (DuckDB handles time-series perfectly)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0;
        INSERT INTO {table_name} SELECT * FROM df;
    """)
    print(f"✅ Appended {len(df)} rows to {table_name} in central DuckDB")
    con.close()