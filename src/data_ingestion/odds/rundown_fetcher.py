"""
TheRundown odds fetcher (dormant — no active subscription).
Appends to data/bronze/odds/rundown_YYYY.parquet (one file per year).
Each row = one event with a snapshot_timestamp.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import json
import requests
import duckdb
from datetime import datetime, timezone

from config.settings import THE_RUNDOWN_API_KEY, BASE_DIR

BASE_URL = "https://api.therundown.io/api/v2"
BRONZE_ODDS_DIR = BASE_DIR / "data" / "bronze" / "odds"


def _append_to_parquet(rows: list[dict], year: int) -> None:
    """Append rows to the per-year bronze parquet file via DuckDB."""
    if not rows:
        return

    BRONZE_ODDS_DIR.mkdir(parents=True, exist_ok=True)
    target = BRONZE_ODDS_DIR / f"rundown_{year}.parquet"

    con = duckdb.connect()
    json_str = json.dumps(rows)
    new_rows_sql = f"SELECT * FROM read_json_auto($${json_str}$$)"

    if target.exists():
        final_sql = f"""
            SELECT * FROM read_parquet('{target}', union_by_name=true)
            UNION ALL BY NAME
            ({new_rows_sql})
        """
    else:
        final_sql = new_rows_sql

    con.execute(f"COPY ({final_sql}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0]
    con.close()
    print(f"   ✅ {target.name}: {n:,} total rows")


def fetch_rundown_mlb() -> list[dict] | None:
    """Fetch MLB events from TheRundown and append to bronze parquet."""
    if not THE_RUNDOWN_API_KEY or not THE_RUNDOWN_API_KEY.strip():
        print("⚠️  TheRundown skipped (no key)")
        return None

    url = f"{BASE_URL}/sports/3/events"  # 3 = MLB
    headers = {"X-TheRundown-Key": THE_RUNDOWN_API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        events = resp.json().get("events", [])

        if not events:
            print("⚠️  TheRundown: no events returned")
            return None

        snapshot_ts = datetime.now(timezone.utc).isoformat()
        year = datetime.now(timezone.utc).year

        rows = [{
            **event,
            "snapshot_timestamp": snapshot_ts,
            "snapshot_date": snapshot_ts[:10],
            "source": "rundown",
        } for event in events]

        _append_to_parquet(rows, year)
        print(f"✅ TheRundown: {len(rows)} events appended")
        return rows
    except Exception as e:
        print(f"⚠️  TheRundown skipped: {e}")
        return None


if __name__ == "__main__":
    fetch_rundown_mlb()
