"""
Polymarket odds fetcher.
Appends to data/bronze/odds/polymarket_YYYY.parquet (one file per year).
Each row = one market with a snapshot_timestamp.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import json
import requests
import duckdb
from datetime import datetime, timezone

from config.settings import BASE_DIR

BRONZE_ODDS_DIR = BASE_DIR / "data" / "bronze" / "odds"
_MLB_KEYWORDS = ["mlb", "baseball"]


def _is_mlb(market: dict) -> bool:
    question = str(market.get("question", "")).lower()
    if any(kw in question for kw in _MLB_KEYWORDS):
        return True
    tags = market.get("tags", [])
    if isinstance(tags, list):
        tags_str = " ".join(str(t) for t in tags).lower()
    else:
        tags_str = str(tags).lower()
    return any(kw in tags_str for kw in _MLB_KEYWORDS)


def _append_to_parquet(rows: list[dict], year: int) -> None:
    """Append rows to the per-year bronze parquet file via DuckDB."""
    if not rows:
        return

    BRONZE_ODDS_DIR.mkdir(parents=True, exist_ok=True)
    target = BRONZE_ODDS_DIR / f"polymarket_{year}.parquet"

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


def fetch_polymarket_mlb() -> list[dict] | None:
    """Fetch active Polymarket MLB markets and append to bronze parquet."""
    url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1000"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        snapshot_ts = datetime.now(timezone.utc).isoformat()
        year = datetime.now(timezone.utc).year

        rows = []
        for market in data:
            if _is_mlb(market):
                rows.append({
                    **market,
                    "snapshot_timestamp": snapshot_ts,
                    "snapshot_date": snapshot_ts[:10],
                    "source": "polymarket",
                })

        if not rows:
            print("⚠️  Polymarket: no MLB markets")
            return None

        _append_to_parquet(rows, year)
        print(f"✅ Polymarket: {len(rows)} MLB markets appended")
        return rows
    except Exception as e:
        print(f"⚠️  Polymarket skipped: {e}")
        return None


if __name__ == "__main__":
    fetch_polymarket_mlb()
