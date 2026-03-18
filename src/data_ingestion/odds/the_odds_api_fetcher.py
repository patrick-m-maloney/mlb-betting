"""
The Odds API fetcher.
Appends to data/bronze/odds/the_odds_api_YYYY.parquet (one file per year).
Each row = one outcome/book/market combination with a snapshot_timestamp.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import json
import os
import tempfile
import requests
import duckdb
from datetime import datetime, timezone

from config.settings import THE_ODDS_API_KEY, BASE_DIR

BASE_URL = "https://api.the-odds-api.com/v4/sports"
BRONZE_ODDS_DIR = BASE_DIR / "data" / "bronze" / "odds"


def _normalize_response(data: list, snapshot_ts: str) -> list[dict]:
    """Flatten nested API response into one row per outcome/book/market."""
    rows = []
    for game in data:
        base = {
            "game_id":       game.get("id"),
            "sport_key":     game.get("sport_key"),
            "commence_time": game.get("commence_time"),
            "home_team":     game.get("home_team"),
            "away_team":     game.get("away_team"),
            "snapshot_timestamp": snapshot_ts,
            "snapshot_date": snapshot_ts[:10],
            "source":        "the_odds_api",
        }
        for book in game.get("bookmakers", []):
            for market in book.get("markets", []):
                for outcome in market.get("outcomes", []):
                    rows.append({
                        **base,
                        "bookmaker":    book["key"],
                        "last_update":  book.get("last_update"),
                        "market":       market["key"],
                        "outcome_name": outcome.get("name"),
                        "odds":         outcome.get("price"),
                        "point":        outcome.get("point"),
                    })
    return rows


def _append_to_parquet(rows: list[dict], year: int) -> None:
    """Append rows to the per-year bronze parquet file via DuckDB."""
    if not rows:
        return

    BRONZE_ODDS_DIR.mkdir(parents=True, exist_ok=True)
    target = BRONZE_ODDS_DIR / f"the_odds_api_{year}.parquet"

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(rows, f)
        con = duckdb.connect()
        new_rows_sql = f"""
            SELECT * REPLACE (
                TRY_CAST(snapshot_timestamp AS TIMESTAMPTZ) AS snapshot_timestamp,
                TRY_CAST(snapshot_date AS DATE) AS snapshot_date
            )
            FROM read_json_auto('{tmp_path}')
        """
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
    finally:
        os.unlink(tmp_path)


def fetch_odds(sport_key: str = "baseball_mlb", markets: str = "h2h,spreads,totals") -> list[dict] | None:
    """Fetch odds from The Odds API and append to bronze parquet."""
    url = f"{BASE_URL}/{sport_key}/odds"
    params = {
        "apiKey":      THE_ODDS_API_KEY,
        "regions":     "us",
        "markets":     markets,
        "oddsFormat":  "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        snapshot_ts = datetime.now(timezone.utc).isoformat()
        rows = _normalize_response(data, snapshot_ts)
        year = datetime.now(timezone.utc).year
        _append_to_parquet(rows, year)
        print(f"✅ The Odds API ({sport_key}): {len(rows)} rows appended")
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"   API credits remaining: {remaining}")
        return rows
    except Exception as e:
        print(f"⚠️  The Odds API skipped: {e}")
        return None


def fetch_futures() -> None:
    """Fetch World Series and other outright futures."""
    for sport in ["baseball_mlb_world_series_winner"]:
        rows = fetch_odds(sport, "outrights")
        if rows:
            print(f"✅ Futures fetched for {sport}")


if __name__ == "__main__":
    fetch_odds()
    fetch_futures()
