"""
Kalshi odds fetcher — direct REST API, no SDK dependency.

Fetches two market types:
  - KXMLBSTGAME: single game head-to-head winner markets
  - KXMLBWINS:   season win total markets

Appends to data/bronze/odds/kalshi_YYYY.parquet (one file per year).
Each row = one market with a snapshot_timestamp and market_type column.

No auth required for public market data reads.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import json
import os
import tempfile
import time
import duckdb
import requests
from datetime import datetime, timezone

from config.settings import BASE_DIR

PRODUCTION_HOST = "https://api.elections.kalshi.com/trade-api/v2"
BRONZE_ODDS_DIR = BASE_DIR / "data" / "bronze" / "odds"

_MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
}


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


def _parse_game_ticker(ticker: str) -> dict:
    """
    Parse KXMLBSTGAME-26MAR171305ATLBOS-BOS into components.
    event_code format: {YY:2}{MON:3}{DD:2}{HHMM:4}{AWAY:3}{HOME:3+}
    """
    rest = ticker.replace("KXMLBSTGAME-", "", 1)
    parts = rest.rsplit("-", 1)
    if len(parts) != 2:
        return {}
    event_code, winner_side = parts
    if len(event_code) < 14:
        return {"winner_side": winner_side}
    yy       = event_code[0:2]
    mon      = event_code[2:5]
    dd       = event_code[5:7]
    time_str = event_code[7:11]
    teams    = event_code[11:]
    # Use winner_side to find the correct split — teams use 2 or 3 char codes
    if teams.endswith(winner_side):
        home = winner_side
        away = teams[: len(teams) - len(winner_side)]
    elif teams.startswith(winner_side):
        away = winner_side
        home = teams[len(winner_side):]
    else:
        away, home = teams[:3], teams[3:]  # fallback
    month    = _MONTH_MAP.get(mon, 0)
    game_date = f"{2000 + int(yy)}-{month:02d}-{int(dd):02d}" if month else None
    game_time = f"{time_str[:2]}:{time_str[2:]}"
    return {
        "away_team":   away,
        "home_team":   home,
        "winner_side": winner_side,
        "game_date":   game_date,
        "game_time":   game_time,
    }


def _parse_win_total_ticker(ticker: str) -> dict:
    """
    Parse KXMLBWINS-WSH-26-T60 into components.
    Format: KXMLBWINS-{TEAM}-{YY}-T{THRESHOLD}
    """
    parts = ticker.split("-")
    if len(parts) != 4:
        return {}
    _, team, _, threshold_str = parts
    win_threshold = int(threshold_str[1:]) if threshold_str.startswith("T") else None
    return {"team": team, "win_threshold": win_threshold}


def _market_to_row(market: dict, market_type: str, snapshot_ts: str, extra: dict) -> dict:
    """Flatten a REST API market dict to a plain dict for parquet storage."""
    def _dollars(val) -> float:
        """Kalshi returns prices as cents (0–100) or fractions (0.0–1.0). Normalize to 0–1."""
        if val is None:
            return 0.0
        f = float(val)
        return f / 100.0 if f > 1.0 else f

    close_time = market.get("close_time")
    return {
        "ticker":             market.get("ticker"),
        "event_ticker":       market.get("event_ticker"),
        "title":              market.get("title"),
        "subtitle":           market.get("subtitle"),
        "status":             market.get("status"),
        "yes_bid":            _dollars(market.get("yes_bid")),
        "yes_ask":            _dollars(market.get("yes_ask")),
        "no_bid":             _dollars(market.get("no_bid")),
        "no_ask":             _dollars(market.get("no_ask")),
        "volume":             float(market.get("volume", 0) or 0),
        "volume_24h":         float(market.get("volume_24h", 0) or 0),
        "open_interest":      float(market.get("open_interest", 0) or 0),
        "close_time":         close_time,
        "market_type":        market_type,
        "snapshot_timestamp": snapshot_ts,
        "snapshot_date":      snapshot_ts[:10],
        "source":             "kalshi",
        **extra,
    }


def _fetch_series(session: requests.Session, series_ticker: str,
                  snapshot_ts: str) -> list[dict]:
    """Paginate through all markets for a series and return flat row dicts."""
    market_type = "game_winner" if series_ticker == "KXMLBSTGAME" else "win_total"
    parser = _parse_game_ticker if series_ticker == "KXMLBSTGAME" else _parse_win_total_ticker
    # Game markets are status=open; win totals are unopened pre-season so fetch without filter
    status = "open" if series_ticker == "KXMLBSTGAME" else None

    rows = []
    cursor = None
    while True:
        params: dict = {
            "limit": 1000,
            "series_ticker": series_ticker,
        }
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor

        resp = session.get(f"{PRODUCTION_HOST}/markets", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for market in data.get("markets", []):
            rows.append(_market_to_row(market, market_type, snapshot_ts,
                                       parser(market.get("ticker", ""))))

        cursor = data.get("cursor")
        if not cursor:
            break
        time.sleep(0.25)

    return rows


def _append_to_parquet(rows: list[dict], year: int) -> None:
    """Append rows to the per-year bronze parquet file via DuckDB."""
    if not rows:
        return
    BRONZE_ODDS_DIR.mkdir(parents=True, exist_ok=True)
    target = BRONZE_ODDS_DIR / f"kalshi_{year}.parquet"

    # Write JSON to a temp file — avoids SQL quoting issues with inline JSON strings
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(rows, f, default=str)
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


def fetch_kalshi_mlb() -> list[dict] | None:
    """Fetch KXMLBSTGAME + KXMLBWINS markets and append to bronze parquet."""
    try:
        session = _make_session()
        snapshot_ts = datetime.now(timezone.utc).isoformat()
        year = datetime.now(timezone.utc).year

        game_rows = _fetch_series(session, "KXMLBSTGAME", snapshot_ts)
        print(f"✅ Kalshi KXMLBSTGAME: {len(game_rows)} game winner markets")

        time.sleep(0.5)

        win_rows = _fetch_series(session, "KXMLBWINS", snapshot_ts)
        print(f"✅ Kalshi KXMLBWINS: {len(win_rows)} win total markets")

        all_rows = game_rows + win_rows
        if not all_rows:
            print("⚠️  Kalshi: no markets returned")
            return None

        _append_to_parquet(all_rows, year)
        print(f"✅ Kalshi: {len(all_rows)} total rows appended")
        return all_rows
    except Exception as e:
        print(f"⚠️  Kalshi skipped: {e}")
        return None


if __name__ == "__main__":
    fetch_kalshi_mlb()
