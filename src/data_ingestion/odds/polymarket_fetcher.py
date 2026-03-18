"""
Polymarket odds fetcher — uses Gamma Events API with tag_slug=mlb.
Appends to data/bronze/odds/polymarket_YYYY.parquet (one file per year).
Each row = one market with a snapshot_timestamp.

Market types captured:
  game_winner  — moneyline, spread, or O/U from "Team A vs. Team B" events
  win_total    — "Will X win more than N.5 games" from the season win totals event
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import json
import os
import re
import tempfile
import requests
import duckdb
from datetime import datetime, timezone

from config.settings import BASE_DIR

EVENTS_URL    = "https://gamma-api.polymarket.com/events"
BRONZE_ODDS_DIR = BASE_DIR / "data" / "bronze" / "odds"


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def _fetch_mlb_events() -> list[dict]:
    """Fetch all active MLB events via tag_slug=mlb. Paginates if needed."""
    all_events: list[dict] = []
    offset = 0
    limit  = 100

    while True:
        resp = requests.get(
            EVENTS_URL,
            params={
                "tag_slug": "mlb",
                "active":   "true",
                "closed":   "false",
                "limit":    limit,
                "offset":   offset,
            },
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_events.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return all_events


# ---------------------------------------------------------------------------
# Filtering & parsing
# ---------------------------------------------------------------------------

def _is_game_event(event: dict) -> bool:
    return " vs. " in event.get("title", "")


def _is_win_total_event(event: dict) -> bool:
    title = event.get("title", "")
    return "Pro Baseball" in title and "Regular Season" in title


def _parse_prices(market: dict) -> tuple[float | None, float | None]:
    raw = market.get("outcomePrices", [])
    try:
        prices = json.loads(raw) if isinstance(raw, str) else raw
        yes_p = float(prices[0]) if len(prices) > 0 else None
        no_p  = float(prices[1]) if len(prices) > 1 else None
    except (ValueError, TypeError, IndexError):
        yes_p = no_p = None
    return yes_p, no_p


def _parse_clob_token_ids(market: dict) -> tuple[str | None, str | None]:
    raw = market.get("clobTokenIds")
    if not raw:
        return None, None
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return (ids[0] if len(ids) > 0 else None,
                ids[1] if len(ids) > 1 else None)
    except (json.JSONDecodeError, IndexError):
        return None, None


_WIN_TOTAL_RE = re.compile(
    r"Will the (.+?) win more than ([\d.]+) games",
    re.IGNORECASE,
)


def _build_game_rows(event: dict, snapshot_ts: str) -> list[dict]:
    title = event.get("title", "")
    parts = title.split(" vs. ", 1)
    away_team = parts[0].strip() if len(parts) == 2 else ""
    home_team = parts[1].strip() if len(parts) == 2 else ""
    event_date = str(event.get("eventDate") or event.get("endDate", ""))[:10]

    rows = []
    for m in event.get("markets", []):
        yes_price, no_price = _parse_prices(m)
        yes_token, no_token = _parse_clob_token_ids(m)
        group = m.get("groupItemTitle") or "moneyline"
        raw_outcomes = m.get("outcomes", [])
        try:
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        except (json.JSONDecodeError, TypeError):
            outcomes = []

        rows.append({
            "market_type":       "game_winner",
            "event_id":          event.get("id"),
            "event_slug":        event.get("slug"),
            "event_title":       title,
            "away_team":         away_team,
            "home_team":         home_team,
            "event_date":        event_date,
            "question":          m.get("question"),
            "condition_id":      m.get("conditionId"),
            "market_slug":       m.get("slug"),
            "group_item_title":  group,
            "outcomes":          json.dumps(outcomes),
            "yes_price":         yes_price,
            "no_price":          no_price,
            "yes_clob_token_id": yes_token,
            "no_clob_token_id":  no_token,
            "volume":            m.get("liquidityNum") or m.get("liquidity"),
            "end_date":          str(m.get("endDateIso") or m.get("endDate") or "")[:10],
            "accepting_orders":  m.get("acceptingOrders"),
            "snapshot_timestamp": snapshot_ts,
            "snapshot_date":     snapshot_ts[:10],
            "source":            "polymarket",
        })
    return rows


def _build_win_total_rows(event: dict, snapshot_ts: str) -> list[dict]:
    rows = []
    for m in event.get("markets", []):
        question = m.get("question", "")
        match = _WIN_TOTAL_RE.search(question)
        team_name     = match.group(1) if match else m.get("groupItemTitle", "")
        win_threshold = float(match.group(2)) if match else None

        yes_price, no_price = _parse_prices(m)
        yes_token, no_token = _parse_clob_token_ids(m)

        rows.append({
            "market_type":       "win_total",
            "event_id":          event.get("id"),
            "event_slug":        event.get("slug"),
            "event_title":       event.get("title"),
            "team_name":         team_name,
            "win_threshold":     win_threshold,
            "question":          question,
            "condition_id":      m.get("conditionId"),
            "market_slug":       m.get("slug"),
            "yes_price":         yes_price,
            "no_price":          no_price,
            "yes_clob_token_id": yes_token,
            "no_clob_token_id":  no_token,
            "volume":            m.get("liquidityNum") or m.get("liquidity"),
            "end_date":          str(m.get("endDateIso") or m.get("endDate") or "")[:10],
            "accepting_orders":  m.get("acceptingOrders"),
            "snapshot_timestamp": snapshot_ts,
            "snapshot_date":     snapshot_ts[:10],
            "source":            "polymarket",
        })
    return rows


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _append_to_parquet(rows: list[dict], year: int) -> None:
    """Append rows to the per-year bronze parquet file via DuckDB."""
    if not rows:
        return

    BRONZE_ODDS_DIR.mkdir(parents=True, exist_ok=True)
    target = BRONZE_ODDS_DIR / f"polymarket_{year}.parquet"

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(rows, f)
        con = duckdb.connect()
        new_rows_sql = f"""
            SELECT * REPLACE (
                TRY_CAST(snapshot_timestamp AS TIMESTAMPTZ) AS snapshot_timestamp,
                TRY_CAST(snapshot_date AS DATE) AS snapshot_date,
                TRY_CAST(volume AS DOUBLE) AS volume
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fetch_polymarket_mlb() -> list[dict] | None:
    """Fetch active Polymarket MLB markets and append to bronze parquet."""
    try:
        events = _fetch_mlb_events()
        snapshot_ts = datetime.now(timezone.utc).isoformat()
        year = datetime.now(timezone.utc).year

        rows: list[dict] = []
        for event in events:
            if _is_game_event(event):
                rows.extend(_build_game_rows(event, snapshot_ts))
            elif _is_win_total_event(event):
                rows.extend(_build_win_total_rows(event, snapshot_ts))

        if not rows:
            print("⚠️  Polymarket: no markets after filtering")
            return None

        game_rows = [r for r in rows if r["market_type"] == "game_winner"]
        wt_rows   = [r for r in rows if r["market_type"] == "win_total"]
        print(f"✅ Polymarket: {len(game_rows)} game markets, {len(wt_rows)} win total markets")

        _append_to_parquet(rows, year)
        return rows
    except Exception as e:
        print(f"⚠️  Polymarket skipped: {e}")
        return None


if __name__ == "__main__":
    fetch_polymarket_mlb()
