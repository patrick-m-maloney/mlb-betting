"""
Player game logs fetcher — daily batting + pitching stats from Baseball Reference.

Two modes:
  - Legacy (pre-2026): pybaseball batting_stats_range / pitching_stats_range per date
  - Modern (2026+):    Scrape BR standard batting/pitching page (cumulative),
                        store daily snapshots, diff consecutive days to get per-day stats.

The modern approach is needed because BR restructured their daily stats page in 2026,
breaking pybaseball's HTML parser.

Output paths:
  data/player_logs/game_by_game/batting_game_logs_{year}.parquet
  data/player_logs/game_by_game/pitching_game_logs_{year}.parquet
  data/bronze/player_logs/cumulative/batting_cumulative_{year}.parquet   (snapshots)
  data/bronze/player_logs/cumulative/pitching_cumulative_{year}.parquet  (snapshots)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import os
import re
import tempfile
import time
import random
import duckdb
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, date, timedelta

from config.settings import BASE_DIR, PLAYER_LOGS_DIR, GAME_BY_GAME_DIR

# ========================= PATHS =========================

CUMULATIVE_DIR = BASE_DIR / "data" / "bronze" / "player_logs" / "cumulative"
CUMULATIVE_DIR.mkdir(parents=True, exist_ok=True)
GAME_BY_GAME_DIR.mkdir(parents=True, exist_ok=True)

BR_BASE_URL = "https://www.baseball-reference.com/leagues/majors"
BR_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Counting stats that can be diffed between cumulative snapshots
_BATTING_COUNT_COLS = [
    "G", "PA", "AB", "R", "H", "2B", "3B", "HR", "RBI",
    "SB", "CS", "BB", "SO", "TB", "GIDP", "HBP", "SH", "SF", "IBB",
]
_PITCHING_COUNT_COLS = [
    "W", "L", "G", "GS", "GF", "CG", "SHO", "SV",
    "H", "R", "ER", "HR", "BB", "IBB", "SO", "HBP", "BK", "WP", "BF",
]

# ========================= SCRAPING =========================


def _scrape_standard_page(year: int, stat_type: str = "batting") -> list[dict] | None:
    """Scrape BR standard batting or pitching page. Returns list of player dicts.

    stat_type: 'batting' or 'pitching'
    """
    url = f"{BR_BASE_URL}/{year}-standard-{stat_type}.shtml"
    print(f"   🌐 Fetching {url}")

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=BR_HEADERS, timeout=30)
            if resp.status_code == 200:
                break
            print(f"      HTTP {resp.status_code}, retry {attempt+1}/3...")
            time.sleep(5 * (attempt + 1))
        except requests.RequestException as e:
            print(f"      Request error: {e}, retry {attempt+1}/3...")
            time.sleep(5 * (attempt + 1))
    else:
        print(f"   ❌ Failed to fetch {url} after 3 attempts")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Find the player-level table
    table_id = f"players_standard_{stat_type}"
    table = soup.find("table", id=table_id)
    if not table:
        # Fallback: try to find any table with matching comment-wrapped HTML
        # BR sometimes hides tables inside HTML comments
        for comment in soup.find_all(string=lambda t: isinstance(t, str) and table_id in str(t)):
            comment_soup = BeautifulSoup(str(comment), "lxml")
            table = comment_soup.find("table", id=table_id)
            if table:
                break

    if not table:
        print(f"   ❌ Could not find table '{table_id}' on page")
        return None

    # Extract headers
    thead = table.find("thead")
    headers = [th.get_text(strip=True) for th in thead.find_all("th")]

    # Extract rows
    tbody = table.find("tbody")
    rows = []
    for tr in tbody.find_all("tr"):
        # Skip spacer/header rows
        if tr.get("class") and ("thead" in tr["class"] or "spacer" in tr["class"]):
            continue

        cells = tr.find_all(["th", "td"])
        if len(cells) < len(headers):
            continue

        row = {}
        for i, cell in enumerate(cells):
            if i < len(headers):
                col_name = headers[i]
                text = cell.get_text(strip=True)
                row[col_name] = text

        # Extract bbref_id from data-append-csv or Player-additional column
        bbref_id = None
        # Method 1: data-append-csv on the row
        if tr.get("data-append-csv"):
            bbref_id = tr["data-append-csv"]
        # Method 2: data-append-csv on the player cell
        if not bbref_id:
            for cell in cells:
                if cell.get("data-append-csv"):
                    bbref_id = cell["data-append-csv"]
                    break
        # Method 3: Player-additional column (from CSV export)
        if not bbref_id and "Player-additional" in row:
            bbref_id = row["Player-additional"]
        # Method 4: player link href
        if not bbref_id:
            link = tr.find("a", href=re.compile(r"/players/"))
            if link:
                # /players/a/arandjo01.shtml → arandjo01
                bbref_id = link["href"].split("/")[-1].replace(".shtml", "")

        if not bbref_id:
            continue  # Skip rows without player ID (League Average, totals, etc.)

        row["bbref_id"] = bbref_id

        # Clean player name — remove handedness markers (*, #, +)
        if "Player" in row:
            row["Player"] = re.sub(r'[*#†+]', '', row.get("Player", "")).strip()

        rows.append(row)

    print(f"   ✅ Parsed {len(rows)} player rows from {stat_type} page")
    return rows


def _cast_row(row: dict, count_cols: list[str]) -> dict:
    """Cast string values to appropriate numeric types."""
    cleaned = {}
    for k, v in row.items():
        if v is None or v == "" or v == "—":
            cleaned[k] = None
            continue
        if k in count_cols:
            try:
                cleaned[k] = int(v)
            except (ValueError, TypeError):
                # Handle IP which can be like "6.1" (6 innings + 1 out)
                try:
                    cleaned[k] = float(v)
                except (ValueError, TypeError):
                    cleaned[k] = None
        elif k in ("BA", "OBP", "SLG", "OPS", "rOBA", "ERA", "FIP", "WHIP",
                    "H9", "HR9", "BB9", "SO9", "SO/BB", "W-L%"):
            try:
                cleaned[k] = float(v)
            except (ValueError, TypeError):
                cleaned[k] = None
        elif k in ("Age", "Rk", "OPS+", "ERA+"):
            try:
                cleaned[k] = int(v)
            except (ValueError, TypeError):
                cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned


# ========================= CUMULATIVE SNAPSHOTS =========================


def _save_cumulative_snapshot(rows: list[dict], year: int, stat_type: str,
                              snapshot_date: str) -> None:
    """Append a dated cumulative snapshot to the bronze parquet."""
    if not rows:
        return

    for r in rows:
        r["snapshot_date"] = snapshot_date

    target = CUMULATIVE_DIR / f"{stat_type}_cumulative_{year}.parquet"

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(rows, f, default=str)
        con = duckdb.connect()
        new_sql = f"SELECT * FROM read_json_auto('{tmp_path}')"
        if target.exists():
            final_sql = f"""
                SELECT * FROM read_parquet('{target}', union_by_name=true)
                UNION ALL BY NAME
                ({new_sql})
            """
        else:
            final_sql = new_sql
        con.execute(f"COPY ({final_sql}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0]
        con.close()
        print(f"   💾 Cumulative snapshot saved: {target.name} ({n:,} total rows)")
    finally:
        os.unlink(tmp_path)


def _load_snapshot(year: int, stat_type: str, snapshot_date: str) -> dict[str, dict] | None:
    """Load a cumulative snapshot for a given date. Returns {bbref_id: row_dict}."""
    target = CUMULATIVE_DIR / f"{stat_type}_cumulative_{year}.parquet"
    if not target.exists():
        return None

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT * FROM read_parquet('{target}')
            WHERE snapshot_date = '{snapshot_date}'
        """).fetchall()
        if not rows:
            return None
        cols = [d[0] for d in con.description]
        result = {}
        for row in rows:
            d = dict(zip(cols, row))
            bid = d.get("bbref_id")
            if bid:
                result[bid] = d
        return result
    finally:
        con.close()


def _get_latest_snapshot_date(year: int, stat_type: str, before_date: str) -> str | None:
    """Find the most recent snapshot date strictly before the given date."""
    target = CUMULATIVE_DIR / f"{stat_type}_cumulative_{year}.parquet"
    if not target.exists():
        return None

    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT MAX(snapshot_date) FROM read_parquet('{target}')
            WHERE snapshot_date < '{before_date}'
        """).fetchone()
        val = result[0] if result else None
        return str(val) if val else None
    finally:
        con.close()


def _snapshot_exists(year: int, stat_type: str, snapshot_date: str) -> bool:
    """Check if a cumulative snapshot already exists for this date."""
    target = CUMULATIVE_DIR / f"{stat_type}_cumulative_{year}.parquet"
    if not target.exists():
        return False

    con = duckdb.connect()
    try:
        n = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{target}')
            WHERE snapshot_date = '{snapshot_date}'
        """).fetchone()[0]
        return n > 0
    finally:
        con.close()


# ========================= DAILY STATS DIFFING =========================


def _compute_daily_batting(today: dict[str, dict],
                           yesterday: dict[str, dict] | None,
                           game_date: str, year: int) -> list[dict]:
    """Diff today's cumulative batting snapshot against yesterday's to get daily stats."""
    daily_rows = []

    for bbref_id, t_row in today.items():
        d = {"bbref_id": bbref_id, "game_date": game_date, "game_year": year}

        # Copy non-counting columns directly from today
        for col in ("Player", "Age", "Team", "Lg", "Pos", "Awards", "Rk"):
            if col in t_row:
                d[col] = t_row[col]

        # Diff counting stats
        y_row = yesterday.get(bbref_id, {}) if yesterday else {}
        for col in _BATTING_COUNT_COLS:
            t_val = t_row.get(col)
            y_val = y_row.get(col)
            if t_val is None:
                d[col] = None
                continue
            t_num = _to_num(t_val)
            y_num = _to_num(y_val) if y_val is not None else 0
            diff = t_num - y_num
            # If diff is 0 or negative and player existed yesterday, they didn't play
            # (negative can happen with stat corrections — treat as 0)
            d[col] = max(0, int(diff)) if diff == int(diff) else max(0.0, diff)

        # Skip players who didn't play today (0 PA)
        if d.get("PA", 0) == 0 and d.get("G", 0) == 0:
            continue

        # Recalculate rate stats from daily counting stats
        ab = d.get("AB", 0) or 0
        h = d.get("H", 0) or 0
        bb = d.get("BB", 0) or 0
        hbp = d.get("HBP", 0) or 0
        sf = d.get("SF", 0) or 0
        tb = d.get("TB", 0) or 0

        d["BA"] = round(h / ab, 3) if ab > 0 else None
        denom_obp = ab + bb + hbp + sf
        d["OBP"] = round((h + bb + hbp) / denom_obp, 3) if denom_obp > 0 else None
        d["SLG"] = round(tb / ab, 3) if ab > 0 else None
        if d["OBP"] is not None and d["SLG"] is not None:
            d["OPS"] = round(d["OBP"] + d["SLG"], 3)
        else:
            d["OPS"] = None

        daily_rows.append(d)

    return daily_rows


def _compute_daily_pitching(today: dict[str, dict],
                            yesterday: dict[str, dict] | None,
                            game_date: str, year: int) -> list[dict]:
    """Diff today's cumulative pitching snapshot against yesterday's."""
    daily_rows = []

    for bbref_id, t_row in today.items():
        d = {"bbref_id": bbref_id, "game_date": game_date, "game_year": year}

        for col in ("Player", "Age", "Team", "Lg", "Awards", "Rk"):
            if col in t_row:
                d[col] = t_row[col]

        y_row = yesterday.get(bbref_id, {}) if yesterday else {}

        # IP needs special handling — BR stores as innings.outs (6.1 = 6 innings + 1 out)
        t_ip = _ip_to_outs(t_row.get("IP"))
        y_ip = _ip_to_outs(y_row.get("IP")) if y_row else 0
        daily_outs = t_ip - y_ip
        d["IP"] = _outs_to_ip(max(0, daily_outs))

        for col in _PITCHING_COUNT_COLS:
            t_val = t_row.get(col)
            y_val = y_row.get(col)
            if t_val is None:
                d[col] = None
                continue
            t_num = _to_num(t_val)
            y_num = _to_num(y_val) if y_val is not None else 0
            d[col] = max(0, int(t_num - y_num))

        # Skip pitchers who didn't pitch today
        if daily_outs <= 0 and d.get("G", 0) == 0:
            continue

        # Recalculate rate stats
        ip = max(daily_outs / 3.0, 0)
        er = d.get("ER", 0) or 0
        h = d.get("H", 0) or 0
        bb = d.get("BB", 0) or 0

        d["ERA"] = round(er * 9.0 / ip, 2) if ip > 0 else None
        d["WHIP"] = round((h + bb) / ip, 3) if ip > 0 else None

        daily_rows.append(d)

    return daily_rows


def _to_num(val) -> float:
    """Convert a value to float, handling None/empty/string."""
    if val is None or val == "" or val == "—":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _ip_to_outs(ip_val) -> int:
    """Convert IP (e.g., 6.1 = 6 innings + 1 out) to total outs."""
    if ip_val is None or ip_val == "" or ip_val == "—":
        return 0
    try:
        ip = float(ip_val)
        full = int(ip)
        frac = round((ip - full) * 10)  # .1 = 1 out, .2 = 2 outs
        return full * 3 + frac
    except (ValueError, TypeError):
        return 0


def _outs_to_ip(outs: int) -> float:
    """Convert total outs to IP notation (e.g., 19 outs = 6.1)."""
    full = outs // 3
    remainder = outs % 3
    return float(f"{full}.{remainder}")


# ========================= SAVE DAILY GAME LOGS =========================


def _save_daily_game_logs(rows: list[dict], year: int, stat_type: str) -> None:
    """Append daily stats rows to the game-by-game parquet file."""
    if not rows:
        return

    prefix = "batting" if stat_type == "batting" else "pitching"
    target = GAME_BY_GAME_DIR / f"{prefix}_game_logs_{year}.parquet"

    # Write new rows to a temp parquet, then UNION ALL BY NAME with existing.
    # Use DuckDB table insert to avoid JSON type inference mismatches.
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(rows, f, default=str)
        con = duckdb.connect()

        if target.exists():
            # Load existing data into a table, then INSERT new rows with matching types
            con.execute(f"""
                CREATE TABLE existing AS
                SELECT * FROM read_parquet('{target}')
            """)
            # Get column names and types from existing table
            cols_info = con.execute("DESCRIBE existing").fetchall()
            # Build cast expressions for each column in the new data
            cast_exprs = []
            for col_name, col_type, *_ in cols_info:
                cast_exprs.append(f'CAST("{col_name}" AS {col_type}) AS "{col_name}"')
            cast_sql = ", ".join(cast_exprs)
            # Insert new rows with types matching existing schema
            con.execute(f"""
                INSERT INTO existing
                SELECT {cast_sql}
                FROM read_json_auto('{tmp_path}')
            """)
            con.execute(f"""
                COPY existing TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            n = con.execute(f"SELECT COUNT(*) FROM existing").fetchone()[0]
        else:
            # No existing file — cast all string-like cols to VARCHAR to avoid JSON type
            con.execute(f"""
                CREATE TABLE new_data AS
                SELECT * FROM read_json_auto('{tmp_path}')
            """)
            # Cast any JSON columns to VARCHAR
            cols_info = con.execute("DESCRIBE new_data").fetchall()
            cast_exprs = []
            for col_name, col_type, *_ in cols_info:
                if col_type == "JSON":
                    cast_exprs.append(f'CAST("{col_name}" AS VARCHAR) AS "{col_name}"')
                else:
                    cast_exprs.append(f'"{col_name}"')
            cast_sql = ", ".join(cast_exprs)
            con.execute(f"""
                COPY (SELECT {cast_sql} FROM new_data)
                TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            n = con.execute(f"SELECT COUNT(*) FROM new_data").fetchone()[0]

        con.close()
        print(f"   ✅ {prefix}_game_logs_{year}.parquet: {n:,} total rows")
    finally:
        os.unlink(tmp_path)


# ========================= INTERPOLATION =========================


def _get_game_dates_in_range(year: int, after_date: date, through_date: date) -> list[date]:
    """Get game dates strictly after after_date and up to/including through_date."""
    flat = BASE_DIR / "data" / "schedules" / f"games_{year}.parquet"
    legacy = BASE_DIR / "data" / "raw" / "schedules" / f"games_{year}.parquet"

    if flat.exists():
        sched_path = flat
    elif legacy.exists():
        sched_path = legacy
    else:
        return []

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT DISTINCT game_date::DATE AS gd
            FROM read_parquet('{sched_path}')
            WHERE game_type = 'R'
              AND game_date::DATE > '{after_date}'
              AND game_date::DATE <= '{through_date}'
            ORDER BY gd
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def _interpolate_rows(total_daily: list[dict], n_days: int,
                      game_date: str, year: int, stat_type: str) -> list[dict]:
    """Split total diff rows evenly across n_days for a single game_date.

    Counting stats are divided by n_days (floored to int, with remainders
    distributed round-robin to earlier days). Rate stats are recalculated
    from the split counting stats.
    """
    count_cols = _BATTING_COUNT_COLS if stat_type == "batting" else _PITCHING_COUNT_COLS
    interpolated = []

    for row in total_daily:
        d = {}
        # Copy non-counting columns
        for k, v in row.items():
            if k not in count_cols and k not in ("BA", "OBP", "SLG", "OPS", "ERA", "WHIP"):
                d[k] = v
        d["game_date"] = game_date
        d["game_year"] = year

        # Split counting stats evenly
        for col in count_cols:
            total_val = row.get(col)
            if total_val is None:
                d[col] = None
            else:
                d[col] = round(total_val / n_days, 2)

        # Recalculate rate stats
        if stat_type == "batting":
            ab = d.get("AB", 0) or 0
            h = d.get("H", 0) or 0
            bb = d.get("BB", 0) or 0
            hbp = d.get("HBP", 0) or 0
            sf = d.get("SF", 0) or 0
            tb = d.get("TB", 0) or 0
            d["BA"] = round(h / ab, 3) if ab > 0 else None
            denom_obp = ab + bb + hbp + sf
            d["OBP"] = round((h + bb + hbp) / denom_obp, 3) if denom_obp > 0 else None
            d["SLG"] = round(tb / ab, 3) if ab > 0 else None
            if d["OBP"] is not None and d["SLG"] is not None:
                d["OPS"] = round(d["OBP"] + d["SLG"], 3)
            else:
                d["OPS"] = None
        else:
            ip_val = d.get("IP", 0) or 0
            ip_innings = ip_val if isinstance(ip_val, (int, float)) else 0
            er = d.get("ER", 0) or 0
            h = d.get("H", 0) or 0
            bb = d.get("BB", 0) or 0
            d["ERA"] = round(er * 9.0 / ip_innings, 2) if ip_innings > 0 else None
            d["WHIP"] = round((h + bb) / ip_innings, 3) if ip_innings > 0 else None

        interpolated.append(d)

    return interpolated


# ========================= MAIN ORCHESTRATOR =========================


def _get_game_dates_from_schedule(year: int) -> list[date]:
    """Load game dates from schedule parquet, filtering to past/today only."""
    flat = BASE_DIR / "data" / "schedules" / f"games_{year}.parquet"
    legacy = BASE_DIR / "data" / "raw" / "schedules" / f"games_{year}.parquet"

    if flat.exists():
        sched_path = flat
    elif legacy.exists():
        sched_path = legacy
    else:
        print(f"   ❌ No schedule file for {year}")
        return []

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT DISTINCT game_date::DATE AS gd
            FROM read_parquet('{sched_path}')
            WHERE game_type = 'R'
              AND game_date::DATE <= CURRENT_DATE
            ORDER BY gd
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def _get_processed_dates(year: int, stat_type: str) -> set[str]:
    """Find which dates already have daily game logs saved."""
    prefix = "batting" if stat_type == "batting" else "pitching"
    target = GAME_BY_GAME_DIR / f"{prefix}_game_logs_{year}.parquet"
    if not target.exists():
        return set()

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT DISTINCT game_date::VARCHAR FROM read_parquet('{target}')
        """).fetchall()
        return {str(r[0])[:10] for r in rows}
    finally:
        con.close()


def fetch_daily_logs(year: int = 2026) -> None:
    """Fetch daily batting + pitching logs for the given year using cumulative snapshot diffing.

    For each game date not yet processed:
      1. Scrape the current BR standard page (cumulative stats for the season so far)
      2. Store the cumulative snapshot
      3. Diff against the previous snapshot to extract single-day stats
      4. Save daily stats to the game-by-game parquet

    Note: Since BR only shows the CURRENT cumulative, this must be run daily to capture
    each day's diff. For the first day (or if no previous snapshot exists), the cumulative
    stats ARE the daily stats.
    """
    print(f"📥 Fetching daily logs for {year} (cumulative snapshot method)...")

    game_dates = _get_game_dates_from_schedule(year)
    if not game_dates:
        print("   No game dates found (check schedule parquet)")
        return

    today = date.today()
    game_dates = [d for d in game_dates if d <= today]
    print(f"   → {len(game_dates)} game dates up to today")

    for stat_type in ("batting", "pitching"):
        print(f"\n--- {stat_type.upper()} ---")
        processed = _get_processed_dates(year, stat_type)
        remaining = [d for d in game_dates if str(d) not in processed]
        print(f"   Already processed: {len(processed)} days")
        print(f"   Remaining: {len(remaining)} days")

        if not remaining:
            print("   ✅ All dates already processed")
            continue

        # We can only scrape the CURRENT cumulative snapshot from BR.
        # So we scrape once and use it to compute today's daily stats.
        # For past days, we need existing cumulative snapshots.

        # Scrape current cumulative
        count_cols = _BATTING_COUNT_COLS if stat_type == "batting" else _PITCHING_COUNT_COLS
        raw_rows = _scrape_standard_page(year, stat_type)
        if not raw_rows:
            print(f"   ❌ Could not fetch {stat_type} page — skipping")
            continue

        cast_rows = [_cast_row(r, count_cols) for r in raw_rows]

        # What date does this snapshot represent?
        # It's the cumulative through the most recent game date <= today.
        snapshot_date = str(today)

        # Save cumulative snapshot for today
        if not _snapshot_exists(year, stat_type, snapshot_date):
            _save_cumulative_snapshot(cast_rows, year, stat_type, snapshot_date)
        else:
            print(f"   ⏭️  Snapshot for {snapshot_date} already exists")

        # Build {bbref_id: row} lookup for today
        today_lookup = {r["bbref_id"]: r for r in cast_rows if r.get("bbref_id")}

        # Process each remaining date
        for gd in remaining:
            gd_str = str(gd)

            if gd == today or gd == max(d for d in game_dates if d <= today):
                # For today (or the most recent game day), use the snapshot we just scraped
                prev_date = _get_latest_snapshot_date(year, stat_type, snapshot_date)
                prev_lookup = _load_snapshot(year, stat_type, prev_date) if prev_date else None

                if stat_type == "batting":
                    daily = _compute_daily_batting(today_lookup, prev_lookup, gd_str, year)
                else:
                    daily = _compute_daily_pitching(today_lookup, prev_lookup, gd_str, year)

                _save_daily_game_logs(daily, year, stat_type)
                print(f"   📅 {gd_str}: {len(daily)} {stat_type} rows")
            else:
                # Past dates require pre-existing consecutive snapshots
                snap_today = _load_snapshot(year, stat_type, gd_str)
                if not snap_today:
                    print(f"   ⚠️  No snapshot for {gd_str} — cannot compute daily stats. "
                          f"Need to have scraped on that date.")
                    continue

                prev_date = _get_latest_snapshot_date(year, stat_type, gd_str)
                prev_lookup = _load_snapshot(year, stat_type, prev_date) if prev_date else None

                if stat_type == "batting":
                    daily = _compute_daily_batting(snap_today, prev_lookup, gd_str, year)
                else:
                    daily = _compute_daily_pitching(snap_today, prev_lookup, gd_str, year)

                _save_daily_game_logs(daily, year, stat_type)
                print(f"   📅 {gd_str}: {len(daily)} {stat_type} rows")

        time.sleep(random.uniform(3, 6))  # Be polite between batting/pitching

    print(f"\n🎉 Done! Check {GAME_BY_GAME_DIR}")


def ingest_csv(csv_path: str, game_date: str, year: int = 2026) -> None:
    """Ingest a manually downloaded BR standard batting/pitching CSV.

    Daily workflow:
      1. Download CSV from BR (standard batting or pitching page)
      2. Save it anywhere (e.g., data/raw/player_logs/game_by_game/player/)
      3. Run: python player_logs_fetcher.py --csv PATH --date YYYY-MM-DD

    The function:
      - Reads the CSV
      - Stores it as a cumulative snapshot for the given date
      - Loads the previous day's snapshot (if any) and diffs to get single-day stats
      - Appends the daily stats to the game-by-game parquet

    If no previous snapshot exists, cumulative = daily (correct for day 1,
    and also a reasonable approximation for a missed day).
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"   ❌ File not found: {csv_file}")
        return

    print(f"📥 Ingesting {csv_file.name} as cumulative stats through {game_date}...")

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT * FROM read_csv_auto('{csv_file}')
        """).fetchall()
        cols = [d[0] for d in con.description]
    finally:
        con.close()

    # Convert to list of dicts
    data = []
    for row in rows:
        d = dict(zip(cols, row))
        # Map Player-additional → bbref_id
        pid = d.get("Player-additional")
        if pid and str(pid).strip() and str(pid) != "-9999":
            d["bbref_id"] = d.pop("Player-additional")
        else:
            continue  # Skip rows without player ID (League Average, totals)
        # Clean player name — remove handedness markers
        if "Player" in d:
            d["Player"] = re.sub(r'[*#†+]', '', str(d.get("Player", ""))).strip()
        data.append(d)

    if not data:
        print("   ❌ No player rows found in CSV")
        return

    # Detect stat type from columns
    stat_type = "batting"
    if "ERA" in cols or "IP" in cols:
        stat_type = "pitching"

    count_cols = _BATTING_COUNT_COLS if stat_type == "batting" else _PITCHING_COUNT_COLS
    cast_data = [_cast_row(r, count_cols) for r in data]

    # 1. Save cumulative snapshot
    if not _snapshot_exists(year, stat_type, game_date):
        _save_cumulative_snapshot(cast_data, year, stat_type, game_date)
    else:
        print(f"   ⏭️  Snapshot for {game_date} already exists — overwriting")
        # Delete existing snapshot for this date and re-save
        _delete_snapshot(year, stat_type, game_date)
        _save_cumulative_snapshot(cast_data, year, stat_type, game_date)

    # 2. Load previous snapshot for diffing
    prev_date = _get_latest_snapshot_date(year, stat_type, game_date)
    prev_lookup = _load_snapshot(year, stat_type, prev_date) if prev_date else None

    if prev_lookup:
        print(f"   📊 Diffing against previous snapshot ({prev_date})")
    else:
        print(f"   📊 No previous snapshot — treating cumulative as daily stats")

    # 3. Compute daily stats — with interpolation for missed days
    today_lookup = {r["bbref_id"]: r for r in cast_data if r.get("bbref_id")}

    # Determine how many game days this diff covers
    prev_dt = date.fromisoformat(prev_date) if prev_date else None
    curr_dt = date.fromisoformat(game_date)

    if prev_dt and (curr_dt - prev_dt).days > 1:
        # Multi-day gap — find game dates in between (inclusive of current)
        game_dates_in_range = _get_game_dates_in_range(year, prev_dt, curr_dt)
        if not game_dates_in_range:
            # No schedule data — fall back to assuming every day was a game day
            game_dates_in_range = [
                prev_dt + timedelta(days=i)
                for i in range(1, (curr_dt - prev_dt).days + 1)
            ]
        n_days = len(game_dates_in_range)
        print(f"   📅 Gap detected: {(curr_dt - prev_dt).days} days, "
              f"{n_days} game day(s) to interpolate")

        # Compute total diff once, then split across days
        if stat_type == "batting":
            total_daily = _compute_daily_batting(today_lookup, prev_lookup, game_date, year)
        else:
            total_daily = _compute_daily_pitching(today_lookup, prev_lookup, game_date, year)

        processed = _get_processed_dates(year, stat_type)
        for day_idx, gd in enumerate(game_dates_in_range):
            gd_str = str(gd)
            interpolated = _interpolate_rows(total_daily, n_days, gd_str, year, stat_type)

            if gd_str in processed:
                print(f"   ⚠️  Daily game logs for {gd_str} already exist — replacing")
                _delete_daily_logs(year, stat_type, gd_str)

            _save_daily_game_logs(interpolated, year, stat_type)
            print(f"   ✅ {len(interpolated)} {stat_type} rows for {gd_str} (interpolated)")
    else:
        # Normal case: single day diff
        if stat_type == "batting":
            daily = _compute_daily_batting(today_lookup, prev_lookup, game_date, year)
        else:
            daily = _compute_daily_pitching(today_lookup, prev_lookup, game_date, year)

        processed = _get_processed_dates(year, stat_type)
        if game_date in processed:
            print(f"   ⚠️  Daily game logs for {game_date} already exist — replacing")
            _delete_daily_logs(year, stat_type, game_date)

        _save_daily_game_logs(daily, year, stat_type)
        print(f"   ✅ {len(daily)} {stat_type} rows for {game_date}")


def _delete_snapshot(year: int, stat_type: str, snapshot_date: str) -> None:
    """Remove a specific date's snapshot from the cumulative parquet."""
    target = CUMULATIVE_DIR / f"{stat_type}_cumulative_{year}.parquet"
    if not target.exists():
        return
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{target}')
                WHERE snapshot_date != '{snapshot_date}'
            ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()


def _delete_daily_logs(year: int, stat_type: str, game_date: str) -> None:
    """Remove a specific date's rows from the daily game logs parquet."""
    prefix = "batting" if stat_type == "batting" else "pitching"
    target = GAME_BY_GAME_DIR / f"{prefix}_game_logs_{year}.parquet"
    if not target.exists():
        return
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{target}')
                WHERE game_date::VARCHAR NOT LIKE '{game_date}%'
            ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()


# ========================= LEGACY MODE (pre-2026) =========================


def fetch_game_logs_legacy(year: int) -> None:
    """Original pybaseball-based fetcher for years where it still works (2017-2025).

    Kept as-is for backward compatibility — only use for historical years
    where data is already partially fetched.
    """
    import pandas as pd
    import pybaseball as pb_legacy
    from tqdm import tqdm as tqdm_legacy

    print(f"📥 Starting / resuming daily BR logs fetch for {year} (legacy mode)...")

    flat_path = BASE_DIR / "data" / "schedules" / f"games_{year}.parquet"
    sub_path = BASE_DIR / "data" / "raw" / "schedules" / f"games_{year}.parquet"

    if flat_path.exists():
        schedule_path = flat_path
    elif sub_path.exists():
        schedule_path = sub_path
    else:
        print(f"❌ No schedule file for {year}")
        return

    schedule = pd.read_parquet(schedule_path)
    schedule['game_date'] = pd.to_datetime(schedule['game_date'])
    if 'game_type' in schedule.columns:
        regular = schedule[schedule['game_type'] == 'R'].copy()
    else:
        regular = schedule[schedule['game_date'] >= f'{year}-03-20'].copy()
    all_dates = sorted(regular['game_date'].dt.date.unique())
    print(f"   → {len(all_dates)} regular-season days total")

    out_dir = BASE_DIR / "data" / "raw" / "player_logs" / "game_by_game"
    out_dir.mkdir(parents=True, exist_ok=True)
    batting_path = out_dir / f"batting_game_logs_{year}.parquet"
    pitching_path = out_dir / f"pitching_game_logs_{year}.parquet"

    batting_dfs, pitching_dfs, processed_dates = [], [], set()
    if batting_path.exists():
        existing = pd.read_parquet(batting_path)
        if 'game_date' in existing.columns:
            processed_dates = set(existing['game_date'].dt.date.unique())
            batting_dfs.append(existing)
            print(f"   Already done: {len(processed_dates)} days")

    remaining = [d for d in all_dates if d not in processed_dates]
    print(f"   → {len(remaining)} days still needed\n")

    # Lahman reference for team enrichment
    lahman_dir = BASE_DIR / "data" / "reference" / "lahman_files"
    teams_df = pd.read_parquet(lahman_dir / "historical_teams_data.parquet")

    def _team_info(yr, tm):
        match = teams_df[(teams_df['yearID'] == yr) & (teams_df['teamID'] == tm)]
        if not match.empty:
            r = match.iloc[0]
            return r['lgID'], r['name']
        fb = teams_df[teams_df['teamID'] == tm].sort_values('yearID', ascending=False)
        if not fb.empty:
            r = fb.iloc[0]
            return r['lgID'], r['name']
        return pd.NA, f"Unknown ({tm})"

    for i, game_date in enumerate(tqdm_legacy(remaining, desc="Fetching")):
        date_str = game_date.strftime('%Y-%m-%d')
        success = False
        for attempt in range(5):
            try:
                bat = pb_legacy.batting_stats_range(date_str, date_str)
                pit = pb_legacy.pitching_stats_range(date_str, date_str)
                time.sleep(random.uniform(4.5, 8.5))
                success = True
                break
            except Exception:
                time.sleep(12 * (2 ** attempt))

        if not success:
            print(f"   ❌ Failed {date_str}")
            continue

        if not bat.empty:
            bat = bat.copy()
            bat['game_date'] = pd.to_datetime(date_str)
            bat['game_year'] = year
            bat[['Lg', 'full_team_name']] = bat.apply(
                lambda r: pd.Series(_team_info(year, r['Tm'])), axis=1)
            bat['Lg'] = bat.Lev.str.split('-').str[1]
            batting_dfs.append(bat)

        if not pit.empty:
            pit = pit.copy()
            pit['game_date'] = pd.to_datetime(date_str)
            pit['game_year'] = year
            pit[['Lg', 'full_team_name']] = pit.apply(
                lambda r: pd.Series(_team_info(year, r['Tm'])), axis=1)
            pit['Lg'] = pit.Lev.str.split('-').str[1]
            pitching_dfs.append(pit)

        if (i + 1) % 10 == 0 or (i + 1) == len(remaining):
            print(f"   💾 Checkpoint after {i+1} days...")
            if batting_dfs:
                pd.concat(batting_dfs, ignore_index=True).to_parquet(batting_path, index=False)
            if pitching_dfs:
                pd.concat(pitching_dfs, ignore_index=True).to_parquet(pitching_path, index=False)

    print(f"\n🎉 {year} complete!")


# ========================= ENTRY POINT =========================


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch MLB player game logs from Baseball Reference",
        epilog="""
Examples:
  # Ingest a manually downloaded CSV (daily workflow for 2026+):
  python player_logs_fetcher.py --csv data/raw/.../sb_1.csv --date 2026-03-27

  # Auto-scrape today's cumulative and diff (requires BR access):
  python player_logs_fetcher.py 2026

  # Legacy pybaseball mode for historical years:
  python player_logs_fetcher.py 2025 --legacy
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("year", type=int, nargs="?", default=2026, help="Season year")
    parser.add_argument("--csv", type=str,
                        help="Path to a BR standard batting/pitching CSV to ingest")
    parser.add_argument("--date", type=str,
                        help="Game date for the CSV snapshot (YYYY-MM-DD)")
    parser.add_argument("--legacy", action="store_true",
                        help="Use legacy pybaseball mode (for pre-2026 years)")
    args = parser.parse_args()

    if args.csv:
        csv_date = args.date
        if not csv_date:
            # Try to parse date from MMDDYYYY filename
            stem = Path(args.csv).stem
            if re.match(r'^\d{8}$', stem):
                try:
                    parsed = datetime.strptime(stem, "%m%d%Y")
                    csv_date = parsed.strftime("%Y-%m-%d")
                    print(f"   📅 Parsed date from filename: {csv_date}")
                except ValueError:
                    pass
        if not csv_date:
            print("❌ --date is required (or name your CSV as MMDDYYYY.csv)")
            print("   Example: --date 2026-03-27  or  03272026.csv")
            sys.exit(1)
        ingest_csv(args.csv, csv_date, args.year)
    elif args.legacy or args.year < 2026:
        fetch_game_logs_legacy(args.year)
    else:
        fetch_daily_logs(args.year)
