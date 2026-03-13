"""
Central DuckDB manager.

Every connection registers views over all Parquet data directories so any
script or notebook can query the full data lake without knowing file paths.

Views created automatically:
    v_odds_the_odds_api   — data/bronze/odds/the_odds_api_*.parquet
    v_odds_kalshi         — data/bronze/odds/kalshi_*.parquet
    v_odds_polymarket     — data/bronze/odds/polymarket_*.parquet
    v_bronze_lineups      — data/bronze/lineups/lineups_*.parquet
    v_silver_lineups      — data/silver/lineups/lineups_*.parquet
    v_schedules           — data/schedules/*.parquet
    v_game_logs_batting   — data/player_logs/game_by_game/batting_*.parquet
    v_game_logs_pitching  — data/player_logs/game_by_game/pitching_*.parquet
    v_fangraphs_batting   — data/player_logs/fangraphs_leaderboards/batting_*.parquet
    v_fangraphs_pitching  — data/player_logs/fangraphs_leaderboards/pitching_*.parquet
    v_linear_weights      — data/reference/linear_weights.parquet
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import duckdb
from datetime import datetime, timezone
from config.settings import DB_PATH, BASE_DIR

# ---------------------------------------------------------------------------
# View definitions: (view_name, glob_pattern_relative_to_BASE_DIR)
# Only created if at least one matching file exists.
# ---------------------------------------------------------------------------
_VIEWS = [
    ("v_odds_the_odds_api",  "data/bronze/odds/the_odds_api_*.parquet"),
    ("v_odds_kalshi",        "data/bronze/odds/kalshi_*.parquet"),
    ("v_odds_polymarket",    "data/bronze/odds/polymarket_*.parquet"),
    ("v_bronze_lineups",     "data/bronze/lineups/lineups_*.parquet"),
    ("v_silver_lineups",     "data/silver/lineups/lineups_*.parquet"),
    ("v_schedules",          "data/schedules/*.parquet"),
    ("v_game_logs_batting",  "data/player_logs/game_by_game/batting_*.parquet"),
    ("v_game_logs_pitching", "data/player_logs/game_by_game/pitching_*.parquet"),
    ("v_fangraphs_batting",  "data/player_logs/fangraphs_leaderboards/batting_*.parquet"),
    ("v_fangraphs_pitching", "data/player_logs/fangraphs_leaderboards/pitching_*.parquet"),
    ("v_linear_weights",     "data/reference/linear_weights.parquet"),
]


def register_views(con: duckdb.DuckDBPyConnection) -> None:
    """Register all data-lake views on an open connection. Skips missing files silently."""
    for view_name, rel_pattern in _VIEWS:
        abs_pattern = str(BASE_DIR / rel_pattern)
        # Only create the view if at least one matching file exists
        matching = sorted(BASE_DIR.glob(rel_pattern))
        if not matching:
            continue
        try:
            con.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{abs_pattern}', union_by_name=true)
            """)
        except Exception as e:
            print(f"   ⚠️  Could not create view {view_name}: {e}")


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with all data-lake views registered."""
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    if not read_only:
        register_views(con)
    return con


def get_views() -> None:
    """Print all registered views and their row counts. Useful for sanity checks."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    register_views(con)

    print(f"\n{'View':<25} {'Rows':>10}  Pattern")
    print("-" * 70)
    for view_name, rel_pattern in _VIEWS:
        matching = sorted((BASE_DIR).glob(rel_pattern))
        if not matching:
            print(f"  {view_name:<23} {'(no files)':>10}  {rel_pattern}")
            continue
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            print(f"  {view_name:<23} {n:>10,}  {rel_pattern}")
        except Exception as e:
            print(f"  {view_name:<23} {'(error)':>10}  {e}")
    print()
    con.close()


def append_to_table(df, table_name: str) -> None:
    """
    Schema-evolving append to a DuckDB table.
    Creates the table on first run, adds new columns automatically.
    Always stamps a fetch_timestamp.

    NOTE: For odds/lineups, prefer appending to Parquet files directly
    (the views will pick up new data automatically). Use this for small
    reference tables or debug data that doesn't have a Parquet home.
    """
    if df is None or df.empty:
        print(f"⚠️  No data to append to {table_name}")
        return

    import pandas as pd  # only needed here; pandas may not always be available

    df = df.copy()
    con = get_connection()

    table_exists = con.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    ).fetchone() is not None

    if not table_exists:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df LIMIT 0")

    existing_cols = [row[0] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]

    if "fetch_timestamp" not in existing_cols:
        try:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN fetch_timestamp TIMESTAMP")
        except Exception:
            pass

    for col in df.columns:
        if col not in existing_cols and col != "fetch_timestamp":
            try:
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" VARCHAR')
            except Exception:
                pass

    df["fetch_timestamp"] = datetime.now(timezone.utc)
    con.register("_append_df", df)
    con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM _append_df")
    print(f"✅ Appended {len(df)} rows to '{table_name}'")
    con.close()


if __name__ == "__main__":
    get_views()
