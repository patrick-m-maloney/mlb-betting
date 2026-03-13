#!/usr/bin/env python3
"""
One-time data reorganization script.
Uses DuckDB only — no pandas dependency.

Consolidates scattered daily snapshot files into per-year append-only parquet files
and copies directories to match the target data architecture.

Usage:
    python scripts/reorganize_data.py --dry-run   # Preview changes without writing
    python scripts/reorganize_data.py              # Execute changes
"""

import sys
import argparse
import shutil
from pathlib import Path
from datetime import date

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def glob_parquet(pattern: str) -> list[Path]:
    """Expand a glob pattern and return sorted list of existing parquet paths."""
    return sorted(Path(ROOT).glob(pattern))


def count_rows(con: duckdb.DuckDBPyConnection, file_or_glob: str) -> int:
    """Return row count for a parquet file or glob pattern. Returns 0 on error."""
    try:
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_or_glob}', union_by_name=true)").fetchone()[0]
    except Exception:
        return 0


def table_columns(con: duckdb.DuckDBPyConnection, file_or_glob: str) -> list[str]:
    """Return column names from a parquet file or glob. Returns [] on error."""
    try:
        return [row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{file_or_glob}', union_by_name=true) LIMIT 0"
        ).fetchall()]
    except Exception:
        return []


# ============================================================
# ODDS CONSOLIDATION
# ============================================================

def consolidate_odds(dry_run: bool) -> dict:
    """
    Consolidate data/raw/odds/YYYY-MM-DD/{games,futures}/*.parquet
    into data/bronze/odds/the_odds_api_YYYY.parquet
    All existing files are from The Odds API.
    """
    print("\n" + "=" * 60)
    print("TASK: Consolidate odds snapshots → data/bronze/odds/")
    print("=" * 60)

    source_dir = DATA_DIR / "raw" / "odds"
    target_dir = DATA_DIR / "bronze" / "odds"

    if not source_dir.exists():
        print("   ⚠️  No source odds directory found, skipping")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    # Discover all parquet files grouped by date folder
    source_files = sorted(source_dir.glob("*/*/*.parquet"))
    if not source_files:
        print("   No odds parquet files found")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    print(f"   Found {len(source_files)} source files across {source_dir}")

    con = duckdb.connect()

    # Build a list of (file_path, snapshot_date, subfolder) tuples
    # Register each file individually so we can add metadata columns
    union_parts = []
    source_date_folders = set()

    for pf in source_files:
        # Structure: raw/odds/YYYY-MM-DD/games|futures/filename.parquet
        snapshot_date = pf.parent.parent.name   # YYYY-MM-DD
        source_date_folders.add(pf.parent.parent)

        cols = table_columns(con, str(pf))
        if not cols:
            print(f"   ⚠️  Could not read columns from {pf.name}, skipping")
            continue

        has_snapshot_timestamp = "snapshot_timestamp" in cols
        has_fetch_timestamp    = "fetch_timestamp" in cols
        has_source             = "source" in cols
        has_snapshot_date      = "snapshot_date" in cols

        extra = []
        if not has_snapshot_date:
            extra.append(f"'{snapshot_date}' AS snapshot_date")
        if not has_source:
            extra.append("'the_odds_api' AS source")
        if not has_snapshot_timestamp:
            if has_fetch_timestamp:
                extra.append("fetch_timestamp AS snapshot_timestamp")
            else:
                extra.append(f"'{snapshot_date}' AS snapshot_timestamp")

        extra_sql = (", " + ", ".join(extra)) if extra else ""
        union_parts.append(
            f"SELECT *{extra_sql} FROM read_parquet('{pf}')"
        )

    if not union_parts:
        print("   No readable odds files found")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    # Count before
    all_union_sql = " UNION ALL BY NAME ".join(union_parts)
    rows_before = con.execute(f"SELECT COUNT(*) FROM ({all_union_sql})").fetchone()[0]
    print(f"   Total rows across source files: {rows_before}")

    # Extract year and group
    years_sql = f"SELECT DISTINCT year(strptime(snapshot_date, '%Y-%m-%d')) AS yr FROM ({all_union_sql})"
    years = [row[0] for row in con.execute(years_sql).fetchall()]

    rows_after = 0
    for year in sorted(years):
        target_file = target_dir / f"the_odds_api_{year}.parquet"
        year_sql = f"""
            SELECT * FROM ({all_union_sql})
            WHERE year(strptime(snapshot_date, '%Y-%m-%d')) = {year}
        """

        # Idempotency: if target exists, union with it and dedup
        if target_file.exists():
            year_sql = f"""
                SELECT * FROM read_parquet('{target_file}', union_by_name=true)
                UNION ALL BY NAME
                ({year_sql})
            """

        dedup_cols = ", ".join(
            c for c in ["snapshot_timestamp", "fetch_timestamp", "game_id", "bookmaker", "market", "outcome_name"]
            if c in table_columns(con, str(source_files[0]))
        )
        if dedup_cols:
            final_sql = f"""
                SELECT * FROM ({year_sql})
                QUALIFY row_number() OVER (
                    PARTITION BY {dedup_cols}
                    ORDER BY snapshot_timestamp DESC NULLS LAST
                ) = 1
            """
        else:
            final_sql = year_sql

        n = con.execute(f"SELECT COUNT(*) FROM ({final_sql})").fetchone()[0]
        rows_after += n

        if dry_run:
            print(f"   [DRY RUN] Would write {n} rows → {target_file.relative_to(ROOT)}")
        else:
            target_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY ({final_sql}) TO '{target_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            actual = count_rows(con, str(target_file))
            print(f"   ✅ Wrote {actual} rows → {target_file.relative_to(ROOT)}")

    con.close()
    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "folders_to_delete": sorted(str(f) for f in source_date_folders),
    }


# ============================================================
# BRONZE LINEUP CONSOLIDATION
# ============================================================

def consolidate_bronze_lineups(dry_run: bool) -> dict:
    """
    Consolidate data/bronze/lineups/YYYY-MM-DD/lineups_*.parquet
    into data/bronze/lineups/lineups_YYYY.parquet
    """
    print("\n" + "=" * 60)
    print("TASK: Consolidate bronze lineup snapshots → lineups_YYYY.parquet")
    print("=" * 60)

    lineups_dir = DATA_DIR / "bronze" / "lineups"
    if not lineups_dir.exists():
        print("   ⚠️  Bronze lineups directory not found, skipping")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    # Only grab files inside date-named subdirs (YYYY-MM-DD), not top-level yearly files
    source_files = sorted(
        pf for pf in lineups_dir.glob("*/*.parquet")
        if pf.parent.name[0].isdigit()
    )
    source_folders = sorted(set(pf.parent for pf in source_files))

    if not source_files:
        print("   No bronze lineup snapshots to consolidate (already done?)")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    print(f"   Found {len(source_files)} source files in {len(source_folders)} date folder(s)")

    con = duckdb.connect()
    glob_pattern = str(lineups_dir / "*" / "*.parquet").replace("*/*", "????-??-??/*")
    # Use explicit union of source files to avoid picking up yearly consolidated files
    union_sql = " UNION ALL BY NAME ".join(
        f"SELECT * FROM read_parquet('{pf}')" for pf in source_files
    )
    rows_before = con.execute(f"SELECT COUNT(*) FROM ({union_sql})").fetchone()[0]
    print(f"   Total rows across source files: {rows_before}")

    years = [row[0] for row in con.execute(
        f"SELECT DISTINCT year(game_date) AS yr FROM ({union_sql}) ORDER BY yr"
    ).fetchall()]

    rows_after = 0
    for year in years:
        target_file = lineups_dir / f"lineups_{year}.parquet"
        year_sql = f"SELECT * FROM ({union_sql}) WHERE year(game_date) = {year}"

        if target_file.exists():
            year_sql = f"""
                SELECT * FROM read_parquet('{target_file}', union_by_name=true)
                UNION ALL BY NAME ({year_sql})
            """

        final_sql = f"""
            SELECT * FROM ({year_sql})
            QUALIFY row_number() OVER (
                PARTITION BY fetch_timestamp, game_date, away_team, home_team
                ORDER BY fetch_timestamp DESC NULLS LAST
            ) = 1
        """

        n = con.execute(f"SELECT COUNT(*) FROM ({final_sql})").fetchone()[0]
        rows_after += n

        if dry_run:
            print(f"   [DRY RUN] Would write {n} rows → {target_file.relative_to(ROOT)}")
        else:
            con.execute(f"COPY ({final_sql}) TO '{target_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            actual = count_rows(con, str(target_file))
            print(f"   ✅ Wrote {actual} rows → {target_file.relative_to(ROOT)}")

    con.close()
    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "folders_to_delete": [str(f) for f in source_folders],
    }


# ============================================================
# SILVER LINEUP CONSOLIDATION
# ============================================================

def consolidate_silver_lineups(dry_run: bool) -> dict:
    """
    Consolidate data/silver/lineups/year=*/month=*/day=*/lineups.parquet
    into data/silver/lineups/lineups_YYYY.parquet  (deduped)
    """
    print("\n" + "=" * 60)
    print("TASK: Consolidate silver lineup partitions → lineups_YYYY.parquet")
    print("=" * 60)

    silver_dir = DATA_DIR / "silver" / "lineups"
    if not silver_dir.exists():
        print("   ⚠️  Silver lineups directory not found, skipping")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    source_files = sorted(silver_dir.glob("year=*/month=*/day=*/lineups.parquet"))
    # Hive top-level folders (year=YYYY) to list for deletion
    source_folders = sorted(set(pf.parent.parent.parent for pf in source_files))

    if not source_files:
        print("   No silver Hive partitions to consolidate (already done?)")
        return {"rows_before": 0, "rows_after": 0, "folders_to_delete": []}

    print(f"   Found {len(source_files)} partition files")

    con = duckdb.connect()
    union_sql = " UNION ALL BY NAME ".join(
        f"SELECT * FROM read_parquet('{pf}')" for pf in source_files
    )
    rows_before = con.execute(f"SELECT COUNT(*) FROM ({union_sql})").fetchone()[0]
    print(f"   Total rows across partition files: {rows_before}")

    years = [row[0] for row in con.execute(
        f"SELECT DISTINCT year(game_date) AS yr FROM ({union_sql}) ORDER BY yr"
    ).fetchall()]

    rows_after = 0
    for year in years:
        target_file = silver_dir / f"lineups_{year}.parquet"
        year_sql = f"SELECT * FROM ({union_sql}) WHERE year(game_date) = {year}"

        if target_file.exists():
            year_sql = f"""
                SELECT * FROM read_parquet('{target_file}', union_by_name=true)
                UNION ALL BY NAME ({year_sql})
            """

        # Silver = deduped: keep latest scrape per game slot
        final_sql = f"""
            SELECT * FROM ({year_sql})
            QUALIFY row_number() OVER (
                PARTITION BY game_date, away_team, home_team, fetch_timestamp
                ORDER BY fetch_timestamp DESC NULLS LAST
            ) = 1
        """

        n = con.execute(f"SELECT COUNT(*) FROM ({final_sql})").fetchone()[0]
        rows_after += n

        if dry_run:
            print(f"   [DRY RUN] Would write {n} rows → {target_file.relative_to(ROOT)}")
        else:
            con.execute(f"COPY ({final_sql}) TO '{target_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            actual = count_rows(con, str(target_file))
            print(f"   ✅ Wrote {actual} rows → {target_file.relative_to(ROOT)}")

    con.close()
    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "folders_to_delete": [str(f) for f in source_folders],
    }


# ============================================================
# MOVE SCHEDULES
# ============================================================

def move_schedules(dry_run: bool) -> dict:
    """Copy data/raw/schedules/ → data/schedules/ (list old for manual deletion)."""
    print("\n" + "=" * 60)
    print("TASK: Move schedules → data/schedules/")
    print("=" * 60)

    source = DATA_DIR / "raw" / "schedules"
    target = DATA_DIR / "schedules"

    if not source.exists():
        print("   ⚠️  Source not found, skipping")
        return {"folders_to_delete": []}

    files = sorted(source.glob("*.parquet"))
    print(f"   Found {len(files)} schedule files")

    if target.exists():
        copied = 0
        for f in files:
            dest = target / f.name
            if not dest.exists():
                if dry_run:
                    print(f"   [DRY RUN] Would copy {f.name}")
                else:
                    shutil.copy2(f, dest)
                    print(f"   ✅ Copied {f.name}")
                copied += 1
        if copied == 0:
            print("   All files already present in target")
    else:
        if dry_run:
            print(f"   [DRY RUN] Would copy {source.relative_to(ROOT)} → {target.relative_to(ROOT)} ({len(files)} files)")
        else:
            shutil.copytree(str(source), str(target))
            print(f"   ✅ Copied {source.relative_to(ROOT)} → {target.relative_to(ROOT)} ({len(files)} files)")

    return {"folders_to_delete": [str(source)]}


# ============================================================
# MOVE PLAYER LOGS + RENAME SEASON_TOTALS
# ============================================================

def move_player_logs(dry_run: bool) -> dict:
    """
    Copy data/raw/player_logs/ → data/player_logs/
    Then rename season_totals/ → fangraphs_leaderboards/ inside the target.
    """
    print("\n" + "=" * 60)
    print("TASK: Move player_logs → data/player_logs/ + rename season_totals")
    print("=" * 60)

    source = DATA_DIR / "raw" / "player_logs"
    target = DATA_DIR / "player_logs"

    if not source.exists():
        print("   ⚠️  Source not found, skipping")
        return {"folders_to_delete": []}

    total_files = sum(1 for _ in source.glob("**/*.parquet"))
    print(f"   Found {total_files} parquet files in source")

    if target.exists():
        copied = 0
        for subdir in sorted(source.iterdir()):
            if not subdir.is_dir():
                continue
            target_sub = target / subdir.name
            target_sub.mkdir(parents=True, exist_ok=True)
            for f in sorted(subdir.glob("*.parquet")):
                dest = target_sub / f.name
                if not dest.exists():
                    if dry_run:
                        print(f"   [DRY RUN] Would copy {subdir.name}/{f.name}")
                    else:
                        shutil.copy2(f, dest)
                    copied += 1
        if copied == 0:
            print("   All files already present in target")
        elif not dry_run:
            print(f"   ✅ Copied {copied} missing files into {target.relative_to(ROOT)}")
        else:
            print(f"   [DRY RUN] Would copy {copied} file(s)")
    else:
        if dry_run:
            print(f"   [DRY RUN] Would copy {source.relative_to(ROOT)} → {target.relative_to(ROOT)}")
        else:
            shutil.copytree(str(source), str(target))
            print(f"   ✅ Copied {source.relative_to(ROOT)} → {target.relative_to(ROOT)}")

    # Rename season_totals → fangraphs_leaderboards inside target
    old_name = target / "season_totals"
    new_name = target / "fangraphs_leaderboards"

    if old_name.exists() and not new_name.exists():
        if dry_run:
            n = sum(1 for _ in old_name.glob("*.parquet"))
            print(f"   [DRY RUN] Would rename season_totals/ → fangraphs_leaderboards/ ({n} files inside)")
        else:
            old_name.rename(new_name)
            print(f"   ✅ Renamed season_totals/ → fangraphs_leaderboards/")
    elif new_name.exists():
        print(f"   fangraphs_leaderboards/ already exists in target")
    else:
        print(f"   ⚠️  season_totals/ not found in target (may not have copied yet)")

    return {"folders_to_delete": [str(source)]}


# ============================================================
# CONVERT LINEAR WEIGHTS CSV → PARQUET
# ============================================================

def convert_linear_weights(dry_run: bool) -> dict:
    """
    Convert data/reference/linear_weights.csv → data/reference/linear_weights.parquet
    Adds a 'date' column (today) so future in-season recalculations can be appended.
    """
    print("\n" + "=" * 60)
    print("TASK: Convert linear_weights.csv → .parquet")
    print("=" * 60)

    csv_path  = DATA_DIR / "reference" / "linear_weights.csv"
    parquet_path = DATA_DIR / "reference" / "linear_weights.parquet"

    if not csv_path.exists():
        print("   ⚠️  CSV not found, skipping")
        return {"files_to_delete": []}

    if parquet_path.exists():
        print(f"   Parquet already exists, skipping (idempotent)")
        return {"files_to_delete": [str(csv_path)]}

    today = date.today().isoformat()
    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}')").fetchone()[0]

    if dry_run:
        print(f"   [DRY RUN] Would convert {n} rows → {parquet_path.relative_to(ROOT)}")
        print(f"   [DRY RUN] Would add 'date' column = '{today}'")
    else:
        con.execute(f"""
            COPY (
                SELECT *, DATE '{today}' AS date
                FROM read_csv_auto('{csv_path}')
            ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        actual = count_rows(con, str(parquet_path))
        print(f"   ✅ Wrote {actual} rows → {parquet_path.relative_to(ROOT)}")
        print(f"   Added 'date' column = '{today}'")

    con.close()
    return {"files_to_delete": [str(csv_path)]}


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Reorganize MLB betting data storage")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    dry_run = args.dry_run

    if dry_run:
        print("🔍 DRY RUN MODE — no files will be created or moved\n")
    else:
        print("🚀 EXECUTING data reorganization\n")

    results = {}
    results["odds"]           = consolidate_odds(dry_run)
    results["bronze_lineups"] = consolidate_bronze_lineups(dry_run)
    results["silver_lineups"] = consolidate_silver_lineups(dry_run)
    results["schedules"]      = move_schedules(dry_run)
    results["player_logs"]    = move_player_logs(dry_run)
    results["linear_weights"] = convert_linear_weights(dry_run)

    # === SUMMARY ===
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for key in ["odds", "bronze_lineups", "silver_lineups"]:
        r = results[key]
        before = r.get("rows_before", 0)
        after  = r.get("rows_after", 0)
        if before > 0:
            ok = "✅" if after >= before else "⚠️  ROW COUNT MISMATCH"
            print(f"   {ok} {key}: {before} rows in → {after} rows out")

    print("\n📋 FILES/FOLDERS SAFE TO DELETE MANUALLY (after verifying above):")
    deletable = []

    for key in ["odds", "bronze_lineups", "silver_lineups", "schedules", "player_logs"]:
        deletable.extend(results[key].get("folders_to_delete", []))

    deletable.extend(results.get("linear_weights", {}).get("files_to_delete", []))

    stale_db = DATA_DIR / "mlb_betting.db"
    if stale_db.exists():
        deletable.append(f"{stale_db}  # stale orphan DB")

    for item in sorted(set(deletable)):
        print(f"   rm -rf {item}")

    if not deletable:
        print("   (nothing to delete)")

    print()


if __name__ == "__main__":
    main()
