# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

MLB betting engine that projects per-game RS/RA using player-specific data (KNN historical comps + lineup-specific wOBA), runs Monte Carlo simulations, and compares to live sportsbook odds for +EV identification. Data flows: external APIs → timestamped Parquet snapshots → DuckDB query layer → Monte Carlo simulation → edge detection.

## Rules You Must Always Follow

- **NEVER delete any files under any circumstances.** If you think a file should be deleted, list it in your response and let the user decide.
- **Always explain what you are about to do before doing it** and wait for confirmation on any significant changes (new modules, architecture changes, data pipeline modifications).
- **When adding new features, follow existing code style and conventions** in the repo. Match the patterns in neighboring files.
- **Prefer modifying existing files over creating new ones** unless a new module is clearly warranted.
- **Never overwrite data files or parquet/duckdb files** without explicit confirmation. These contain historical data that cannot be regenerated.
- **Never commit `.env` or files containing API keys.** The `.gitignore` already excludes `.env` but be vigilant.

## Environment Setup

```bash
source venv314/bin/activate    # Python 3.13/3.14
pip install -r requirements.txt
```

Key dependencies: duckdb, pybaseball, scikit-learn, rapidfuzz, beautifulsoup4, apscheduler, MLB-StatsAPI, pyarrow, requests.

**⚠️ pandas is broken in venv314 (Python 3.13).** All new data pipeline code must use DuckDB + Python stdlib only. `lineups.py` still uses pandas for HTML parsing (BeautifulSoup → DataFrame) but its `save_lineups()` uses DuckDB for parquet I/O. Do not add pandas dependencies to any new or updated files.

All modules use `sys.path.insert(0, ...)` to make `config` importable regardless of working directory. This is the established pattern — follow it in new files.

## Running Scripts

```bash
# Automated scheduler (lineups + all odds sources every 30 min)
caffeinate -s python -m src.scripts.mlb_scraper_bot

# Manual scrape (all sources → DuckDB)
python src/scripts/manual_scrape_odds.py

# Monte Carlo simulation on latest lineups
python src/models/monte_carlo.py

# Individual fetchers (each runnable standalone)
python src/data_ingestion/lineups.py
python src/data_ingestion/odds/the_odds_api_fetcher.py
python src/data_ingestion/player_logs_fetcher.py
python src/data_ingestion/schedule_fetcher.py
python src/features/player_fingerprinting.py
```

No test framework is configured yet.

## Architecture

### Source Code Layout

```
src/
├── data_ingestion/
│   ├── lineups.py              # Rotowire scraper → bronze + silver Parquet + DuckDB
│   ├── player_id_matching.py   # PlayerMatcher: 3-tier name→ID resolution
│   ├── player_logs_fetcher.py  # Daily BR game logs with checkpoint/resume
│   ├── schedule_fetcher.py     # MLB-StatsAPI schedules by year
│   └── odds/
│       ├── the_odds_api_fetcher.py  # Primary sportsbook odds
│       ├── kalshi_fetcher.py        # Kalshi prediction markets
│       ├── polymarket_fetcher.py    # Polymarket prediction markets
│       └── rundown_fetcher.py       # TheRundown (dormant, no API key)
├── features/
│   └── player_fingerprinting.py    # PlayerComps: KNN historical comps
├── models/
│   └── monte_carlo.py              # 10k-sim game simulator
├── database/
│   └── db_manager.py               # DuckDB append_to_table helper
├── scripts/
│   ├── mlb_scraper_bot.py          # APScheduler automated pipeline
│   └── manual_scrape_odds.py       # One-off manual scrape
└── season_total_logs_fetcher.py    # FanGraphs leaderboard fetcher (misplaced, should be in data_ingestion/)
```

### Configuration

`config/settings.py` — Central hub for all paths, API keys (from `.env` via python-dotenv), scraping delays, matching thresholds. Also loads `config/data_schemas.yaml`. Always import from here rather than hardcoding paths.

`config/data_schemas.yaml` — Schema definitions for all DuckDB tables and silver layer views.

### Data Pipeline

**Storage pattern:** Append-only per-year Parquet files (medallion architecture). Never overwrite — always union new rows with existing file.

```
External APIs → data/bronze/ (raw, append-only per-year Parquet)
                    → data/silver/ (deduped per-year Parquet)
                        → data/gold/ (model-ready, planned)
All layers queried via DuckDB views registered in db_manager.py.
```

**Key data paths** (constants defined in `config/settings.py`):
- `data/bronze/odds/SOURCE_YYYY.parquet` — one file per source per year (the_odds_api, kalshi, polymarket, rundown)
- `data/bronze/lineups/lineups_YYYY.parquet` — raw lineup scrapes, all snapshots appended
- `data/silver/lineups/lineups_YYYY.parquet` — deduped by (game_date, away_team, home_team), latest fetch_timestamp wins
- `data/schedules/games_YYYY.parquet` — one file per year (2000–2026)
- `data/player_logs/game_by_game/` — daily Baseball Reference batting/pitching logs
- `data/player_logs/fangraphs_leaderboards/` — FanGraphs season leaderboards
- `data/reference/linear_weights.parquet` — run values (1871–2025), player_id_map.parquet
- `data/simulations/` — Monte Carlo output files
- `data/db/mlb_betting.duckdb` — central DuckDB (views over Parquet + reference tables)

**DuckDB view layer** (`src/database/db_manager.py`): `get_connection()` calls `register_views()` on every open, which creates `CREATE OR REPLACE VIEW` over each Parquet glob pattern. Views silently skip missing data. Use `get_views()` to inspect row counts. All new code should use `get_connection()` rather than raw `duckdb.connect()`.

**All fetchers use DuckDB + stdlib only — no pandas.** The append pattern:
```python
json_str = json.dumps(rows)
new_rows_sql = f"SELECT * FROM read_json_auto($${json_str}$$)"
# if target exists: UNION ALL BY NAME with existing parquet, then COPY TO
```

### Key Classes

**`PlayerComps`** (`src/features/player_fingerprinting.py`): KNN-based historical comp system. `build_historical_data()` pulls from FanGraphs and caches. `fit(is_batter)` trains separate models. `predict_adjusted_projection()` blends comp delta + rolling stats with October fade and rookie wall adjustments. All columns normalized to lowercase via `_normalize_columns()`.

**`PlayerMatcher`** (`src/data_ingestion/player_id_matching.py`): Resolves names to cross-system IDs. Downloads SFBB master map from GitHub, caches to Parquet. Three-tier matching: exact SFBB → pybaseball fuzzy → rapidfuzz with context boost.

**`MonteCarloSimulator`** (`src/models/monte_carlo.py`): Loads all historical batting logs at init, builds mlbID→wOBA lookup. `simulate_game()` projects lineup runs via batting-order weighted wOBA, then runs 10k normal simulations. All magic numbers are named constants at the top of the file.

**`register_views()` / `get_connection()`** (`src/database/db_manager.py`): Registers 11 DuckDB views over all Parquet data directories on every connection. Prefer `get_connection()` over raw `duckdb.connect()` in all query code.

**`append_to_table()`** (`src/database/db_manager.py`): Schema-evolving DuckDB append for small reference tables. For odds/lineups, use direct Parquet file appends instead (views pick up new data automatically).

### Known Issues

- `src/scripts/mlb_scraper_bot.py` has broken imports — references `src.data_ingestion.the_odds_api_fetcher` but file was moved to `src.data_ingestion.odds.the_odds_api_fetcher`
- Missing `__init__.py` in: `src/data_ingestion/odds/`, `src/database/`, `src/models/`, `src/scripts/`
- `data/mlb_betting.db` at project root is a stale orphan — real DB is `data/db/mlb_betting.duckdb`
- `load_linear_weights()` in `player_fingerprinting.py` is defined inside the `PlayerComps` class without `self` (should be `@staticmethod` or extracted)
- wOBA calculation is duplicated in `monte_carlo.py`, `player_fingerprinting.py`, and `playground.ipynb`
