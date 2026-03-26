# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

MLB betting engine that projects per-game RS/RA using player-specific data (KNN historical comps + lineup-specific wOBA), runs Monte Carlo simulations, and compares to live sportsbook odds for +EV identification. Data flows: external APIs → timestamped Parquet snapshots → DuckDB query layer → Monte Carlo simulation → edge detection.

**Planned: Cross-market arbitrage strategy.** When odds across any combination of sources (sportsbooks via The Odds API, Kalshi, Polymarket) imply a guaranteed profit regardless of outcome — i.e. the sum of implied probabilities across all outcomes is less than 1.0 — flag it and execute the trade. This should be a dedicated module that runs on every scrape cycle, compares the same event across all sources, and alerts/acts when a true arb exists. The key challenge is normalizing different price formats (American odds, Kalshi yes/no cents, Polymarket probability) into a common implied probability so they can be compared directly.

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
│   ├── projections.py             # Player wOBA projection engine (historical avg, future: KNN + Kalman)
│   ├── monte_carlo.py             # 10k-sim game simulator (DuckDB-based)
│   └── edge_detection.py          # Model vs market odds comparison
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

**All fetchers use DuckDB + stdlib only — no pandas.** The append pattern writes rows to a temp JSON file then reads via `read_json_auto()` — do NOT use inline `$$...$$` dollar-quoting (DuckDB treats it as a file glob, not a string literal):
```python
tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
with os.fdopen(tmp_fd, "w") as f:
    json.dump(rows, f)
new_rows_sql = f"SELECT * FROM read_json_auto('{tmp_path}')"
# if target exists: UNION ALL BY NAME with existing parquet, then COPY TO
# always os.unlink(tmp_path) in a finally block
```

**Kalshi fetcher** (`src/data_ingestion/odds/kalshi_fetcher.py`):
- SDK: `kalshi_python_sync` (`KalshiClient` + `Configuration`)
- Production API: `https://api.elections.kalshi.com/trade-api/v2`
- **No auth required** for public market data reads (auth only needed for trading)
- Series fetched: `KXMLBSTGAME` (game winner markets, `status="open"`) and `KXMLBWINS` (season win totals, no status filter — `unopened` pre-season)
- Output: `data/bronze/odds/kalshi_YYYY.parquet`
- `market_type` column: `'game_winner'` or `'win_total'`
- Ticker parsing uses `winner_side` to resolve 2- vs 3-letter team codes (e.g. SD, SF, TB, AZ, KC are 2-letter; most others 3-letter)

### Key Classes

**`PlayerComps`** (`src/features/player_fingerprinting.py`): KNN-based historical comp system. **DuckDB + numpy only — no pandas.** `build_historical_data()` reads from local FanGraphs leaderboard parquets (2015–2025, PA >= 50) as primary source; falls back to pybaseball if local data has gaps. Caches to `data/raw/stats/comps_cache_*.parquet`. `fit(is_batter)` trains separate batter/pitcher models (sklearn NearestNeighbors + StandardScaler). `get_comps()` returns k=15 nearest historical comps + weighted-average target stat values. `predict_adjusted_projection()` blends comp delta + rolling stats with October fade and rookie wall adjustments. All feature columns use clean lowercase names (no `_normalize_columns()` — handled via SQL aliases in the DuckDB load).

**`PlayerMatcher`** (`src/data_ingestion/player_id_matching.py`): Resolves names to cross-system IDs. Downloads SFBB master map from GitHub, caches to Parquet. Three-tier matching: exact SFBB → pybaseball fuzzy → rapidfuzz with context boost.

**`ProjectionEngine`** (`src/models/projections.py`): Two-layer player wOBA projection. Layer 0 (career avg): PA-weighted wOBA from 2017–2025 game logs. Layer 1 (KNN): `PlayerComps` finds 15 nearest historical FG comps → weighted comp-avg wRC+ converted to wOBA, blended 65%/35% with player's actual FG wOBA. Crosswalk: bbref_id → IDFANGRAPHS (via SFBB player_id_map) → FG season stats. Fallback priority: L1 → L0 → league avg (0.316). Pre-computes projections for all ~1,250 players with FG data at init.

**`MonteCarloSimulator`** (`src/models/monte_carlo.py`): Loads `ProjectionEngine` at init. `simulate_game()` projects lineup runs via batting-order weighted wOBA, then runs 10k normal simulations. All magic numbers are named constants at the top of the file.

**`register_views()` / `get_connection()`** (`src/database/db_manager.py`): Registers 11 DuckDB views over all Parquet data directories on every connection. Prefer `get_connection()` over raw `duckdb.connect()` in all query code.

**`append_to_table()`** (`src/database/db_manager.py`): Schema-evolving DuckDB append for small reference tables. For odds/lineups, use direct Parquet file appends instead (views pick up new data automatically).

### Projection Pipeline

The full model flow from confirmed lineup to edge detection:

**Step 1 — KNN Season Projection ✅ IMPLEMENTED**
- Match player's entering FanGraphs profile (age, PA, wRC+, ISO, K%, BB%, BABIP, HardHit%, Barrel%, Spd) to 15 nearest historical comps (2015–2025 FG leaderboards)
- Weighted-average comp wRC+ converted to wOBA; blended 65%/35% with player's actual FG wOBA
- Implemented in `ProjectionEngine._init_knn_comps()` + `PlayerComps.get_comps()`
- **Baseline vs KNN comparison (2026-03-17, 24 games):** KNN produced 50% wider win-probability spreads (10.2 pp range → 15.2 pp), more conviction on good/bad lineups, 22 edges vs 15 edges at >3% threshold

**Step 2 — Kalman-style Blending (planned)**
- Use KNN projection as the prior; blend with actual cumulative season stats
- Weight formula: early season → weight toward KNN prior; late season → weight toward actuals (confidence grows with PA)
- Mean-reversion component: regress player rolling average toward comp-based projection, tunable via backtesting

**Step 3 — Monte Carlo ✅ IMPLEMENTED**
- Feed blended player projections into confirmed lineup
- 10k game simulations → run distribution → win probability

**Step 4 — Edge Detection ✅ IMPLEMENTED**
- Compare model win probability to market implied probability (The Odds API, Kalshi, Polymarket)
- Flags +EV opportunities above a configurable threshold (default 3%)

Backtesting will calibrate the blending weights and inform when to transition between early/late season models.

**Planned: Implied Win Probability Tracker**
- For each game, convert all market lines (sportsbooks, Kalshi, Polymarket, model) to implied win % using standard vig removal
- American odds → implied prob: positive: 100/(odds+100), negative: |odds|/(|odds|+100)
- Track how each source's implied probability shifts throughout the day as lines move
- Visualize as a time-series graph: x = time, y = implied win %, one line per source + model
- Useful for spotting when the market moves toward or away from the model's view

### Railway Deployment

Repo is configured for Railway deployment as a background worker (no web server):
- `Procfile`: `worker: python src/scripts/mlb_scraper_bot.py`
- `railway.toml`: Python 3.12, nixpacks builder, restart on failure
- Set all API keys in Railway dashboard as environment variables (see `.env.example`)
- `config/settings.py` creates all `data/` subdirectories at import time — no pre-created dirs needed on the server
- **Data persistence**: Railway volumes must be mounted at `/data` if you want parquet files to survive deploys. Without a volume, data resets on each deploy. Alternatively, point the scraper at a remote storage backend (S3/GCS) for the parquet files.

### Known Issues

- `data/mlb_betting.db` at project root is a stale orphan — real DB is `data/db/mlb_betting.duckdb`
- wOBA calculation is duplicated in `monte_carlo.py`, `player_fingerprinting.py`, and `playground.ipynb`

### KNN Layer 1 — Tuning Backlog

- **Extreme-performer overshoot**: Ohtani-tier players (.418 FG wOBA) get projected too high (~.464) because their comp pool is other elite hitters whose comp average exceeds their actual. The 35% comp blend weight should be tuned down for outlier performers, or the comp pool should be capped at a percentile.
- **Comp blend weight should be PA-dependent (Layer 2)**: Current weight of 0.35 is hardcoded. Should scale with PA: high PA → trust actual more, low PA → weight toward comps. This is the foundation of the Kalman blend.
- **Add mean reversion to rolling average**: The blending formula should include a mean-reversion component that pulls player rolling averages toward their comp-based projection. Weights tunable via backtesting.
- **julgery01 ID crosswalk miss**: `julgery01` (and likely other players) fail to match bbref_id → IDfg and fall back to L0 career avg. SFBB map coverage should be investigated — may need supplemental matching by name for players not in the SFBB map.
- **Player fingerprinting cache is cold on Railway**: `data/raw/stats/comps_cache_*.parquet` won't exist on a fresh Railway deploy. Either commit the cache files (small, ~few MB) or have the bot run `PlayerComps.build_historical_data()` on first startup if cache is missing. Currently `fit()` auto-builds if cache is absent, so this is handled — but takes time on first run.
