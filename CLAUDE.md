# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Advanced MLB betting engine targeting +EV edges on FanDuel/DraftKings/BetMGM and prediction markets (Kalshi/Polymarket). Core philosophy: hyper-specific per-game projections (lineup × probable starter × platoon × park/weather × bullpen) → Monte Carlo RS/RA → Pythagorean win% → compare to live odds.

## Environment Setup

```bash
# Activate the virtual environment (from project root)
source venv314/bin/activate

# Install dependencies
pip install -r requirements.txt
```

The venv is named `venv314` (Python 3.13/3.14). All scripts use `sys.path.insert(0, ...)` to make `config` importable regardless of working directory.

## Running Scripts

Each module can be run directly as a script:

```bash
python src/features/player_fingerprinting.py        # KNN comps test run
python src/data_ingestion/player_id_matching.py     # Build/test player ID map
python src/season_total_logs_fetcher.py             # Fetch game-by-game logs from pybaseball
```

There are no automated tests or linter configured yet.

## Architecture

### Data Flow

```
External APIs / pybaseball
        ↓
  data/raw/         ← Bronze: timestamped Parquet snapshots (never overwrite)
  data/bronze/      ← (planned data-lake tier)
  data/silver/      ← cleaned/joined
  data/gold/        ← model-ready features
  data/db/mlb_betting.duckdb  ← queryable via DuckDB
```

### Key Data Paths (defined in `config/settings.py`)

- `data/raw/odds/YYYY-MM-DD/games/` and `futures/` — odds snapshots (The Odds API)
- `data/raw/schedules/games_YYYY.parquet` — schedule/game records by year
- `data/raw/player_logs/season_totals/` — batting/pitching game logs from pybaseball
- `data/raw/stats/comps_cache.parquet` — cached KNN historical database (2015–2025 by default)
- `data/reference/player_id_map.parquet` — master player ID crosswalk (SFBB + pybaseball)
- `data/reference/linear_weights.csv` — season-specific linear weights for run value calcs

### Configuration (`config/settings.py`)

Central hub for all paths, API keys (loaded from `.env` via `python-dotenv`), scraping delays, and matching thresholds. Also loads `config/data_schemas.yaml` at import time. Always import settings rather than hardcoding paths.

### Core Modules

**`src/features/player_fingerprinting.py` — `PlayerComps` class**
- KNN-based historical comp system (sklearn `NearestNeighbors`, Euclidean distance, `n_neighbors=15`)
- Features: age, service_time, PA/IP, wRC+, FIP, ISO, K%, BB%, BABIP, HardHit%, Barrel%, Spd
- `build_historical_data()` pulls FanGraphs via pybaseball and saves to `comps_cache.parquet`
- `fit(is_batter=True/False)` trains separate KNN models for batters vs. pitchers
- `predict_adjusted_projection()` blends comp delta + rolling in-season stats; applies October fade (×0.96) and rookie wall (×1.07) flags
- All DataFrame columns are normalized to lowercase via `_normalize_columns()` — critical for consistent column access

**`src/data_ingestion/player_id_matching.py` — `PlayerMatcher` class**
- Resolves player names to cross-system IDs (bbref, MLBAM, FanGraphs, Rotowire)
- Three-tier matching: (1) exact SFBB lookup from GitHub mirror, (2) pybaseball fuzzy lookup, (3) rapidfuzz token sort + team/position context boost
- Handles duplicate names by grouping SFBB records into lists
- Caches master map to `data/reference/player_id_map.parquet`; pass `force_refresh=True` to rebuild

**`src/season_total_logs_fetcher.py`**
- Fetches per-game batting/pitching logs and platoon splits (vs_LHP, vs_RHP) from pybaseball
- Saves to `data/raw/player_logs/season_totals/` and `season_splits/`
- Rate-limits at 1.3s per year; backs off 5s on errors

### Projection Methodology (in-progress implementation)

Pre-season: 100% KNN comp model (historical peers at same age/service time → weighted YoY delta applied to season arc)

In-season blend:
```
final = w_preseason * preseason_model + w_inseason * inseason_comps + w_kalman * kalman_estimate
```
Weights shift toward in-season data as games played increases (Kalman filter target: not yet implemented).

Game-day: team RS/RA from lineup × batter projections vs. starter/bullpen → Pythagorean `RS^1.83 / (RS^1.83 + RA^1.83)` → compare to market implied probability.

## Planned Components (not yet built)

- Monte Carlo RS/RA simulator (10k+ games)
- APScheduler for multi-time-per-day odds + lineup polling
- Kalman filter inside `PlayerComps` for in-season updating
- Daily lineup scraper (Rotowire)
- Edge calculator + limit-tracking meta layer

## Security Note

The `.env` file currently contains live API keys and is tracked by git. The Anthropic API key and The Odds API key in `.env` should be rotated and `.env` should be added to `.gitignore`.
