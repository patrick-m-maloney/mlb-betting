# MLB Betting Algorithm System

Advanced matchup-specific MLB betting engine — daily games (ML, RL, totals) + futures with +EV edge across FanDuel/DraftKings/BetMGM + Kalshi/Polymarket.

**Core philosophy:** Hyper-specific per-game projections (lineup x probable starter x platoon x park/weather x bullpen) → Monte Carlo RS/RA → Pythagorean win% → compare to live odds for +EV identification.

## Current State (March 2026)

### Working

- **Odds ingestion** — The Odds API (h2h, spreads, totals, futures), Kalshi, Polymarket public markets. Appended to per-source per-year bronze Parquet (`data/bronze/odds/SOURCE_YYYY.parquet`). DuckDB views registered automatically.
- **Lineup scraper** — Rotowire daily lineups (live + local HTML for backtesting). Extracts game time, teams, starters + handedness, full 9-man batting orders with positions and bats, confirmed status, weather, umpire, per-book odds (LINE + O/U). Saves to per-year bronze + silver Parquet.
- **Player ID matching** — 3-tier resolution: (1) exact SFBB lookup from GitHub mirror, (2) pybaseball fuzzy, (3) rapidfuzz token sort + team/position context boost. Handles duplicate names. Cached to `data/reference/player_id_map.parquet`.
- **Player fingerprinting (KNN comps)** — `PlayerComps` class using sklearn NearestNeighbors on age, service time, wRC+, FIP, ISO, K%, BB%, BABIP, HardHit%, Barrel%, Spd. Weighted YoY delta from top 15 comps, in-season rolling blend, October fade (x0.96), rookie wall adjustment (x1.07).
- **Monte Carlo simulator** — 10k-sim engine. Loads latest lineup scrape, maps players to historical wOBA via BBRef→mlbID crosswalk with fuzzy name fallback, batting-order weighted PA projection, home/away park multipliers. Outputs RS/RA, total, spread, win probability per game.
- **Scheduler bot** — APScheduler-based, polls every 30 min (10 min near game time). Runs lineups + all odds sources in a single scrape cycle.
- **Historical data** — Schedules 2000–2026 (MLB-StatsAPI), FanGraphs leaderboards 1980–2025, daily Baseball Reference game logs 2017–2025, Lahman database (1871–2025), linear weights (1871–2025).

### Not yet built

- Kalman filter for in-season player updating (true talent as hidden state, each game as observation)
- Platoon split integration (vs LHP/RHP) in Monte Carlo projections
- Park factors and weather adjustments in simulation
- Bullpen usage modeling (starter pitch count → bullpen handoff)
- Edge calculator + limit-tracking meta layer
- Batter-vs-pitcher Markov chain simulation (evolution from aggregate RS/RA)
- Backtesting framework

## What's Next (prioritized)

1. **Fix scraper bot imports** — broken since odds fetchers moved to `src/data_ingestion/odds/`
2. **Platoon splits in Monte Carlo** — use batter hand vs starter hand for wOBA lookup
3. **Park factors + weather** — integrate into run projection multipliers
4. **Kalman filter** — add to `PlayerComps` for in-season true-talent updating
5. **Edge calculator** — compare model probability to market implied probability, track +EV opportunities
6. **Full batter-vs-pitcher simulation** — evolve from aggregate wOBA to Statcast outcome distributions

## Tech Stack

- **Python 3.13/3.14** (venv at `venv314/`)
- **Data**: pandas, pyarrow, Parquet (append-only timestamped snapshots), DuckDB (query layer)
- **Stats**: pybaseball, MLB-StatsAPI, scikit-learn (KNN), numpy
- **Scraping**: requests, BeautifulSoup4, lxml
- **Matching**: rapidfuzz (fuzzy string matching)
- **Scheduling**: APScheduler
- **Reference**: Lahman database, FanGraphs linear weights, SFBB Player ID Map

## Data Architecture

Medallion architecture — append-only per-year Parquet files, queried via DuckDB views.

```
data/
├── bronze/
│   ├── odds/SOURCE_YYYY.parquet        # Per-source per-year (the_odds_api, kalshi, polymarket, rundown)
│   └── lineups/lineups_YYYY.parquet   # All raw lineup scrapes appended
├── silver/
│   └── lineups/lineups_YYYY.parquet   # Deduped by (game_date, away_team, home_team)
├── schedules/games_YYYY.parquet       # One file per year (2000–2026)
├── player_logs/
│   ├── game_by_game/                  # Daily BR batting/pitching logs per year
│   └── fangraphs_leaderboards/        # FanGraphs season leaderboards per year
├── reference/
│   ├── linear_weights.parquet         # Run values (1871–2025)
│   └── player_id_map.parquet          # Cross-system player ID mapping
├── simulations/                        # Monte Carlo output files
├── db/mlb_betting.duckdb              # Central DuckDB (views over all Parquet)
└── gold/                              # (planned) model-ready features
```

All directories are registered as DuckDB views in `src/database/db_manager.py`. Use `get_connection()` to get a connection with all views pre-registered.

## Data Sources

| Source | What | Library/API |
|--------|------|-------------|
| FanGraphs | Player stats (wRC+, FIP, xwOBA, Barrel%, etc.) — already park/league normalized | pybaseball |
| Baseball Reference | Daily game-by-game batting/pitching logs | pybaseball (`batting_stats_range`) |
| MLB-StatsAPI | Game schedules, scores, game types | `statsapi` |
| The Odds API | Sportsbook odds (h2h, spreads, totals, futures) | REST API |
| Kalshi | Prediction market contracts (futures, props) | Public REST API |
| Polymarket | Prediction market contracts | Public REST API |
| Rotowire | Daily lineups, starters, weather, umpires | HTML scraping |
| Lahman Database | Historical player/team reference data (1871–2025) | Parquet files |
| SFBB Player ID Map | Cross-system player ID mapping | GitHub CSV |

## Projection Methodology

**Pre-season (100% model):** Current player vector (age, service time, last-season stats + Statcast) → KNN fingerprinting against top 15 historical comps → weighted improvement modifier matrix → projected season arc.

**In-season blend (planned):**
```
final = w_preseason * preseason_model + w_inseason * inseason_comps + w_kalman * kalman_estimate
```
Weights shift toward in-season data as games accumulate.

**Game-day:** Team RS/RA from lineup wOBA x batting-order PA weights x park/home multipliers → 10k Monte Carlo simulations → win probability → compare to market implied probability for +EV edges.

**Key constants:** Pythagorean exponent 1.83, HFA baseline ~54.5%, league avg wOBA ~0.320, runs/game scale 5.15.

## Setup

```bash
source venv314/bin/activate
pip install -r requirements.txt
# Copy .env.example to .env and add your API keys
```

## Running

```bash
# Automated scraping (lineups + odds every 30 min)
caffeinate -s python -m src.scripts.mlb_scraper_bot

# Manual one-off scrape
python src/scripts/manual_scrape_odds.py

# Run Monte Carlo simulation on latest lineups
python src/models/monte_carlo.py

# Fetch historical data
python src/data_ingestion/schedule_fetcher.py
python src/data_ingestion/player_logs_fetcher.py
```
