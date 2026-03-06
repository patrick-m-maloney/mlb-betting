================================================================================
GROK README - MLB Betting Algorithm System
Living Project Log
Last updated: March 06, 2026 01:30 PM EST
================================================================================

Project Goal
------------
Advanced, hyper-specific MLB betting system focused on daily games (ML, RL, totals) 
with secondary futures on traditional books + Kalshi/Polymarket. 
Core idea: Build extremely accurate RS/RA projections per game using real lineups, 
batter-pitcher matchups, platoon splits, park/weather, and dynamic player 
"fingerprinting" (KNN historical comps + in-season Kalman-style updating), then 
compare to market odds for +EV bets. Also track bookmaker limits and use offset 
betting strategy.

Timeline & Thought Process (chronological)
------------------------------------------
Feb 19, 2026 – Initial system outline created. Decided on modular structure, 
              pybaseball + MLB-StatsAPI, Parquet storage, The Odds API as primary.

Feb 20–22, 2026 – Odds ingestion layer built and stabilized (The Odds API). 
              Handled preseason/futures, timestamped snapshots for line movement analysis.

Feb 23, 2026 – Player Fingerprinting module completed after several iterations 
              (KNN comps on age/service-time + Statcast + traditional metrics, 
              weighted YoY deltas, in-season rolling blend, October fade + rookie flags).

Feb 24–March 02, 2026 – Daily lineup scraper built for live Rotowire. 
              Multiple refinements to handle current page structure. 
              Now extracts: game time, teams, starters+hand, ordered lists for 
              lineup/pos/bats, confirmed status (boolean), weather, umpire, 
              and full per-book odds (LINE + O/U as dicts).

March 03, 2026 – Schedule fetcher completed (2000–2026, month-by-month for old years). 
              Linear weights table finalized (1871–2025, your Pelota + FanGraphs).

March 04, 2026 – Monte Carlo Simulator v1.0 completed (aggregate team-level RS/RA 
              using lineup lists + linear weights + placeholder projections). 
              Wired for future fingerprinting calls.

March 06, 2026 – Player game-by-game logs + platoon splits fetcher added.
              Data foundation strengthened for fine-grained modeling.

Current Status (March 06, 2026)
-------------------------------
Data Pipeline: COMPLETE & PRODUCTION-READY
- Odds fetcher (timestamped, multi-book)
- Lineup scraper (structured, lists + per-book odds)
- Player fingerprinting (KNN + dynamic updates)
- Full historical schedules (2000–2026)
- Linear weights reference (1871–2025)
- Game-by-game player logs + platoon splits (2015+ full; earlier seasons season-level splits)

Next Immediate Priority: 
- Upgrade Monte Carlo to full batter-vs-pitcher event simulation
- Add park factors, bullpen usage, and in-season run environment weighting

Data Structures – Detailed Storage & Schemas
--------------------------------------------
All files are Parquet unless noted. Granularity, fields, and types below.

1. Lineups (data/raw/lineups/YYYY-MM-DD/lineups_HHMMSS.parquet)
   - Granularity: Per scrape (multiple per day possible)
   - One row per game
   - Columns & types:
     - fetch_timestamp: datetime64[ns, UTC]
     - game_time: string (e.g. "1:05 PM ET")
     - away_team / home_team: string (3-letter codes)
     - away_starter_name / home_starter_name: string
     - away_starter_hand / home_starter_hand: string (L/R)
     - away_lineup / home_lineup: list[string] (ordered 9 players)
     - away_lineup_pos / home_lineup_pos: list[string] (ordered positions)
     - away_lineup_bats / home_lineup_bats: list[string] (ordered bats: L/R/S)
     - is_confirmed: boolean
     - weather: string
     - umpire: string (None if not announced)
     - odds_line: dict (keys: composite, fanduel, draftkings, betmgm, pointsbet → float or None)
     - odds_ou: dict (same keys → float or None)

2. Odds (data/raw/odds/YYYY-MM-DD/games/... or futures/...)
   - Granularity: Per scrape (multiple per day)
   - One row per outcome/book/market
   - Columns: fetch_timestamp, game_id, home_team, away_team, bookmaker, market, outcome_name, odds, point, last_update, etc.

3. Schedules (data/raw/schedules/YYYY/games_YYYY.parquet)
   - Granularity: Per game
   - Columns & types:
     - game_id: int64
     - game_date: datetime64[ns]
     - year: int64
     - away_team / home_team: string
     - away_score / home_score: Int64 (nullable integer)
     - status: string
     - doubleheader: bool
     - game_type: string (R=regular, P=post, S=spring)

4. Linear Weights (data/reference/linear_weights.csv)
   - Granularity: Per season
   - Columns: Season (index), wOBA, wOBAScale, wBB, wHBP, w1B, w2B, w3B, wHR, runSB, runCS, R/PA, R/W, cFIP
   - All numeric (float64)

5. Player Game Logs (data/raw/player_logs/YYYY/batting_logs.parquet & pitching_logs.parquet)
   - Granularity: Per player per game (2015+ full; earlier years season-level splits)
   - Columns: player_id, game_date, year, G, AB, R, H, 2B, 3B, HR, RBI, BB, SO, AVG, OBP, SLG, wOBA, etc. (full FanGraphs columns)

6. Platoon Splits (data/raw/player_logs/YYYY/batting_splits_vs_LHP.parquet etc.)
   - Granularity: Per player per season per split (vs LHP/RHP)
   - Columns: player_id, year, split, PA, wOBA, ISO, K%, BB%, etc.

================================================================================