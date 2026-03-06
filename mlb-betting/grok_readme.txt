================================================================================
GROK README - MLB Betting Algorithm System
Living Project Log
Last updated: March 03, 2026 10:35 AM EST
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

Current Status (March 03, 2026)
-------------------------------
Data Pipeline: COMPLETE & PRODUCTION-READY
- Odds fetcher (timestamped, multi-book)
- Lineup scraper (structured, lists + per-book odds)
- Player fingerprinting (KNN + dynamic updates)
- Full historical schedules (2000–2026)
- Linear weights reference

Next Immediate Priority: Monte Carlo RS/RA Simulator (being written now)

Data Structures – Current Schemas
---------------------------------
All Parquet files use these exact column names and types:

1. Lineups (data/raw/lineups/YYYY-MM-DD/lineups_HHMMSS.parquet)
   - fetch_timestamp: datetime64[ns, UTC]
   - game_time: string
   - away_team / home_team: string
   - away_starter_name / home_starter_name: string
   - away_starter_hand / home_starter_hand: string
   - away_lineup / home_lineup: list[string]          # ordered 9 players
   - away_lineup_pos / home_lineup_pos: list[string]
   - away_lineup_bats / home_lineup_bats: list[string]
   - is_confirmed: boolean
   - weather: string
   - umpire: string (None if not announced)
   - odds_line: dict (keys: composite,fanduel,draftkings,betmgm,pointsbet → float or None)
   - odds_ou: dict (same keys → float or None)

2. Odds (data/raw/odds/YYYY-MM-DD/...)
   - Full per-book snapshots (moneyline, spreads, totals, outrights)

3. Schedules (data/raw/schedules/YYYY/games_YYYY.parquet)
   - game_id, game_date, year, away_team, home_team, away_score, home_score, status, etc.

4. Linear Weights (data/reference/linear_weights.csv)
   - Season (index), wOBA, wOBAScale, wBB, wHBP, w1B, w2B, w3B, wHR, runSB, runCS, R/PA, R/W, cFIP

Next Module: Monte Carlo Simulator (written below)

================================================================================