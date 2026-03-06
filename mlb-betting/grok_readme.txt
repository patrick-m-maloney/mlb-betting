================================================================================
GROK README - MLB Betting Algorithm System
Living Project Log
GitHub Repo: https://github.com/patrick-m-maloney/mlb-betting
Last updated: March 06, 2026 02:15 PM EST
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

March 06, 2026 – Player game-by-game logs fetcher added (Statcast 2015+ + season fallback). 
              Folder reorg: season_totals/ for aggregates, game_by_game/ for per-game files 
              (flat, one file per year: batting_game_logs_YYYY.parquet, etc.).

Current Status (March 06, 2026)
-------------------------------
Data Pipeline: STRONG FOUNDATION – GAME-BY-GAME DATA FLOWING
- Odds fetcher (timestamped, multi-book)
- Lineup scraper (structured, lists + per-book odds)
- Player fingerprinting (KNN + dynamic updates)
- Full historical schedules (2000–2026)
- Linear weights reference (1871–2025)
- Game-by-game player logs (2015+ via Statcast + season totals fallback)
- Season aggregates safely separated in season_totals/

Next Immediate Priorities:
- Post-process game logs → compute platoon averages (vs LHP/RHP) per player/year
- Hook platoon splits into lineup scraper (batter bats vs starter hand)
- Upgrade Monte Carlo to use real projected wOBA/FIP per player + linear weights
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

5. Player Season Totals (data/raw/player_logs/season_totals/)
   - Granularity: Per player per season
   - Files: batting_season_totals_YYYY.parquet, pitching_season_totals_YYYY.parquet
   - Columns: player_id, year, G, AB, R, H, 2B, 3B, HR, RBI, BB, SO, AVG, OBP, SLG, wOBA, etc. (full FanGraphs columns)

6. Player Game-by-Game Logs (data/raw/player_logs/game_by_game/)
   - Granularity: Per player per game (2015+ via Statcast; earlier years season-level fallback)
   - Files (one per year): 
     - batting_game_logs_YYYY.parquet
     - pitching_game_logs_YYYY.parquet
     - statcast_pitch_level_YYYY.parquet (2015+ only, pitch-level detail)
     - statcast_game_summary_YYYY.parquet (aggregated per player/game)
   - Columns (varies by file): player_id, game_date, year, AB, H, HR, RBI, BB, SO, AVG, OBP, SLG, wOBA, events, hit_distance_sc, launch_angle, launch_speed, etc.

================================================================================

Personal Project Notes and Future Improvements (basically want a running list of project goals and direction)
This section compiles suggestions, future improvements, and notes from conversations related to the MLB betting/baseball data project (repo: mlb-betting under umbrella sports-trading). These are drawn from user inputs across chats to maintain an ongoing model of project goals, enhancements, and tasks. Organized chronologically by conversation timestamp for traceability, with details on what was suggested and why.
From February 24, 2026 Conversation (Folder Structure and Setup)

Rename local folder for consistency with GitHub repo: Change the local sports_trading folder name to mlb-betting (or align closely) to match the GitHub repo name. This improves clarity and avoids confusion when pushing/pulling code. Consider standardizing to lowercase sports-trading for professionalism if keeping as parent.
Umbrella structure for multi-sport expansion: Use SPORTS_TRADING (or renamed sports-trading) as a top-level folder containing sub-projects like mlb-betting. Plan to add sibling folders for NFL, NBA, etc., to support future betting models across sports. This allows shared resources (e.g., virtual environments) while keeping sport-specific code isolated.
File organization within repo: Move project-specific files like .env, .gitignore, README.md, and requirements.txt (reqs) into the mlb-betting subfolder. Keep shared items like the virtual environment (venv314) in the parent folder for reuse across sports projects.
Fix Python versioning in virtual environment: Resolve issues where scripts fail to run with python file_name.py due to version mismatches. Recreate the venv with the correct Python version (e.g., via symlinks like ln -s /usr/local/bin/python3.14 /usr/local/bin/python) to ensure consistent execution across the project.

From March 3, 2026 Conversation (Project Tracking and Access)

Document GitHub repo details: Always include the exact repo URL (e.g., github.com/HissingWings/mlb-betting) in discussions for quick reference. Add a project recap section to the README to summarize current state, avoiding reliance on chat history.
Improve project continuity across sessions: Since chats are isolated, create a "living log" file (e.g., project_log.md in the repo) to track progress, summaries, and timelines. This can be pasted into new chats or used as a central reference.
Explore repo visibility and sharing: If the repo is private, consider making it public or sharing access tokens for collaborative reviews. Include status checks in the log, like last commit date and open issues.

From March 6, 2026 Conversation (Data Fetching and Weighting)

Enhance data weighting procedures: Add all linear weights (e.g., from FanGraphs Guts table for 2021–2025) to a central file (e.g., append to pelota.csv). Apply the same weighting methodology used for other elements to in-season weight estimates for consistency in betting models.
Improve schedule fetcher resilience: Modify the schedule fetcher script to save data for each year (or month for older data) individually as soon as it's fetched. This prevents total data loss if errors occur mid-process (e.g., during long runs for historical seasons). Implement month-by-month fetching for pre-modern eras to handle API limits better.
Maintain chat/project continuity: For ongoing development, prefer continuing in the same chat thread when possible. If starting new, include a pasted summary of progress/timeline to avoid re-explaining. Add this as a best practice to the README.
Create a project timeline and status overview: Regularly update a dedicated section in the README with a timeline of evolution (e.g., initial setup, data fetching additions, weighting integrations) and current status (e.g., completed scripts, pending expansions). Include next steps like testing historical data pulls.

From Current Conversation (March 6, 2026) (Player Logs Fetching)

Update file saving structure for game logs: Change the output paths in player_logs_fetcher.py to save files flatly in data/raw/player_logs/season_totals (e.g., batting_game_logs_YYYY.parquet, pitching_game_logs_YYYY.parquet) instead of per-year subfolders. This simplifies access and organization for analysis.
Expand historical data fetching: Currently set to 2000–2014; plan to adjust start_year to 1980 or earlier when data reliability is confirmed (e.g., full game logs are available and consistent). Start with 2015+ (Statcast era) for modern analysis, then backfill.
Handle platoon splits separately: Keep platoon splits (vs_LHP/RHP) in a dedicated folder like season_splits with flattened filenames (e.g., batting_season_vs_LHP_YYYY.parquet). Consider options to flatten them further, remove if not needed for betting models, or integrate into main logs.
Add error handling and retries: Incorporate retry logic (e.g., up to 3 attempts per year with sleeps) for failed fetches due to API issues. This builds on the schedule fetcher improvements for robustness.

These notes form an actionable backlog. Prioritize based on immediate needs (e.g., data fetching stability), and update this list as new suggestions arise. Track completion in the README with checkboxes or a Kanban-style table for visibility.