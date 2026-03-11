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

March 06, 2026 – player_logs_fetcher.py upgraded to daily Baseball-Reference loop
              - Uses batting_stats_range / pitching_stats_range per game day
              - Full Lahman enrichment (Lg, full_team_name, key_mlbam)
              - Automatically skips Spring Training (game_type == 'S')
              - Flat schedule path + robust retries
              → Now stable and ready for lineup scraper + Monte Carlo

March 07, 2026 – player_logs_fetcher.py hardened for historical backfill
              - Increased sleep (4.2–7.8s random) + exponential backoff
              - Now logs failed dates
              - Recommendation: run ONE YEAR at a time with short breaks
              → Survived 2020–2021 cleanly; ready for 2022–2024

March 07, 2026 – player_logs_fetcher.py now fully resumable + checkpointed
              - Saves every 10 days + on interrupt
              - Auto-resumes from existing parquet
              - Failed dates logged separately
              - Recommended: run with `caffeinate` for long backfills
              → Safe for 2020–2024 historical data (2022 already complete)

March 08, 2026 – Monte Carlo Simulator v2 completed
              - Park + home/away weighting
              - Starter pitch count estimate + bullpen as average
              - Outputs: Avg RS/RA, total, spread, implied moneyline
              - Ready to compare vs TheRundown/Kalshi
              - Full platoon splits coming next

March 08, 2026 – Monte Carlo Simulator fully fixed
              - Uses mlbID (from logs) + bbref IDs (from lineups) via Lahman people.parquet mapping
              - Auto-calculates wOBA
              - Simulates every game in latest lineup scrape using ALL historical player data
              - Ready for betting comparison (Kalshi/Polymarket/TheRundown)

March 11, 2026 - Monte Carlo Simulator issues
   Loaded all historical batting logs
   Tried to map Rotowire BBRef IDs → mlbID using people.parquet
   Averaged wOBA per player
   Scaled team wOBA → projected runs with wOBA * 5.15 * (0.98/1.04)
   Ran 10,000 normal simulations

   Issues that were resolved

      BBRef → mlbID mapping failed for many players because people.parquet (Lahman) only contains the BBRef playerID, not the numeric mlbID your batting logs use.
      The wOBA-to-runs scaling in the projection cell had a bug (5.15 / 38.5 was applied too early) → unrealistic totals like 1.53 runs.
      No fallback for players with zero historical data → they disappeared from the projection.
      Batting order was ignored (all 9 players treated equally).

   What we changed

      Built a reliable BBRef → name → mlbID bridge using fuzzy matching on your existing name_to_mlb dict.
      Added league-average wOBA fallback (dynamic, calculated from your data) for any missing player.
      Added realistic expected PA per batting-order spot (4.85 down to 3.95).
      Fixed the wOBA → runs math to the correct multiplier (≈ 0.418) so a league-average lineup now projects exactly ~5.15 runs.
      Made every magic number a named constant at the top of the file.
      Kept your original 10,000-simulation Monte Carlo engine intact (now fed much more accurate per-player wOBA + order weighting).

Core Pipeline Now Working (March 2026)
We have a fully functional daily projection engine that:

Scrapes fresh lineups + odds every day (lineups.py)
Uses all historical player data you have (2020–2026 batting/pitching logs)
Runs Monte Carlo for every game in the latest lineup scrape
Outputs realistic RS/RA, total, spread, and implied moneyline ready for Kalshi/Polymarket/TheRundown comparison

Major Components Completed & Fixed
1. Player Logs Fetcher (src/data_ingestion/player_logs_fetcher.py)

Daily Baseball-Reference loop using batting_stats_range / pitching_stats_range
Auto-skips Spring Training (game_type == 'R')
Resumable + checkpoints every 10 days
Flat schedule path (data/raw/schedules/games_YYYY.parquet)
Lahman enrichment (Lg, full_team_name) via post-process or built-in

2. Lineup Scraper (src/data_ingestion/lineups.py)

Working after rapidfuzz downgrade to 3.13.0
Saves structured lineups with away_lineup_bbref_ids, home_lineup_bbref_ids, starters, odds, weather
Path: data/bronze/lineups/YYYY-MM-DD/lineups_HHMMSS.parquet (or your current bronze folder)

3. Monte Carlo Simulator (src/models/monte_carlo.py) – CURRENT VERSION

Loads latest lineup file automatically
Matches players using:
Primary: away_lineup_bbref_ids / home_lineup_bbref_ids → Lahman people.parquet (bbref → mlbID)
Fallback: fuzzy name match on away_lineup / home_lineup names → batting logs Name + mlbID

Calculates wOBA on-the-fly from counting stats + linear_weights.csv (if missing)
Starter IP estimate + bullpen as average pitcher
Park factor placeholder + home advantage
10,000 sims per game → realistic totals (~8.5–10 runs)
Outputs: RS/RA per team, total, spread, implied ML, win prob
Saves results to data/simulations/simulation_results_YYYYMMDD_HHMM.parquet
Debug prints show every game being processed (SIMULATING: TOR @ BAL) and wOBA used

4. Reference Data

Lahman 1871–2025 (people.parquet, historical_teams_data.parquet, etc.)
Linear weights (for wOBA calc)
All player logs use mlbID (numeric); lineups use bbref strings → crosswalk now reliable

5. Key Fixes Applied

rapidfuzz version pin (3.13.0)
ID matching (bbref first, fuzzy name fallback)
wOBA floor (0.290) for prospects/rookies
Realistic scaling (5.05 multiplier) so totals look normal
Checkpointing + resumable fetcher
No more "made-up games" — now processes every row in the latest lineup file

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

Yearly Lahman Redownload (after each season):
1. Go to https://sabr.app.box.com/s/y1prhc795jk8zvmelfd3jq7tl389y6cd
2. Download fresh CSVs + readme
3. Convert to parquet (keep exact same filenames)
4. Overwrite everything in data/reference/lahman_files/

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