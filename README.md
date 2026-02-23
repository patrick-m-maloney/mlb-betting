
## Data Pipeline (Daily)
1. Morning: Fetch probable pitchers + projected lineups
2. Continuous: Poll/WebSocket odds (minute-by-minute snapshots)
3. Pre-game: Confirmed lineups, weather, injuries → run full projections
4. Post-game: Store results, update player fingerprints

## Core Modeling
- RS/RA estimation per game via Monte Carlo (10k+ sims) or regressions
- Win % = Pythagorean (or Pythagenport) on simulated runs
- Bayesian updating on pitcher/batter true talent
- **Player Fingerprinting Module** (excellent idea we discussed):
  - KNN similarity search (not pure K-means) on age, service time, prior wRC+/FIP, Statcast metrics, handedness, position, etc.
  - Weighted comps from historical similar players → YoY delta adjustments
  - In-season: Blend with rolling performance + special flags (rookie wall, September fade, post-injury, hot/cold streaks)
  - Feeds directly into each batter/pitcher's expected wOBA/xwOBA distribution in the simulator

## Odds Ingestion Status (as of Feb 19, 2026)
Primary: **TheRundown.io** (WebSocket, Kalshi/Polymarket native, line movement history, 15+ books)
Backup: The Odds API (simple historical snapshots)
Both free tiers available now. Code skeletons coming next.

## Development Roadmap
**Phase 1 (MVP)**: Odds ingestion + storage + basic backtester  
**Phase 2**: Lineup scraper + player fingerprinting + RS/RA simulator  
**Phase 3**: Full edge calculator + betting logic + limit tracking  
**Phase 4**: Automation + dashboard + live execution

## Progress So Far (Feb 19, 2026)
- Full system outline completed
- Detailed tech stack & project structure
- Odds API comparison & recommendation
- Rough code skeleton for odds_fetcher.py (The Odds API version — will update to TheRundown WebSocket)
- Player fingerprinting design (KNN comps class with in-season updates)
- Bankroll, limit-tracking, and meta-analysis strategy defined

## Next Steps (you tell me the order)
1. Create the GitHub repo and add this README
2. I give you: `odds_fetcher.py` (TheRundown version with WebSocket example) + requirements.txt + config/settings.py
3. Or: Full player_fingerprinting.py + pybaseball integration
4. Or: Lineup scraper module
5. Sign up for TheRundown API key and test

Last updated: February 19, 2026