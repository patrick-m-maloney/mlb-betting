import sys
from pathlib import Path

# To run: caffeinate -s python -m src.automation.mlb_scraper_bot

# Robust path fix (repo src/ layout)
root = Path(__file__).resolve().parent.parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import time
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Existing + new prediction-market fetchers
from src.data_ingestion.lineups import fetch_lineups, save_lineups
from src.data_ingestion.odds.the_odds_api_fetcher import fetch_odds, fetch_futures
from src.data_ingestion.odds.rundown_fetcher import fetch_rundown_mlb
from src.data_ingestion.odds.kalshi_fetcher import fetch_kalshi_mlb
from src.data_ingestion.odds.polymarket_fetcher import fetch_polymarket_mlb

scheduler = BackgroundScheduler()
current_interval = 30

def scrape_all():
    global current_interval
    print(f"\n🚀 Scrape started @ {datetime.now():%H:%M:%S} ET")

    # 1. Lineups
    df_lineups = fetch_lineups(live=True)
    if df_lineups is not None and not df_lineups.empty:
        save_lineups(df_lineups)
        print("✅ Lineups saved (Rotowire)")

    # 2. The Odds API
    fetch_odds(sport_key="baseball_mlb", markets="h2h,spreads,totals")
    fetch_futures()

    # 3. TheRundown (skipped until activated)
    fetch_rundown_mlb()

    # 4. Prediction markets (new — public)
    fetch_kalshi_mlb()
    fetch_polymarket_mlb()

    # Dynamic interval logic (unchanged)
    new_interval = 30
    if df_lineups is not None and not df_lineups.empty:
        now = datetime.now(timezone.utc)
        for gt in df_lineups.get("game_time", []):
            try:
                if isinstance(gt, str):
                    gt = datetime.fromisoformat(gt.replace("Z", "+00:00"))
                minutes_to_game = (gt - now).total_seconds() / 60
                if 0 < minutes_to_game <= 60:
                    new_interval = 10
                    break
            except:
                pass

    if new_interval != current_interval:
        current_interval = new_interval
        print(f"⏰ Switching to every {new_interval} minutes")
        scheduler.reschedule_job('scrape_job', trigger=IntervalTrigger(minutes=new_interval))

def main():
    print("Starting MLB Scraper Bot (+ Kalshi & Polymarket)")
    scheduler.add_job(scrape_all, trigger=IntervalTrigger(minutes=30), id='scrape_job', next_run_time=datetime.now())
    scheduler.start()
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main()