import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import statsapi
import pandas as pd
from datetime import datetime, timedelta
import time

RAW_SCHEDULE_PATH = Path("data/raw/schedules")
RAW_SCHEDULE_PATH.mkdir(parents=True, exist_ok=True)

def fetch_schedules(start_year=2000, end_year=None):
    if end_year is None:
        end_year = datetime.now().year

    print(f"📥 Fetching MLB schedules {start_year}–{end_year} (month-by-month for old years)...")

    for year in range(start_year, end_year + 1):
        print(f"   → {year}")
        year_games = []
        
        # For older years use smaller chunks to avoid timeouts
        chunk_size = 30 if year < 2015 else 365
        current = datetime(year, 1, 1)
        end = datetime(year, 12, 31)

        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_size), end)
            try:
                schedule = statsapi.schedule(
                    start_date=current.strftime("%Y-%m-%d"),
                    end_date=chunk_end.strftime("%Y-%m-%d")
                )
                
                for game in schedule:
                    year_games.append({
                        "game_id": game["game_id"],
                        "game_date": game["game_date"],
                        "year": year,
                        "away_team": game["away_name"],
                        "home_team": game["home_name"],
                        "away_score": game.get("away_score"),
                        "home_score": game.get("home_score"),
                        "status": game["status"],
                        "doubleheader": game.get("doubleheader"),
                        "game_type": game["game_type"],
                    })
                
                time.sleep(0.6)  # respectful pause
            except Exception as e:
                print(f"      ⚠️  Chunk error {current.date()}–{chunk_end.date()}: {e}")
            
            current = chunk_end + timedelta(days=1)

        if year_games:
            df = pd.DataFrame(year_games)
            
            # Fix dtype - convert scores to nullable Int64 (handles '0', None, etc.)
            for col in ["away_score", "home_score"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
            # Save this year immediately
            path = RAW_SCHEDULE_PATH / str(year)
            path.mkdir(parents=True, exist_ok=True)
            filename = path / f"games_{year}.parquet"
            df.to_parquet(filename, compression="snappy")
            print(f"   ✅ Saved {len(df):,} games for {year} → {filename}")

    print("✅ All schedules fetched and saved per-year!")

if __name__ == "__main__":
    fetch_schedules(start_year=2000)