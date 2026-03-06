import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pybaseball as pb
import pandas as pd
from datetime import datetime
import time
import warnings
warnings.filterwarnings("ignore")

GAME_LOGS_PATH = Path("data/raw/player_logs/game_by_game")
GAME_LOGS_PATH.mkdir(parents=True, exist_ok=True)

def fetch_game_logs(start_year=2015, end_year=None):
    if end_year is None:
        end_year = datetime.now().year

    print(f"📥 Fetching game-by-game player logs {start_year}–{end_year} (Baseball-Reference via pybaseball)...")

    for year in range(start_year, end_year + 1):
        print(f"   → {year}")

        for attempt in range(3):  # retry up to 3 times
            try:
                # Batting game logs (per player per game)
                batting_logs = pb.batting_stats_bref(year)
                if batting_logs is None or batting_logs.empty:
                    print(f"      No batting data for {year}")
                    break

                batting_logs["year"] = year
                batting_filename = GAME_LOGS_PATH / f"batting_game_logs_{year}.parquet"
                batting_logs.to_parquet(batting_filename)
                print(f"      Saved batting game logs ({len(batting_logs):,} rows) → {batting_filename}")

                # Pitching game logs
                pitching_logs = pb.pitching_stats_bref(year)
                if pitching_logs is None or pitching_logs.empty:
                    print(f"      No pitching data for {year}")
                    break

                pitching_logs["year"] = year
                pitching_filename = GAME_LOGS_PATH / f"pitching_game_logs_{year}.parquet"
                pitching_logs.to_parquet(pitching_filename)
                print(f"      Saved pitching game logs ({len(pitching_logs):,} rows) → {pitching_filename}")

                break  # success → next year

            except Exception as e:
                print(f"      ⚠️  Attempt {attempt+1} failed for {year}: {e}")
                time.sleep(5)  # wait longer before retry

        time.sleep(2.5)  # gentle pause between years

    print("✅ Game-by-game logs saved (one file per year).")

if __name__ == "__main__":
    # Start from 2015 (full game logs reliable here)
    # Change to 2000 or 1980 later when ready
    fetch_game_logs(start_year=2015)


