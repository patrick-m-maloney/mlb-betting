import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pybaseball as pb
import pandas as pd
from datetime import datetime
import time

# ────────────────────────────────────────────────
# Output structure
# ────────────────────────────────────────────────
SEASON_TOTALS_PATH = Path("data/raw/player_logs/season_totals")
SEASON_TOTALS_PATH.mkdir(parents=True, exist_ok=True)

# Optional: still save splits in per-year folders (or flatten them too)
SPLITS_PATH = Path("data/raw/player_logs/season_splits")
SPLITS_PATH.mkdir(parents=True, exist_ok=True)

def fetch_player_logs(start_year=2015, end_year=None):
    if end_year is None:
        end_year = datetime.now().year

    print(f"📥 Fetching player game-by-game logs + platoon splits {start_year}–{end_year}...")

    for year in range(start_year, end_year + 1):
        print(f"   → {year}")

        try:
            # ─── Batting game logs (game-by-game) ───
            batting_logs = pb.batting_stats(year, qual=0, ind=1)  # ind=1 → per-game
            if batting_logs is not None and not batting_logs.empty:
                batting_logs["year"] = year
                batting_path = SEASON_TOTALS_PATH / f"batting_game_logs_{year}.parquet"
                batting_logs.to_parquet(batting_path)
                print(f"      Saved batting game logs ({len(batting_logs):,} rows) → {batting_path.name}")
            else:
                print(f"      No batting game logs for {year}")

            # ─── Pitching game logs (game-by-game) ───
            pitching_logs = pb.pitching_stats(year, qual=0, ind=1)
            if pitching_logs is not None and not pitching_logs.empty:
                pitching_logs["year"] = year
                pitching_path = SEASON_TOTALS_PATH / f"pitching_game_logs_{year}.parquet"
                pitching_logs.to_parquet(pitching_path)
                print(f"      Saved pitching game logs ({len(pitching_logs):,} rows) → {pitching_path.name}")
            else:
                print(f"      No pitching game logs for {year}")

            # ─── Optional: Platoon splits (season-level vs LHP/RHP) ───
            for split in ["vs_LHP", "vs_RHP"]:
                # Batting splits
                batting_split = pb.batting_stats(year, split=split)
                if batting_split is not None and not batting_split.empty:
                    batting_split["split"] = split
                    batting_split["year"] = year
                    split_path = SPLITS_PATH / f"batting_season_{split}_{year}.parquet"
                    batting_split.to_parquet(split_path)
                    print(f"      Saved batting {split} splits → {split_path.name}")

                # Pitching splits
                pitching_split = pb.pitching_stats(year, split=split)
                if pitching_split is not None and not pitching_split.empty:
                    pitching_split["split"] = split
                    pitching_split["year"] = year
                    split_path = SPLITS_PATH / f"pitching_season_{split}_{year}.parquet"
                    pitching_split.to_parquet(split_path)
                    print(f"      Saved pitching {split} splits → {split_path.name}")

            time.sleep(1.3)  # gentle rate limit — pybaseball + baseball-reference can be sensitive

        except Exception as e:
            print(f"      ⚠️  Error on {year}: {e}")
            time.sleep(5)   # longer wait on failure
            continue

    print("\n✅ Done. Game-by-game logs saved in:")
    print(f"   {SEASON_TOTALS_PATH}")
    print("Platoon splits saved in:")
    print(f"   {SPLITS_PATH}")


if __name__ == "__main__":
    # You requested 2000–2014 in the example call
    fetch_player_logs(start_year=1980, end_year=1999)
    # Or run modern years:
    # fetch_player_logs(start_year=2015)