import os
import pandas as pd
import pybaseball as pb
import time
import random
from tqdm import tqdm

# ====================== LAHMAN REFERENCE ======================
LAHMAN_DIR = "data/reference/lahman_files"
teams_df    = pd.read_parquet(f"{LAHMAN_DIR}/historical_teams_data.parquet")
people_df   = pd.read_parquet(f"{LAHMAN_DIR}/people.parquet")

def get_team_info(year: int, tm: str):
    match = teams_df[(teams_df['yearID'] == year) & (teams_df['teamID'] == tm)]
    if not match.empty:
        row = match.iloc[0]
        return row['lgID'], row['name']
    fallback = teams_df[teams_df['teamID'] == tm].sort_values('yearID', ascending=False)
    if not fallback.empty:
        row = fallback.iloc[0]
        return row['lgID'], row['name']
    return pd.NA, f"Unknown ({tm})"

# ====================== FETCHER WITH CHECKPOINTS ======================
def fetch_game_logs(year: int = 2023):
    print(f"📥 Starting / resuming daily BR logs fetch for {year}...")

    # Smart path: tries flat first, then old subfolder
    flat_path = f"data/schedules/games_{year}.parquet"
    sub_path  = f"data/raw/schedules/games_{year}.parquet"
    
    if os.path.exists(flat_path):
        schedule_path = flat_path
        print(f"   → Using flat schedule file: {schedule_path}")
    elif os.path.exists(sub_path):
        schedule_path = sub_path
        print(f"   → Using nested schedule file: {schedule_path}")
    else:
        print(f"❌ ERROR: Schedule file missing for {year}!")
        print(f"   Checked:\n   1. {flat_path}\n   2. {sub_path}")
        print("\nRun your schedule fetcher first for this year (or move the file).")
        return

    schedule = pd.read_parquet(schedule_path)
    schedule['game_date'] = pd.to_datetime(schedule['game_date'])

    if 'game_type' in schedule.columns:
        regular = schedule[schedule['game_type'] == 'R'].copy()
    else:
        regular = schedule[schedule['game_date'] >= f'{year}-03-20'].copy()

    all_dates = sorted(regular['game_date'].dt.date.unique())
    print(f"   → {len(all_dates)} regular-season days total")

    out_dir = "data/raw/player_logs/game_by_game"
    os.makedirs(out_dir, exist_ok=True)
    batting_path = f"{out_dir}/batting_game_logs_{year}.parquet"
    pitching_path = f"{out_dir}/pitching_game_logs_{year}.parquet"

    # Resume from existing file
    batting_dfs = []
    pitching_dfs = []
    processed_dates = set()

    if os.path.exists(batting_path):
        print("   → Resuming from existing file...")
        existing = pd.read_parquet(batting_path)
        if 'game_date' in existing.columns:
            processed_dates = set(existing['game_date'].dt.date.unique())
            batting_dfs.append(existing)
            print(f"      Already done: {len(processed_dates)} days")

    remaining_dates = [d for d in all_dates if d not in processed_dates]
    print(f"   → {len(remaining_dates)} days still needed\n")

    for i, game_date in enumerate(tqdm(remaining_dates, desc="Fetching")):
        date_str = game_date.strftime('%Y-%m-%d')

        success = False
        for attempt in range(5):
            try:
                bat = pb.batting_stats_range(date_str, date_str)
                pit = pb.pitching_stats_range(date_str, date_str)
                time.sleep(random.uniform(4.5, 8.5))
                success = True
                break
            except Exception:
                wait = 12 * (2 ** attempt)
                time.sleep(wait)

        if not success:
            print(f"   ❌ Failed {date_str} — skipping")
            continue

        if not bat.empty:
            bat = bat.copy()
            bat['game_date'] = pd.to_datetime(date_str)
            bat['game_year'] = year
            bat[['Lg', 'full_team_name']] = bat.apply(lambda r: pd.Series(get_team_info(year, r['Tm'])), axis=1)
            bat['Lg'] = bat.Lev.str.split('-').str[1]
            batting_dfs.append(bat)

        if not pit.empty:
            pit = pit.copy()
            pit['game_date'] = pd.to_datetime(date_str)
            pit['game_year'] = year
            pit[['Lg', 'full_team_name']] = pit.apply(lambda r: pd.Series(get_team_info(year, r['Tm'])), axis=1)
            pit['Lg'] = pit.Lev.str.split('-').str[1]
            pitching_dfs.append(pit)

        # 💾 SAVE EVERY 10 SUCCESSFUL DAYS
        if (i + 1) % 10 == 0 or (i + 1) == len(remaining_dates):
            print(f"   💾 Checkpointing after {i+1} new days...")
            if batting_dfs:
                pd.concat(batting_dfs, ignore_index=True).to_parquet(batting_path, index=False)
            if pitching_dfs:
                pd.concat(pitching_dfs, ignore_index=True).to_parquet(pitching_path, index=False)

    print(f"\n🎉 {year} complete!")
    print(f"   Batting : {sum(len(df) for df in batting_dfs):,} rows")
    print(f"   Pitching: {sum(len(df) for df in pitching_dfs):,} rows")
    print("   (Progress saved every 10 days — you can safely stop/restart anytime)")

if __name__ == "__main__":
    fetch_game_logs(2026)   # ← CHANGE THIS NUMBER ONLY

# data/schedules/games_2026.parquet