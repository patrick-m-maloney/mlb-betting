import os
import pandas as pd
from pybaseball import statcast
import time

# ====================== LAHMAN REFERENCE ======================
LAHMAN_DIR = "data/reference/lahman_files"
teams_df = pd.read_parquet(f"{LAHMAN_DIR}/historical_teams_data.parquet")
people_df = pd.read_parquet(f"{LAHMAN_DIR}/people.parquet")

def get_team_info(year: int, tm: str):
    """Year-aware lookup for Lg + full team name (with fallback)"""
    match = teams_df[(teams_df['yearID'] == year) & (teams_df['teamID'] == tm)]
    if not match.empty:
        row = match.iloc[0]
        return row['lgID'], row['name']
    
    # Fallback to most recent year for this franchise
    fallback = teams_df[teams_df['teamID'] == tm].sort_values('yearID', ascending=False)
    if not fallback.empty:
        row = fallback.iloc[0]
        print(f"  → Using {row['yearID']} team data for {tm} in {year}")
        return row['lgID'], row['name']
    return pd.NA, f"Unknown ({tm})"

# ====================== MAIN FETCHER ======================
def fetch_game_logs(year: int = 2025):
    # Load schedule to get exact game dates
    schedule_path = f"data/raw/schedules/{year}/games_{year}.parquet"
    schedule = pd.read_parquet(schedule_path)
    start_date = schedule['game_date'].min().strftime('%Y-%m-%d')
    end_date   = schedule['game_date'].max().strftime('%Y-%m-%d')
    print(f"📥 Fetching Statcast game-by-game logs for {year} ({start_date} → {end_date})...")

    # Fetch with retries
    df = None
    for attempt in range(1, 4):
        try:
            df = statcast(start_dt=start_date, end_dt=end_date)
            break
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            time.sleep(6)
    if df is None or df.empty:
        print("❌ No Statcast data returned.")
        return

    # Save raw pitch-level
    raw_dir = "data/raw/player_logs/game_by_game"
    os.makedirs(raw_dir, exist_ok=True)
    df.to_parquet(f"{raw_dir}/statcast_pitch_level_{year}.parquet")
    print(f"✅ Raw pitch-level saved ({len(df):,} pitches)")

    # Team assignment
    df['batter_team'] = df.apply(lambda r: r['away_team'] if r['inning_topbot'] == 'top' else r['home_team'], axis=1)
    df['pitcher_team'] = df.apply(lambda r: r['home_team'] if r['inning_topbot'] == 'top' else r['away_team'], axis=1)

    outcomes = df[df['events'].notna()].copy()

    # ==================== BATTING LOGS ====================
    batting_logs = outcomes.groupby(['game_date', 'game_pk', 'game_year', 'batter', 'batter_team']).agg(
        player_name=('player_name', 'first'),
        PA=('events', 'count'),
        BB=('events', lambda x: (x == 'walk').sum()),
        IBB=('events', lambda x: (x == 'intent_walk').sum()),
        HBP=('events', lambda x: (x == 'hit_by_pitch').sum()),
        SO=('events', lambda x: (x == 'strikeout').sum()),
        SF=('events', lambda x: (x == 'sac_fly').sum()),
        SH=('events', lambda x: (x == 'sac_bunt').sum()),
        single=('events', lambda x: (x == 'single').sum()),
        double=('events', lambda x: (x == 'double').sum()),
        triple=('events', lambda x: (x == 'triple').sum()),
        HR=('events', lambda x: (x == 'home_run').sum()),
        RBI=('rbi', 'sum')
    ).reset_index()

    batting_logs = batting_logs.rename(columns={'batter': 'player_id', 'batter_team': 'Tm'})
    batting_logs['AB'] = batting_logs['PA'] - batting_logs['BB'] - batting_logs['IBB'] - batting_logs['HBP'] - batting_logs['SF'] - batting_logs['SH']
    batting_logs['H'] = batting_logs['single'] + batting_logs['double'] + batting_logs['triple'] + batting_logs['HR']
    batting_logs['2B'] = batting_logs['double']
    batting_logs['3B'] = batting_logs['triple']

    # Add Lg + full_team_name from Lahman
    batting_logs[['Lg', 'full_team_name']] = batting_logs.apply(
        lambda row: pd.Series(get_team_info(int(row['game_year']), row['Tm'])), axis=1
    )

    batting_logs.to_parquet(f"{raw_dir}/batting_game_logs_{year}.parquet")
    print(f"✅ Batting game logs saved: {batting_logs.shape}")

    # ==================== PITCHING LOGS ====================
    out_events = ['field_out','strikeout','force_out','grounded_into_double_play','fielders_choice',
                  'fielders_choice_out','sac_fly','sac_bunt','double_play','triple_play']

    pitching_logs = outcomes.groupby(['game_date', 'game_pk', 'game_year', 'pitcher', 'pitcher_team']).agg(
        PA=('events', 'count'),
        BB=('events', lambda x: (x == 'walk').sum()),
        IBB=('events', lambda x: (x == 'intent_walk').sum()),
        HBP=('events', lambda x: (x == 'hit_by_pitch').sum()),
        SO=('events', lambda x: (x == 'strikeout').sum()),
        single=('events', lambda x: (x == 'single').sum()),
        double=('events', lambda x: (x == 'double').sum()),
        triple=('events', lambda x: (x == 'triple').sum()),
        HR=('events', lambda x: (x == 'home_run').sum()),
        outs=('events', lambda x: sum(1 for e in x if e in out_events)),
        R=('delta_home_score', 'sum')
    ).reset_index()

    pitching_logs = pitching_logs.rename(columns={'pitcher': 'player_id', 'pitcher_team': 'Tm'})
    pitching_logs['IP'] = pitching_logs['outs'] / 3.0
    pitching_logs['H'] = pitching_logs['single'] + pitching_logs['double'] + pitching_logs['triple'] + pitching_logs['HR']

    # Add Lg + full_team_name from Lahman
    pitching_logs[['Lg', 'full_team_name']] = pitching_logs.apply(
        lambda row: pd.Series(get_team_info(int(row['game_year']), row['Tm'])), axis=1
    )

    # Add player_name from Lahman people table (key_mlbam = Statcast ID)
    pitcher_map = people_df[people_df['key_mlbam'].isin(pitching_logs['player_id'])]\
        [['key_mlbam', 'nameFirst', 'nameLast']]
    pitcher_map['player_name'] = pitcher_map['nameFirst'] + ' ' + pitcher_map['nameLast']
    pitching_logs = pitching_logs.merge(pitcher_map[['key_mlbam', 'player_name']], 
                                        left_on='player_id', right_on='key_mlbam', how='left')
    pitching_logs.drop(columns=['key_mlbam'], inplace=True, errors='ignore')

    pitching_logs.to_parquet(f"{raw_dir}/pitching_game_logs_{year}.parquet")
    print(f"✅ Pitching game logs saved: {pitching_logs.shape}")

    print(f"🎉 All done for {year}!")

if __name__ == "__main__":
    fetch_game_logs(2025)