import os
import pandas as pd
from pybaseball import statcast, playerid_reverse_lookup
from datetime import datetime
import time

# Hardcoded team mapping for Lg, full_name, and 3-letter (Tm) consistency
team_info = {
    'ARI': {'league': 'NL', 'full_name': 'Arizona Diamondbacks'},
    'ATL': {'league': 'NL', 'full_name': 'Atlanta Braves'},
    'BAL': {'league': 'AL', 'full_name': 'Baltimore Orioles'},
    'BOS': {'league': 'AL', 'full_name': 'Boston Red Sox'},
    'CHC': {'league': 'NL', 'full_name': 'Chicago Cubs'},
    'CHW': {'league': 'AL', 'full_name': 'Chicago White Sox'},
    'CIN': {'league': 'NL', 'full_name': 'Cincinnati Reds'},
    'CLE': {'league': 'AL', 'full_name': 'Cleveland Guardians'},
    'COL': {'league': 'NL', 'full_name': 'Colorado Rockies'},
    'DET': {'league': 'AL', 'full_name': 'Detroit Tigers'},
    'HOU': {'league': 'AL', 'full_name': 'Houston Astros'},
    'KCR': {'league': 'AL', 'full_name': 'Kansas City Royals'},
    'LAA': {'league': 'AL', 'full_name': 'Los Angeles Angels'},
    'LAD': {'league': 'NL', 'full_name': 'Los Angeles Dodgers'},
    'MIA': {'league': 'NL', 'full_name': 'Miami Marlins'},
    'MIL': {'league': 'NL', 'full_name': 'Milwaukee Brewers'},
    'MIN': {'league': 'AL', 'full_name': 'Minnesota Twins'},
    'NYM': {'league': 'NL', 'full_name': 'New York Mets'},
    'NYY': {'league': 'AL', 'full_name': 'New York Yankees'},
    'OAK': {'league': 'AL', 'full_name': 'Oakland Athletics'},
    'PHI': {'league': 'NL', 'full_name': 'Philadelphia Phillies'},
    'PIT': {'league': 'NL', 'full_name': 'Pittsburgh Pirates'},
    'SDP': {'league': 'NL', 'full_name': 'San Diego Padres'},
    'SFG': {'league': 'NL', 'full_name': 'San Francisco Giants'},
    'SEA': {'league': 'AL', 'full_name': 'Seattle Mariners'},
    'STL': {'league': 'NL', 'full_name': 'St. Louis Cardinals'},
    'TBR': {'league': 'AL', 'full_name': 'Tampa Bay Rays'},
    'TEX': {'league': 'AL', 'full_name': 'Texas Rangers'},
    'TOR': {'league': 'AL', 'full_name': 'Toronto Blue Jays'},
    'WSN': {'league': 'NL', 'full_name': 'Washington Nationals'},
}

def fetch_game_logs(year=2025):
    # Load schedule to determine date range (only dates with games)
    schedule_path = f"data/raw/schedules/{year}/games_{year}.parquet"
    if not os.path.exists(schedule_path):
        print(f"⚠️ Schedule for {year} not found at {schedule_path}. Exiting.")
        return

    schedule = pd.read_parquet(schedule_path)
    if schedule.empty:
        print(f"⚠️ Empty schedule for {year}. Exiting.")
        return

    start_date = schedule['game_date'].min().strftime('%Y-%m-%d')
    end_date = schedule['game_date'].max().strftime('%Y-%m-%d')
    print(f"📥 Fetching game-by-game player logs for {year} based on schedule ({start_date} to {end_date})...")

    # Fetch Statcast with retries
    df = None
    for attempt in range(1, 4):
        try:
            df = statcast(start_dt=start_date, end_dt=end_date)
            break
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}. Retrying in 5s...")
            time.sleep(5)
    if df is None or df.empty:
        print("⚠️ No Statcast data found after retries. Exiting.")
        return

    # Save raw pitch-level data
    raw_dir = "data/raw/player_logs/game_by_game"
    os.makedirs(raw_dir, exist_ok=True)
    raw_path = f"{raw_dir}/statcast_pitch_level_{year}.parquet"
    df.to_parquet(raw_path)
    print(f"✅ Saved raw pitch-level data to {raw_path}")

    # Add team columns
    df['batter_team'] = df.apply(lambda row: row['away_team'] if row['inning_topbot'] == 'top' else row['home_team'], axis=1)
    df['pitcher_team'] = df.apply(lambda row: row['home_team'] if row['inning_topbot'] == 'top' else row['away_team'], axis=1)

    # Filter to PA outcomes
    outcomes = df[df['events'].notna()]

    # Aggregate batting game logs (per game_pk)
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
    )
    batting_logs['AB'] = batting_logs['PA'] - batting_logs['BB'] - batting_logs['IBB'] - batting_logs['HBP'] - batting_logs['SF'] - batting_logs['SH']
    batting_logs['H'] = batting_logs['single'] + batting_logs['double'] + batting_logs['triple'] + batting_logs['HR']
    batting_logs['2B'] = batting_logs['double']
    batting_logs['3B'] = batting_logs['triple']

    # Add Lg and full_team_name
    batter_teams = batting_logs.index.get_level_values('batter_team')
    batting_logs['Lg'] = batter_teams.map(lambda x: team_info.get(x, {}).get('league', pd.NA))
    batting_logs['full_team_name'] = batter_teams.map(lambda x: team_info.get(x, {}).get('full_name', pd.NA))

    # Reset index, rename batter to player_id (mlbID)
    batting_logs = batting_logs.reset_index().rename(columns={'batter': 'player_id', 'batter_team': 'Tm'})

    # Save batting logs
    batting_path = f"{raw_dir}/batting_game_logs_{year}.parquet"
    batting_logs.to_parquet(batting_path)
    print(f"✅ Saved batting game logs to {batting_path} (shape: {batting_logs.shape})")

    # Aggregate pitching game logs (per game_pk)
    out_events = [
        'field_out', 'strikeout', 'force_out', 'grounded_into_double_play', 'fielders_choice',
        'fielders_choice_out', 'sac_fly', 'sac_bunt', 'double_play', 'triple_play',
        'sac_fly_double_play', 'sac_bunt_double_play', 'strikeout_double_play'
    ]
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
        H=('events', lambda x: sum(1 for e in x if e in ['single', 'double', 'triple', 'home_run'])),
        R=('delta_home_score', 'sum')
    )
    pitching_logs['IP'] = pitching_logs['outs'] / 3.0

    # Add Lg and full_team_name
    pitcher_teams = pitching_logs.index.get_level_values('pitcher_team')
    pitching_logs['Lg'] = pitcher_teams.map(lambda x: team_info.get(x, {}).get('league', pd.NA))
    pitching_logs['full_team_name'] = pitcher_teams.map(lambda x: team_info.get(x, {}).get('full_name', pd.NA))

    # Add player_name via lookup
    unique_pitchers = pitching_logs.index.get_level_values('pitcher').unique()
    if len(unique_pitchers) > 0:
        pitcher_names_df = playerid_reverse_lookup(unique_pitchers.tolist(), key_type='mlbam')
        pitcher_names_df['player_id'] = pitcher_names_df['key_mlbam']
        pitcher_names_df['player_name'] = pitcher_names_df['name_first'] + ' ' + pitcher_names_df['name_last']
        pitcher_names_df = pitcher_names_df[['player_id', 'player_name']]
        pitching_logs = pitching_logs.reset_index().merge(pitcher_names_df, how='left', left_on='pitcher', right_on='player_id')
        pitching_logs.drop(columns=['player_id'], inplace=True)  # Drop duplicate after merge
    else:
        pitching_logs = pitching_logs.reset_index()
    pitching_logs.rename(columns={'pitcher': 'player_id', 'pitcher_team': 'Tm'}, inplace=True)

    # Save pitching logs
    pitching_path = f"{raw_dir}/pitching_game_logs_{year}.parquet"
    pitching_logs.to_parquet(pitching_path)
    print(f"✅ Saved pitching game logs to {pitching_path} (shape: {pitching_logs.shape})")

if __name__ == "__main__":
    fetch_game_logs(2025)