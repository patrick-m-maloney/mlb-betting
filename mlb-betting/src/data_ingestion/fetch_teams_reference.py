import os
import pybaseball as pb
import pandas as pd

def fetch_teams_reference():
    # Fetch historical teams data (1871–2023)
    teams_df = pb.lahman.teams_core()
    franchises_df = pb.lahman.teams_franchises()
    
    # Merge for full context (use current franchName as fallback)
    teams_df = teams_df.merge(franchises_df[['franchID', 'franchName']], on='franchID', how='left')
    
    # Clean/select key columns
    teams_df = teams_df[['yearID', 'teamID', 'lgID', 'franchID', 'name', 'franchName']].rename(columns={
        'yearID': 'year', 'teamID': 'Tm', 'lgID': 'Lg', 'name': 'full_team_name', 'franchName': 'current_franch_name'
    })
    
    # Save to reference
    ref_dir = "data/reference"
    os.makedirs(ref_dir, exist_ok=True)
    ref_path = f"{ref_dir}/teams_historical.parquet"
    teams_df.to_parquet(ref_path)
    print(f"✅ Saved teams reference to {ref_path} (shape: {teams_df.shape})")
    
    return teams_df

if __name__ == "__main__":
    fetch_teams_reference()