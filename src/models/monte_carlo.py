import os
import pandas as pd
import numpy as np
import glob
import ast
from datetime import datetime
from rapidfuzz import process

class MonteCarloSimulator:
    def __init__(self):
        self.logs_dir = "data/raw/player_logs/game_by_game"
        self.lineups_dir = "data/bronze/lineups"

        # Load ALL player data
        bat_files = sorted(glob.glob(f"{self.logs_dir}/batting_game_logs_*.parquet"))
        self.batting = pd.concat([pd.read_parquet(f) for f in bat_files], ignore_index=True)
        
        print(f"✅ Loaded {len(self.batting):,} batting rows from all years")

        # Name → mlbID map (for fallback)
        self.name_to_mlb = {}
        for _, row in self.batting.iterrows():
            name = str(row.get('Name', '')).strip().lower()
            mlbid = row.get('mlbID')
            if name and pd.notna(mlbid):
                self.name_to_mlb[name] = mlbid

        # wOBA calculation
        if 'wOBA' not in self.batting.columns:
            print("   Calculating wOBA...")
            self.batting = self._calculate_woba(self.batting)
        
        self.player_woba = self.batting.groupby('mlbID')['wOBA'].mean().to_dict()

        self.people = pd.read_parquet("data/reference/lahman_files/people.parquet")

    def _calculate_woba(self, df):
        lw = pd.read_csv("data/reference/linear_weights.csv").set_index('Season')
        df = df.copy()
        def calc(row):
            year = int(row.get('game_year', 2025))
            if year not in lw.index:
                year = lw.index.max()
            w = lw.loc[year]
            pa = row.get('PA', 1) or 1
            num = (w.get('wBB', 0.69) * row.get('BB', 0) +
                   w.get('wHBP', 0.72) * row.get('HBP', 0) +
                   w.get('w1B', 0.88) * (row.get('H', 0) - row.get('2B', 0) - row.get('3B', 0) - row.get('HR', 0)) +
                   w.get('w2B', 1.25) * row.get('2B', 0) +
                   w.get('w3B', 1.57) * row.get('3B', 0) +
                   w.get('wHR', 2.0) * row.get('HR', 0))
            return num / pa if pa > 0 else 0.320
        df['wOBA'] = df.apply(calc, axis=1)
        return df

    def load_latest_lineups(self):
        files = sorted(glob.glob(f"{self.lineups_dir}/*/*.parquet"), reverse=True)
        if not files:
            raise FileNotFoundError("No lineup files — run lineups.py first!")
        df = pd.read_parquet(files[0])
        print(f"📋 LOADED {len(df)} games from latest scrape: {files[0]}")
        return df

    def get_mlbid_list(self, bbref_list, name_list):
        mlbids = []
        # 1. Try bbref IDs first (best data)
        for bbref in bbref_list:
            if pd.notna(bbref):
                match = self.people[self.people['playerID'] == bbref]
                if not match.empty:
                    mlbids.append(match.iloc[0].get('mlbID') or match.iloc[0].get('key_mlbam'))
                    continue
        # 2. Fall back to name fuzzy matching
        for name in name_list:
            if not name: continue
            clean = str(name).strip().lower()
            if clean in self.name_to_mlb:
                mlbids.append(self.name_to_mlb[clean])
                continue
            match = process.extractOne(clean, self.name_to_mlb.keys(), score_cutoff=75)
            if match:
                mlbids.append(self.name_to_mlb[match[0]])
        return [m for m in mlbids if m is not None]

    def simulate_game(self, row):
        print(f"SIMULATING: {row['away_team']} @ {row['home_team']}")

        try:
            away_bbref = ast.literal_eval(row.get('away_lineup_bbref_ids', '[]'))
            home_bbref = ast.literal_eval(row.get('home_lineup_bbref_ids', '[]'))
            away_names = ast.literal_eval(row.get('away_lineup', '[]'))
            home_names = ast.literal_eval(row.get('home_lineup', '[]'))
        except:
            away_bbref = []
            home_bbref = []
            away_names = row.get('away_lineup', [])
            home_names = row.get('home_lineup', [])

        away_mlbid = self.get_mlbid_list(away_bbref, away_names)
        home_mlbid = self.get_mlbid_list(home_bbref, home_names)

        def team_woba(ids):
            wobas = [self.player_woba.get(mid, 0.305) for mid in ids]
            return np.mean(wobas) if wobas else 0.320

        away_woba = team_woba(away_mlbid)
        home_woba = team_woba(home_mlbid)

        print(f"   wOBA: {row['away_team']} = {away_woba:.3f} ({len(away_mlbid)} matched) | {row['home_team']} = {home_woba:.3f} ({len(home_mlbid)} matched)")

        away_rs_mean = away_woba * 5.15 * 0.98
        home_rs_mean = home_woba * 5.15 * 1.04

        n_sims = 10000
        np.random.seed(42)
        away_rs = np.random.normal(away_rs_mean, 2.6, n_sims).clip(0)
        home_rs = np.random.normal(home_rs_mean, 2.6, n_sims).clip(0)

        avg_away = away_rs.mean()
        avg_home = home_rs.mean()
        win_prob = (away_rs > home_rs).mean()
        total = avg_away + avg_home

        return {
            'game_date': row.get('game_date'),
            'away_team': row['away_team'],
            'home_team': row['home_team'],
            'away_rs_proj': round(avg_away, 2),
            'home_rs_proj': round(avg_home, 2),
            'total_proj': round(total, 2),
            'away_win_prob': round(win_prob, 4),
            'implied_ml_away': round(100 * (win_prob / (1 - win_prob)) if win_prob < 0.5 else -100 * ((1 - win_prob) / win_prob)),
        }

    def run_all_games(self):
        lineups_df = self.load_latest_lineups()
        results = []
        for _, row in lineups_df.iterrows():
            result = self.simulate_game(row)
            results.append(result)

        results_df = pd.DataFrame(results)
        
        os.makedirs("data/simulations", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        save_path = f"data/simulations/simulation_results_{timestamp}.parquet"
        results_df.to_parquet(save_path, index=False)
        
        print(f"\n🎉 DONE — {len(results_df)} games simulated!")
        print(f"Saved to: {save_path}")
        print("\nTop 5 highest projected totals:")
        print(results_df.nlargest(5, 'total_proj')[['away_team', 'home_team', 'away_rs_proj', 'home_rs_proj', 'total_proj', 'implied_ml_away']])

if __name__ == "__main__":
    sim = MonteCarloSimulator()
    sim.run_all_games()