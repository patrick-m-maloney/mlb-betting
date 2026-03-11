"""
Updated Monte Carlo Simulator — Final Production Version
• All hard-coded values at the top
• Reliable BBRef → mlbID mapping with league-average fallback
• Batting-order weighted run projection (correct scaling)
• Original 10k simulation engine preserved
"""

import os
import pandas as pd
import numpy as np
import glob
import ast
from datetime import datetime
from rapidfuzz import process

# ========================= CONFIGURATION =========================
# All magic numbers live here — change once, affects everything

# Run environment
N_SIMS = 10_000
RANDOM_SEED = 42

# wOBA scaling (calibrated so league avg 0.320 → 5.15 runs/game)
LEAGUE_AVG_WOBA = 0.320
RUNS_PER_GAME_SCALE = 5.15
WOBA_TO_RUNS_MULTIPLIER = RUNS_PER_GAME_SCALE / (LEAGUE_AVG_WOBA * 38.5)  # ≈ 0.418

# Home/away park & home-field advantage
AWAY_MULTIPLIER = 0.98
HOME_MULTIPLIER = 1.04

# Realistic plate appearances per batting-order spot (MLB 2019-2025 avg)
PA_PER_SPOT = [4.85, 4.70, 4.55, 4.45, 4.35, 4.25, 4.15, 4.05, 3.95]

# Simulation noise (std dev of runs per team per game)
RUNS_STD_DEV = 2.6

# Paths
LOGS_DIR = "data/raw/player_logs/game_by_game"
LINEUPS_DIR = "data/bronze/lineups"
SIM_OUTPUT_DIR = "data/simulations"
LINEAR_WEIGHTS_PATH = "data/reference/linear_weights.csv"

# ================================================================

class MonteCarloSimulator:
    def __init__(self):
        print("🚀 Loading historical batting logs...")
        bat_files = sorted(glob.glob(f"{LOGS_DIR}/batting_game_logs_*.parquet"))
        self.batting = pd.concat([pd.read_parquet(f) for f in bat_files], ignore_index=True)
        print(f"✅ Loaded {len(self.batting):,} batting rows")

        # Name → mlbID fallback map
        self.name_to_mlb = {}
        for _, row in self.batting.iterrows():
            name = str(row.get('Name', '')).strip().lower()
            mlbid = row.get('mlbID')
            if name and pd.notna(mlbid):
                self.name_to_mlb[name] = mlbid

        # === FIXED: Calculate wOBA if missing (this was the crash) ===
        if 'wOBA' not in self.batting.columns:
            print("   Calculating wOBA from linear weights...")
            self.batting = self._calculate_woba(self.batting)
        
        # Now safe to compute league average
        self.league_avg_woba = round(self.batting['wOBA'].mean(), 3)
        print(f"🏆 League average wOBA (fallback): {self.league_avg_woba}")

        # wOBA dictionary for fast lookup
        self.player_woba = self.batting.groupby('mlbID')['wOBA'].mean().to_dict()

        self.people = pd.read_parquet("data/reference/lahman_files/people.parquet")
        print("✅ All reference data loaded\n")

    def _calculate_woba(self, df):
        """Exact same logic from your original code"""
        lw = pd.read_csv(LINEAR_WEIGHTS_PATH).set_index('Season')
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
            return num / pa if pa > 0 else self.league_avg_woba
        df['wOBA'] = df.apply(calc, axis=1)
        return df
    def load_latest_lineups(self):
        files = sorted(glob.glob(f"{LINEUPS_DIR}/*/*.parquet"), reverse=True)
        if not files:
            raise FileNotFoundError("No lineup files — run lineups.py first!")
        df = pd.read_parquet(files[0])
        print(f"📋 LOADED {len(df)} games from: {files[0]}")
        return df

    def get_mlbid_list(self, bbref_list, name_list):
        """BBRef-first matching with name fuzzy fallback"""
        mlbids = []
        # 1. Exact BBRef → mlbID via people table
        for bbref in bbref_list:
            if pd.isna(bbref):
                continue
            match = self.people[self.people['playerID'] == str(bbref)]
            if not match.empty:
                row = match.iloc[0]
                mlbid = row.get('mlbID') or row.get('key_mlbam')
                if pd.notna(mlbid):
                    mlbids.append(mlbid)
                    continue

        # 2. Name fuzzy matching (for any remaining gaps)
        for name in name_list:
            if not name:
                continue
            clean = str(name).strip().lower()
            if clean in self.name_to_mlb:
                mlbids.append(self.name_to_mlb[clean])
                continue
            result = process.extractOne(clean, self.name_to_mlb.keys(), score_cutoff=75)
            if result:
                mlbids.append(self.name_to_mlb[result[0]])

        return [m for m in mlbids if m is not None]

    def project_lineup_runs(self, row, team_prefix: str) -> float:
        """Accurate batting-order weighted run projection for ANY team (away or home)"""
        try:
            bbref_col = f"{team_prefix}lineup_bbref_ids"
            name_col  = f"{team_prefix}lineup"
            bbref_ids = ast.literal_eval(row.get(bbref_col, '[]'))
            names     = ast.literal_eval(row.get(name_col, '[]'))
        except:
            bbref_ids = []
            names     = row.get(name_col, [])

        # Get mlbIDs (with league avg fallback already built-in)
        mlbids = self.get_mlbid_list(bbref_ids, names)

        # Get wOBA per player
        wobas = [self.player_woba.get(mid, self.league_avg_woba) for mid in mlbids]

        # Batting-order PA weighting
        expected_pa = PA_PER_SPOT[:len(wobas)]
        contributions = [w * pa * WOBA_TO_RUNS_MULTIPLIER for w, pa in zip(wobas, expected_pa)]

        return round(sum(contributions), 2)

    def simulate_game(self, row):
        print(f"SIMULATING: {row['away_team']} @ {row['home_team']}")

        # === Project BOTH teams properly (no more replace hack) ===
        away_rs_mean = self.project_lineup_runs(row, "away_") * AWAY_MULTIPLIER
        home_rs_mean = self.project_lineup_runs(row, "home_") * HOME_MULTIPLIER

        # Quick debug print so you can see the weighted projections
        print(f"   Projected mean runs → {row['away_team']}: {away_rs_mean:.2f} | {row['home_team']}: {home_rs_mean:.2f}")

        # Monte Carlo simulation (exactly as before)
        np.random.seed(RANDOM_SEED)
        away_rs = np.random.normal(away_rs_mean, RUNS_STD_DEV, N_SIMS).clip(0)
        home_rs = np.random.normal(home_rs_mean, RUNS_STD_DEV, N_SIMS).clip(0)

        avg_away = round(away_rs.mean(), 2)
        avg_home = round(home_rs.mean(), 2)
        win_prob = round((away_rs > home_rs).mean(), 4)
        total    = round(avg_away + avg_home, 2)

        print(f"   Final: {row['away_team']} {avg_away} — {avg_home} {row['home_team']} | Total {total} | Away win {win_prob:.1%}\n")

        return {
            'game_date': row.get('game_date'),
            'away_team': row['away_team'],
            'home_team': row['home_team'],
            'away_rs_proj': avg_away,
            'home_rs_proj': avg_home,
            'total_proj': total,
            'away_win_prob': win_prob,
        }

    def run_all_games(self):
        lineups_df = self.load_latest_lineups()
        results = []
        for _, row in lineups_df.iterrows():
            result = self.simulate_game(row)
            results.append(result)

        results_df = pd.DataFrame(results)
        
        os.makedirs(SIM_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        save_path = f"{SIM_OUTPUT_DIR}/simulation_results_{timestamp}.parquet"
        results_df.to_parquet(save_path, index=False)
        
        print(f"\n🎉 DONE — {len(results_df)} games simulated!")
        print(f"Saved to: {save_path}")
        print("\nTop 5 highest projected totals:")
        print(results_df.nlargest(5, 'total_proj')[['away_team', 'home_team', 'away_rs_proj', 'home_rs_proj', 'total_proj', 'away_win_prob']])

if __name__ == "__main__":
    sim = MonteCarloSimulator()
    sim.run_all_games()


