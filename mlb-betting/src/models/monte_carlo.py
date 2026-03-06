import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime
from src.features.player_fingerprinting import PlayerComps

# Linear weights loader
def load_linear_weights():
    path = Path("data/reference/linear_weights.csv")
    df = pd.read_csv(path)
    df = df.set_index("Season").sort_index(ascending=False)
    return df

class MonteCarloSimulator:
    def __init__(self, n_sims: int = 10000):
        self.n_sims = n_sims
        self.batter_comps = PlayerComps()
        self.pitcher_comps = PlayerComps()
        self.batter_comps.fit(is_batter=True)
        self.pitcher_comps.fit(is_batter=False)
        self.weights = load_linear_weights()

    def simulate_game(self, lineup_row: pd.Series, market_odds: dict = None) -> dict:
        """
        lineup_row = single row from lineups Parquet
        market_odds = optional dict with current book lines (e.g. from odds fetcher)
        """
        date = lineup_row["fetch_timestamp"].date()
        year = date.year
        w = self.weights.loc[year] if year in self.weights.index else self.weights.iloc[0]

        # Project team runs (aggregate for v1)
        away_rs_exp = self._project_team_runs(lineup_row["away_lineup"], lineup_row["home_starter_name"], w, is_away=True)
        home_rs_exp = self._project_team_runs(lineup_row["home_lineup"], lineup_row["away_starter_name"], w, is_away=False)

        # Monte Carlo runs (negative binomial for realism)
        away_runs_sim = np.random.negative_binomial(n=9, p=9/(9 + away_rs_exp), size=self.n_sims)
        home_runs_sim = np.random.negative_binomial(n=9, p=9/(9 + home_rs_exp), size=self.n_sims)

        # Home-field advantage (your ~8% → ~54.5% home win rate)
        home_win_prob = (home_runs_sim > away_runs_sim).mean() * 1.09
        home_win_prob = min(0.99, max(0.01, home_win_prob))
        away_win_prob = 1 - home_win_prob

        result = {
            "game_date": date,
            "away_team": lineup_row["away_team"],
            "home_team": lineup_row["home_team"],
            "model_away_rs": round(away_rs_exp, 2),
            "model_home_rs": round(home_rs_exp, 2),
            "model_away_win_prob": round(away_win_prob, 4),
            "model_home_win_prob": round(home_win_prob, 4),
            "n_sims": self.n_sims,
        }

        # Edge vs every book
        if market_odds and "odds_line" in lineup_row:
            edges = {}
            for book in ["draftkings", "fanduel", "betmgm", "pointsbet", "composite"]:
                ml = lineup_row["odds_line"].get(book)
                if ml and isinstance(ml, (int, float)):
                    implied = 100 / (ml + 100) if ml > 0 else abs(ml) / (abs(ml) + 100)
                    edges[f"edge_{book}_home"] = round(home_win_prob - implied, 4)
            result["edges"] = edges

        return result

    def _project_team_runs(self, lineup: list, starter: str, weights: pd.Series, is_away: bool) -> float:
        """v1 aggregate projection – will upgrade to full event sim"""
        if not lineup or len(lineup) < 9:
            return 4.5

        total_woba = 0.0
        for player_name in lineup[:9]:
            # Placeholder: use fingerprinting once we have current stats dict
            # For now use league average + small fingerprint adjustment
            base_woba = 0.315
            # TODO: real fingerprint call here
            total_woba += base_woba

        avg_woba = total_woba / 9
        pa_per_game = 38.0 if is_away else 38.3   # slight home PA advantage
        runs = (avg_woba * pa_per_game * weights["wOBAScale"]) / 1.15   # rough wOBA-to-runs scalar
        return max(2.0, runs)

if __name__ == "__main__":
    # Quick test
    from src.data_ingestion.lineups import fetch_lineups
    sim = MonteCarloSimulator(n_sims=5000)
    df = fetch_lineups(live=True)
    if not df.empty:
        result = sim.simulate_game(df.iloc[0])
        print(result)