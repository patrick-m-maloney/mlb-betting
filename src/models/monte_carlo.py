"""
Monte Carlo game simulator — DuckDB-based, no pandas.

Pipeline:
  1. Load today's confirmed lineups from silver layer
  2. Get per-player wOBA from ProjectionEngine
  3. Project team runs via batting-order weighted wOBA
  4. Run 10,000 normal simulations per game
  5. Output win probabilities + projected run totals

Usage:
  python src/models/monte_carlo.py                # simulate today's games
  python src/models/monte_carlo.py 2026-03-17     # simulate a specific date
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import os
import tempfile
import numpy as np
import duckdb
from datetime import datetime, timezone

from config.settings import SILVER_LINEUPS_DIR, SIMULATIONS_DIR
from src.models.projections import ProjectionEngine


# ========================= CONFIGURATION =========================

N_SIMS = 10_000
RANDOM_SEED = 42

# Home/away multipliers (empirical MLB advantage)
AWAY_MULTIPLIER = 0.98
HOME_MULTIPLIER = 1.04

# Realistic PA per batting-order spot (MLB 2019-2025 avg)
PA_PER_SPOT = [4.85, 4.70, 4.55, 4.45, 4.35, 4.25, 4.15, 4.05, 3.95]

# Simulation noise (std dev of runs per team per game)
RUNS_STD_DEV = 2.6

# League calibration: avg wOBA × avg PA → avg runs/game
RUNS_PER_GAME_TARGET = 4.60

# =================================================================


class MonteCarloSimulator:
    def __init__(self):
        self.projections = ProjectionEngine()
        total_pa = sum(PA_PER_SPOT)
        self.woba_to_runs = RUNS_PER_GAME_TARGET / (
            self.projections.league_avg_woba * total_pa
        )

    def load_todays_games(self, game_date: str | None = None) -> list[dict]:
        """Load confirmed lineups from silver layer for a given date."""
        if game_date is None:
            game_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        year = game_date[:4]
        silver_path = SILVER_LINEUPS_DIR / f"lineups_{year}.parquet"

        if not silver_path.exists():
            print(f"⚠️  No lineup file: {silver_path}")
            return []

        con = duckdb.connect()
        rows = con.execute(f"""
            SELECT *
            FROM read_parquet('{silver_path}')
            WHERE game_date = '{game_date}'
        """).fetchall()
        cols = [d[0] for d in con.description]
        con.close()

        games = [dict(zip(cols, row)) for row in rows]
        confirmed = [g for g in games if g.get("is_confirmed")]
        print(f"📋 {len(games)} games for {game_date} ({len(confirmed)} confirmed)")
        return games

    def project_lineup_runs(self, bbref_ids: list, multiplier: float) -> float:
        """Project team runs from lineup wOBA values + batting-order PA weights."""
        wobas = self.projections.get_lineup_wobas(bbref_ids)
        pa_weights = PA_PER_SPOT[: len(wobas)]
        contributions = [
            w * pa * self.woba_to_runs for w, pa in zip(wobas, pa_weights)
        ]
        return sum(contributions) * multiplier

    def simulate_game(self, game: dict) -> dict:
        """Run N_SIMS Monte Carlo simulations for a single game."""
        away = game["away_team"]
        home = game["home_team"]
        away_bbref = game.get("away_lineup_bbref_ids") or []
        home_bbref = game.get("home_lineup_bbref_ids") or []

        away_runs_mean = self.project_lineup_runs(away_bbref, AWAY_MULTIPLIER)
        home_runs_mean = self.project_lineup_runs(home_bbref, HOME_MULTIPLIER)

        # Simulation
        rng = np.random.default_rng(RANDOM_SEED)
        away_rs = rng.normal(away_runs_mean, RUNS_STD_DEV, N_SIMS).clip(0)
        home_rs = rng.normal(home_runs_mean, RUNS_STD_DEV, N_SIMS).clip(0)

        away_win_prob = float((away_rs > home_rs).mean())
        home_win_prob = float((home_rs > away_rs).mean())

        print(
            f"  {away:<5} @ {home:<5}  "
            f"{away_runs_mean:.2f} - {home_runs_mean:.2f}  "
            f"({away} {away_win_prob:.1%} / {home} {home_win_prob:.1%})"
        )

        return {
            "game_date": str(game.get("game_date", ""))[:10],
            "away_team": away,
            "home_team": home,
            "away_runs_proj": round(float(away_rs.mean()), 2),
            "home_runs_proj": round(float(home_rs.mean()), 2),
            "total_proj": round(float(away_rs.mean() + home_rs.mean()), 2),
            "away_win_prob": round(away_win_prob, 4),
            "home_win_prob": round(home_win_prob, 4),
            "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def run_all(self, game_date: str | None = None) -> list[dict]:
        """Simulate all games for a given date."""
        games = self.load_todays_games(game_date)
        if not games:
            print("No games to simulate.")
            return []

        # Skip games with empty lineups (no player data to simulate)
        simulable = [
            g for g in games
            if (g.get("away_lineup_bbref_ids") or [])
            and (g.get("home_lineup_bbref_ids") or [])
        ]
        skipped = len(games) - len(simulable)
        if skipped:
            print(f"   ⚠️  Skipping {skipped} games with missing lineups")

        print(f"\n🎲 Simulating {len(simulable)} games ({N_SIMS:,} sims each)...")
        results = [self.simulate_game(g) for g in simulable]

        # Save
        self._save_results(results)

        # Summary table
        print(f"\n🎉 {len(results)} games simulated!")
        sorted_r = sorted(results, key=lambda r: r["total_proj"], reverse=True)
        print(
            f"\n{'Away':<6} {'Home':<6} {'AwayRS':>7} {'HomeRS':>7} "
            f"{'Total':>6} {'AwayWin%':>9}"
        )
        print("-" * 50)
        for r in sorted_r:
            print(
                f"{r['away_team']:<6} {r['home_team']:<6} "
                f"{r['away_runs_proj']:>7.2f} {r['home_runs_proj']:>7.2f} "
                f"{r['total_proj']:>6.1f} {r['away_win_prob']:>8.1%}"
            )

        return results

    def _save_results(self, results: list[dict]) -> None:
        if not results:
            return
        SIMULATIONS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        target = SIMULATIONS_DIR / f"simulation_results_{ts}.parquet"

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(results, f)
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{tmp_path}')) "
                f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            n = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{target}')"
            ).fetchone()[0]
            con.close()
            print(f"💾 Saved {target.name}: {n} rows")
        finally:
            os.unlink(tmp_path)


if __name__ == "__main__":
    sim = MonteCarloSimulator()
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sim.run_all(date_arg)
