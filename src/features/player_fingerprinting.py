"""
KNN-based historical player comp system — DuckDB + numpy, no pandas.

Finds the k nearest historical player-seasons for a given player profile
and produces weighted-average stat deltas for projection adjustments.

Data sources (in priority order):
  1. Local FanGraphs leaderboard parquets (data/player_logs/fangraphs_leaderboards/)
  2. pybaseball batting_stats / pitching_stats (fallback for gaps/refresh)

Usage:
  python src/features/player_fingerprinting.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import os
import tempfile
import numpy as np
import duckdb
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings("ignore")

from config.settings import FANGRAPHS_DIR, REFERENCE_DIR, RAW_DIR


# ========================= FEATURE DEFINITIONS =========================

BATTER_FEATURE_COLS = [
    "age", "service_time", "pa", "wrc_plus", "iso", "k_pct", "bb_pct",
    "babip", "hardhit_pct", "barrel_pct", "spd",
]
BATTER_TARGET_COLS = [
    "wrc_plus", "iso", "k_pct", "bb_pct", "babip", "hardhit_pct", "barrel_pct",
]

PITCHER_FEATURE_COLS = [
    "age", "service_time", "ip", "fip", "k_pct", "bb_pct", "gb_pct",
    "hardhit_pct", "barrel_pct",
]
PITCHER_TARGET_COLS = [
    "fip", "k_pct", "bb_pct", "gb_pct", "hardhit_pct", "barrel_pct",
]

# FanGraphs column name → our internal clean name
_BATTING_COL_MAP = {
    "IDfg": "player_id", "Season": "season", "Name": "player_name",
    "Team": "team", "Age": "age", "PA": "pa",
    "wRC+": "wrc_plus", "ISO": "iso", "K%": "k_pct", "BB%": "bb_pct",
    "BABIP": "babip", "HardHit%": "hardhit_pct", "Barrel%": "barrel_pct",
    "Spd": "spd",
}
_PITCHING_COL_MAP = {
    "IDfg": "player_id", "Season": "season", "Name": "player_name",
    "Team": "team", "Age": "age", "IP": "ip",
    "FIP": "fip", "K%": "k_pct", "BB%": "bb_pct", "GB%": "gb_pct",
    "HardHit%": "hardhit_pct", "Barrel%": "barrel_pct",
}

# Qualification thresholds (match pybaseball defaults)
_BATTER_QUAL_PA = 50
_PITCHER_QUAL_IP = 20

# Default year range for comps (Statcast era — all features available)
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025


# ========================= CACHE PATHS =========================

_CACHE_DIR = RAW_DIR / "stats"
_BATTER_CACHE = _CACHE_DIR / "comps_cache_batters.parquet"
_PITCHER_CACHE = _CACHE_DIR / "comps_cache_pitchers.parquet"


class PlayerComps:
    def __init__(self, n_neighbors=15):
        self.n_neighbors = n_neighbors
        self.scaler = StandardScaler()
        self.nn = NearestNeighbors(n_neighbors=n_neighbors, metric="euclidean")
        self.is_batter: bool | None = None
        self.feature_cols: list[str] = []
        self.target_cols: list[str] = []
        # Stored data for comp lookups after fit()
        self._hist_features: np.ndarray | None = None  # (N, n_features) scaled
        self._hist_targets: np.ndarray | None = None    # (N, n_targets)
        self._hist_meta: list[dict] | None = None       # player_name, season, etc.
        self._feature_medians: np.ndarray | None = None  # for filling NULLs in queries

        _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Data loading — local FG parquets + pybaseball fallback
    # ------------------------------------------------------------------

    def _build_select_clause(self, col_map: dict) -> str:
        """Build SQL SELECT with renamed columns from FanGraphs parquet."""
        parts = []
        for fg_col, clean_name in col_map.items():
            parts.append(f'"{fg_col}" AS {clean_name}')
        return ", ".join(parts)

    def _load_local_fg(self, is_batter: bool, start_year: int, end_year: int,
                       con: duckdb.DuckDBPyConnection) -> list[dict] | None:
        """Try loading from local FanGraphs leaderboard parquets."""
        if is_batter:
            pattern = str(FANGRAPHS_DIR / "batting_game_logs_*.parquet")
            col_map = _BATTING_COL_MAP
            qual_filter = f"PA >= {_BATTER_QUAL_PA}"
        else:
            pattern = str(FANGRAPHS_DIR / "pitching_game_logs_*.parquet")
            col_map = _PITCHING_COL_MAP
            qual_filter = f"IP >= {_PITCHER_QUAL_IP}"

        # Check files exist
        import glob
        files = glob.glob(pattern)
        if not files:
            print(f"   ⚠️  No local FG {'batting' if is_batter else 'pitching'} files found")
            return None

        select_clause = self._build_select_clause(col_map)
        try:
            rows = con.execute(f"""
                SELECT {select_clause}
                FROM read_parquet('{pattern}')
                WHERE Season >= {start_year}
                  AND Season <= {end_year}
                  AND {qual_filter}
            """).fetchall()
            col_names = [d[0] for d in con.description]
        except Exception as e:
            print(f"   ⚠️  Error reading local FG data: {e}")
            return None

        if not rows:
            return None

        result = [dict(zip(col_names, row)) for row in rows]

        # Check for critical feature gaps
        key_feature = "hardhit_pct" if is_batter else "gb_pct"
        non_null = sum(1 for r in result if r.get(key_feature) is not None)
        coverage = non_null / len(result) if result else 0
        if coverage < 0.5:
            print(f"   ⚠️  Local FG data has low {key_feature} coverage ({coverage:.0%}), "
                  f"falling back to pybaseball")
            return None

        print(f"   📂 Loaded {len(result)} {'batter' if is_batter else 'pitcher'} "
              f"player-seasons from local FG parquets ({start_year}-{end_year})")
        return result

    def _load_pybaseball_fallback(self, is_batter: bool, start_year: int,
                                  end_year: int) -> list[dict]:
        """Fall back to pybaseball for data that's missing locally."""
        from pybaseball import batting_stats, pitching_stats

        print(f"   🌐 Fetching {'batting' if is_batter else 'pitching'} stats "
              f"from pybaseball ({start_year}-{end_year})...")
        if is_batter:
            import pandas as pd
            df = batting_stats(start_season=start_year, end_season=end_year,
                               qual=_BATTER_QUAL_PA)
            col_map = _BATTING_COL_MAP
        else:
            import pandas as pd
            df = pitching_stats(start_season=start_year, end_season=end_year,
                                qual=_PITCHER_QUAL_IP)
            col_map = _PITCHING_COL_MAP

        # Rename columns to our clean names
        rename = {fg: clean for fg, clean in col_map.items() if fg in df.columns}
        df = df.rename(columns=rename)

        # Convert to list of dicts, keeping only the columns we care about
        keep_cols = [c for c in col_map.values() if c in df.columns]
        result = df[keep_cols].to_dict("records")
        print(f"   ✅ Got {len(result)} player-seasons from pybaseball")
        return result

    def build_historical_data(self, start_year=DEFAULT_START_YEAR,
                              end_year=DEFAULT_END_YEAR, force_pybaseball=False):
        """Build the historical comps database.

        Tries local FG parquets first, falls back to pybaseball if gaps exist.
        Saves separate caches for batters and pitchers.
        """
        print("📥 Building historical comps database...")
        con = duckdb.connect()

        for is_batter in [True, False]:
            label = "batter" if is_batter else "pitcher"
            cache_path = _BATTER_CACHE if is_batter else _PITCHER_CACHE

            rows = None
            if not force_pybaseball:
                rows = self._load_local_fg(is_batter, start_year, end_year, con)

            if rows is None:
                rows = self._load_pybaseball_fallback(is_batter, start_year, end_year)

            # Compute debut_year per player
            debut_years: dict[str, int] = {}
            for r in rows:
                name = r.get("player_name", "")
                season = r.get("season", end_year)
                if name not in debut_years or season < debut_years[name]:
                    debut_years[name] = season
            for r in rows:
                r["debut_year"] = debut_years.get(r.get("player_name", ""), start_year)
                r["service_time"] = r.get("season", end_year) - r["debut_year"]
                r["is_batter"] = is_batter

            # Save cache via DuckDB
            self._save_cache(rows, cache_path, con)
            print(f"   ✅ Cached {len(rows)} {label} player-seasons → {cache_path.name}")

        con.close()
        print("✅ Historical comps database ready.")

    def _save_cache(self, rows: list[dict], cache_path: Path,
                    con: duckdb.DuckDBPyConnection) -> None:
        """Write rows to parquet via DuckDB temp JSON pattern."""
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(rows, f)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{tmp_path}')) "
                f"TO '{cache_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            os.unlink(tmp_path)

    def _load_cache(self, is_batter: bool, con: duckdb.DuckDBPyConnection) -> list[dict] | None:
        """Load cached data for given player type."""
        cache_path = _BATTER_CACHE if is_batter else _PITCHER_CACHE
        if not cache_path.exists():
            return None
        try:
            rows = con.execute(
                f"SELECT * FROM read_parquet('{cache_path}')"
            ).fetchall()
            col_names = [d[0] for d in con.description]
            result = [dict(zip(col_names, row)) for row in rows]
            print(f"   📂 Loaded {len(result)} {'batter' if is_batter else 'pitcher'} "
                  f"player-seasons from cache")
            return result
        except Exception as e:
            print(f"   ⚠️  Cache read error: {e}")
            return None

    # ------------------------------------------------------------------
    # Feature extraction — pure numpy, no pandas
    # ------------------------------------------------------------------

    def _extract_features(self, rows: list[dict]) -> tuple[np.ndarray, np.ndarray]:
        """Extract feature and target arrays from row dicts.

        Returns:
            features: (N, n_features) float64 array, NULLs filled with column medians
            targets:  (N, n_targets) float64 array, NULLs left as NaN
        """
        n = len(rows)
        n_feat = len(self.feature_cols)
        n_targ = len(self.target_cols)

        features = np.full((n, n_feat), np.nan, dtype=np.float64)
        targets = np.full((n, n_targ), np.nan, dtype=np.float64)

        for i, row in enumerate(rows):
            for j, col in enumerate(self.feature_cols):
                val = row.get(col)
                if val is not None:
                    try:
                        features[i, j] = float(val)
                    except (ValueError, TypeError):
                        pass
            for j, col in enumerate(self.target_cols):
                val = row.get(col)
                if val is not None:
                    try:
                        targets[i, j] = float(val)
                    except (ValueError, TypeError):
                        pass

        # Fill NaN features with column medians
        self._feature_medians = np.nanmedian(features, axis=0)
        for j in range(n_feat):
            mask = np.isnan(features[:, j])
            features[mask, j] = self._feature_medians[j]

        return features, targets

    def _extract_query_features(self, player: dict) -> np.ndarray:
        """Extract feature vector for a single player query dict.

        Uses stored medians from fit() for missing values.
        """
        vec = np.full(len(self.feature_cols), np.nan, dtype=np.float64)
        for j, col in enumerate(self.feature_cols):
            val = player.get(col)
            if val is not None:
                try:
                    vec[j] = float(val)
                except (ValueError, TypeError):
                    pass

        # Fill missing with medians from training data
        if self._feature_medians is not None:
            mask = np.isnan(vec)
            vec[mask] = self._feature_medians[mask]

        return vec.reshape(1, -1)

    # ------------------------------------------------------------------
    # Fit / predict
    # ------------------------------------------------------------------

    def fit(self, is_batter=True):
        """Load historical data and fit the KNN model."""
        self.is_batter = is_batter
        if is_batter:
            self.feature_cols = BATTER_FEATURE_COLS
            self.target_cols = BATTER_TARGET_COLS
        else:
            self.feature_cols = PITCHER_FEATURE_COLS
            self.target_cols = PITCHER_TARGET_COLS

        con = duckdb.connect()

        # Try cache first, then build if missing
        rows = self._load_cache(is_batter, con)
        if rows is None:
            con.close()
            self.build_historical_data()
            con = duckdb.connect()
            rows = self._load_cache(is_batter, con)
            if rows is None:
                con.close()
                raise RuntimeError("Failed to build historical data")

        con.close()

        # Extract numpy arrays
        features, targets = self._extract_features(rows)
        self._hist_targets = targets
        self._hist_meta = rows

        # Scale and fit KNN
        self._hist_features = self.scaler.fit_transform(features)
        self.nn.fit(self._hist_features)
        print(f"✅ KNN fitted for {'batters' if is_batter else 'pitchers'} "
              f"({len(features)} samples, {len(self.feature_cols)} features)")

    def get_comps(self, player: dict, n: int | None = None) -> tuple[list[dict], dict]:
        """Find k nearest historical comps for a player.

        Args:
            player: dict with keys matching feature_cols (clean names).
            n: number of comps (default: self.n_neighbors).

        Returns:
            comps: list of dicts (meta + distance + weight for each comp)
            weighted_delta: dict of {target_col: weighted average value}
        """
        if n is None:
            n = self.n_neighbors

        X_query = self._extract_query_features(player)
        X_scaled = self.scaler.transform(X_query)

        distances, indices = self.nn.kneighbors(X_scaled, n_neighbors=n)
        dists = distances[0]
        idxs = indices[0]

        weights = np.exp(-dists / 2.0)
        weight_sum = weights.sum()

        # Build comp list with metadata
        comps = []
        for i, (idx, dist, w) in enumerate(zip(idxs, dists, weights)):
            comp = dict(self._hist_meta[idx])
            comp["distance"] = float(dist)
            comp["weight"] = float(w)
            comps.append(comp)

        # Weighted average of target columns
        target_vals = self._hist_targets[idxs]  # (n, n_targets)
        weighted_delta = {}
        for j, col in enumerate(self.target_cols):
            col_vals = target_vals[:, j]
            # Skip NaN values in the weighted average
            valid = ~np.isnan(col_vals)
            if valid.any():
                weighted_delta[col] = float(
                    np.sum(col_vals[valid] * weights[valid]) / weights[valid].sum()
                )
            else:
                weighted_delta[col] = 0.0

        return comps, weighted_delta

    def predict_adjusted_projection(self, current_stats: dict,
                                    rolling_weight: float = 0.4) -> float:
        """Produce an adjusted projection blending comps + rolling stats.

        Args:
            current_stats: dict with clean column names (matching feature_cols).
            rolling_weight: how much weight to give rolling recent performance.

        Returns:
            Adjusted projection value for the primary target stat.
        """
        _comps, comp_delta = self.get_comps(current_stats)

        primary_target = self.target_cols[0]  # wrc_plus for batters, fip for pitchers
        rolling_key = "rolling_wrc_plus" if self.is_batter else "rolling_fip"

        rolling = current_stats.get(rolling_key, current_stats.get(primary_target, 100))
        base_stat = current_stats.get(primary_target, 100)
        delta = comp_delta.get(primary_target, 0.0)

        final = (1 - rolling_weight) * (base_stat + delta) + rolling_weight * rolling

        # Contextual adjustments
        month = str(current_stats.get("month", "apr")).lower()
        if month in ("sep", "oct"):
            final *= 0.96   # October fade

        age = current_stats.get("age", 25)
        pa = current_stats.get("pa", 0)
        if age is not None and age <= 24 and pa is not None and pa < 250:
            final *= 1.07   # Rookie wall adjustment

        player_name = current_stats.get("player_name", "Player")
        print(f"🎯 Adjusted projection for {player_name}: {final:.1f}")
        return final


# === Test ===
if __name__ == "__main__":
    print("🚀 Testing player fingerprinting (DuckDB version)...")
    batter_comps = PlayerComps()
    batter_comps.fit(is_batter=True)

    # Test with a sample player profile (clean column names)
    example = {
        "player_name": "Test Rookie",
        "age": 23,
        "season": 2026,
        "pa": 180,
        "wrc_plus": 98,
        "iso": 0.145,
        "k_pct": 0.27,
        "bb_pct": 0.09,
        "babip": 0.305,
        "hardhit_pct": 0.39,
        "barrel_pct": 0.085,
        "spd": 5.1,
        "rolling_wrc_plus": 105,
        "month": "apr",
        "debut_year": 2025,
        "service_time": 1,
    }
    batter_comps.predict_adjusted_projection(example)

    # Also test pitcher comps
    print("\n--- Pitcher comps ---")
    pitcher_comps = PlayerComps()
    pitcher_comps.fit(is_batter=False)

    pit_example = {
        "player_name": "Test Starter",
        "age": 27,
        "season": 2026,
        "ip": 120,
        "fip": 3.85,
        "k_pct": 0.24,
        "bb_pct": 0.07,
        "gb_pct": 0.44,
        "hardhit_pct": 0.36,
        "barrel_pct": 0.07,
        "rolling_fip": 3.60,
        "month": "jun",
        "debut_year": 2022,
        "service_time": 4,
    }
    pitcher_comps.predict_adjusted_projection(pit_example)
