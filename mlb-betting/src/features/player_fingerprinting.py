import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from pybaseball import batting_stats, pitching_stats
import warnings
warnings.filterwarnings("ignore")

class PlayerComps:
    def __init__(self, n_neighbors=15):
        self.n_neighbors = n_neighbors
        self.scaler = StandardScaler()
        self.nn = NearestNeighbors(n_neighbors=n_neighbors, metric="euclidean")
        self.historical_df = None
        self.is_batter = None
        self.feature_cols = None
        self.target_cols = None
        self.cache_path = Path("data/raw/stats/comps_cache.parquet")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Make EVERY column lowercase so everything is consistent."""
        df = df.copy()
        df.columns = [col.lower() if isinstance(col, str) else col for col in df.columns]
        return df

    def _get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        base = self._normalize_columns(df)
        base["age"] = base.get("age", 25.0)
        base["service_time"] = base.get("season", 2026) - base.get("debut_year", 2020)
        
        if self.is_batter:
            self.feature_cols = ["age", "service_time", "pa", "wrc+", "iso", "k%", "bb%", "babip", 
                                 "hardhit%", "barrel%", "spd"]
            self.target_cols = ["wrc+", "iso", "k%", "bb%", "babip", "hardhit%", "barrel%"]
        else:
            self.feature_cols = ["age", "service_time", "ip", "fip", "k%", "bb%", "gb%", "hardhit%", "barrel%"]
            self.target_cols = ["fip", "k%", "bb%", "gb%", "hardhit%", "barrel%"]
        
        X = base[self.feature_cols].copy()
        for col in ["bats", "throws"]:
            if col in X.columns:
                X = pd.get_dummies(X, columns=[col], dummy_na=True)
        return X.fillna(X.median())

    def build_historical_data(self, start_year=2015, end_year=2025):
        print("📥 Building historical comps database (only once)...")
        bat = batting_stats(start_season=start_year, end_season=end_year, qual=50)
        bat = bat.rename(columns={"Name": "player_name", "IDfg": "player_id", "Season": "season"})
        bat["is_batter"] = True
        bat["debut_year"] = bat.groupby("player_name")["season"].transform("min")
        
        pit = pitching_stats(start_season=start_year, end_season=end_year, qual=50)
        pit = pit.rename(columns={"Name": "player_name", "IDfg": "player_id", "Season": "season"})
        pit["is_batter"] = False
        pit["debut_year"] = pit.groupby("player_name")["season"].transform("min")
        
        self.historical_df = pd.concat([bat, pit], ignore_index=True)
        self.historical_df = self._normalize_columns(self.historical_df)   # ← CRITICAL FIX
        self.historical_df.to_parquet(self.cache_path)
        print(f"✅ Saved {len(self.historical_df)} player-seasons to cache.")

    def fit(self, is_batter=True):
        self.is_batter = is_batter
        if self.historical_df is None:
            if self.cache_path.exists():
                self.historical_df = pd.read_parquet(self.cache_path)
                self.historical_df = self._normalize_columns(self.historical_df)  # ← CRITICAL FIX
                print("✅ Loaded & normalized cache.")
            else:
                self.build_historical_data()
        
        mask = self.historical_df["is_batter"] == is_batter
        X = self._get_features(self.historical_df[mask])
        X_scaled = self.scaler.fit_transform(X)
        self.nn.fit(X_scaled)
        print(f"✅ KNN fitted for {'batters' if is_batter else 'pitchers'} ({len(X)} samples)")

    def get_comps(self, player_row: pd.Series, n=None):
        if n is None:
            n = self.n_neighbors
        X_query = self._get_features(pd.DataFrame([player_row]))
        X_scaled = self.scaler.transform(X_query)
        
        distances, indices = self.nn.kneighbors(X_scaled)
        comps = self.historical_df.iloc[indices[0]].copy()
        comps["distance"] = distances[0]
        comps["weight"] = np.exp(-comps["distance"] / 2)
        
        weighted_delta = (comps[self.target_cols].multiply(comps["weight"], axis=0)).sum() / comps["weight"].sum()
        
        return comps, weighted_delta

    def predict_adjusted_projection(self, current_stats: pd.Series, rolling_weight=0.4):
        comps_df, comp_delta = self.get_comps(current_stats)
        
        rolling = current_stats.get("rolling_wrc+", current_stats.get("wrc+", 100))
        base_stat = current_stats.get(self.target_cols[0], 100)
        final = (1 - rolling_weight) * (base_stat + comp_delta.iloc[0]) + rolling_weight * rolling
        
        # Your requested flags
        month = current_stats.get("month", "apr").lower()
        if month in ["sep", "oct"]:
            final *= 0.96   # October fade
        if current_stats.get("age", 25) <= 24 and current_stats.get("pa", 0) < 250:
            final *= 1.07   # Rookie wall adjustment
        
        print(f"🎯 Adjusted projection for {current_stats.get('player_name', 'Player')}: {final:.1f}")
        return final

# === Test ===
if __name__ == "__main__":
    print("🚀 Testing player fingerprinting (final version)...")
    batter_comps = PlayerComps()
    batter_comps.fit(is_batter=True)
    
    example = pd.Series({
        "player_name": "Test Rookie",
        "age": 23,
        "season": 2026,
        "pa": 180,
        "wrc+": 98,
        "iso": 0.145,
        "k%": 0.27,
        "bb%": 0.09,
        "babip": 0.305,
        "hardhit%": 0.39,
        "barrel%": 0.085,
        "spd": 5.1,
        "rolling_wrc+": 105,
        "month": "apr"
    })
    batter_comps.predict_adjusted_projection(example)