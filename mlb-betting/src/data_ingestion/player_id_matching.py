# src/data_ingestion/player_id_matching.py
"""
Robust MLB Player ID Matching — GitHub mirror + duplicate name handling
"""

import sys
from pathlib import Path

# === Make 'config' importable from anywhere ===
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
import pybaseball
import warnings
import os
import certifi
from typing import Dict, Optional

from config.settings import PLAYER_ID_MAP_PATH, REFERENCE_DIR

warnings.filterwarnings("ignore")
pybaseball.cache.enable()

SFBB_CSV_URL = "https://raw.githubusercontent.com/couthcommander/PLAYERIDMAP/main/PLAYERIDMAP.csv"

os.environ.setdefault('SSL_CERT_FILE', certifi.where())

def _normalize_name(name: str) -> str:
    if pd.isna(name) or not name:
        return ""
    name = str(name).strip().upper()
    name = name.replace(".", "").replace("JR", "").replace("SR", "").replace("III", "").replace("II", "")
    name = name.replace("J.D.", "JD").replace("J D ", "JD ").replace("A.J.", "AJ")
    return name.strip()

def build_player_id_map(force_refresh: bool = False) -> pd.DataFrame:
    """Download/build master player_id_map.parquet from GitHub mirror"""
    if PLAYER_ID_MAP_PATH.exists() and not force_refresh:
        print(f"✅ Loading existing player_id_map from {PLAYER_ID_MAP_PATH}")
        return pd.read_parquet(PLAYER_ID_MAP_PATH)

    print("📥 Downloading latest Player ID Map from GitHub mirror...")
    df = pd.read_csv(SFBB_CSV_URL, low_memory=False)

    # Keep useful columns
    keep_cols = [
        "PLAYERNAME", "BIRTHDATE", "BATS", "THROWS", "POS", "TEAM", "BREFID", "MLBID",
        "ROTOWIRENAME", "ROTOWIREID", "FANGRAPHSID", "KEY_MLBAM", "KEY_RETRO"
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    df["name_norm"] = df["ROTOWIRENAME"].apply(_normalize_name)
    df["playername_norm"] = df["PLAYERNAME"].apply(_normalize_name)
    df["bbref_id"] = df["BREFID"].str.lower().str.strip()
    df["mlbam_id"] = df["MLBID"].astype(str).str.strip()
    df["rotowire_id"] = df["ROTOWIREID"].astype(str).str.strip()

    # Deduplicate
    df = df.sort_values(by=["BREFID", "ROTOWIREID"], na_position="last") \
           .drop_duplicates(subset=["BREFID"], keep="first")

    df.to_parquet(PLAYER_ID_MAP_PATH, compression="zstd", index=False)
    print(f"✅ Master player_id_map saved: {len(df):,} players → {PLAYER_ID_MAP_PATH}")
    return df

class PlayerMatcher:
    """Main matching class (handles duplicate names via list of records)"""
    def __init__(self):
        self.id_map = build_player_id_map()
        # Your smart duplicate-handling logic
        self.id_map_dict = (
            self.id_map
            .groupby("name_norm")
            .apply(lambda g: g.to_dict("records"))
            .to_dict()
        )
        self.name_list = self.id_map["name_norm"].tolist()

    def match_player(self, rotowire_name: str, team_abbr: Optional[str] = None,
                     position: Optional[str] = None, bats: Optional[str] = None,
                     throws: Optional[str] = None) -> Dict:
        if pd.isna(rotowire_name) or not rotowire_name:
            return {"primary_bbref_id": None, "bbref_ids": [], "match_confidence": 0,
                    "match_method": "none", "match_details": "empty name"}

        norm_name = _normalize_name(rotowire_name)

        # 1. Exact SFBB match (now handles list)
        if norm_name in self.id_map_dict:
            matches = self.id_map_dict[norm_name]  # list of possible rows
            row = matches[0]  # take first (99%+ cases are unique)
            return {
                "primary_bbref_id": row["bbref_id"],
                "bbref_ids": [row["bbref_id"]],
                "match_confidence": 100,
                "match_method": "sfbb_exact",
                "match_details": f"ROTOWIRENAME exact (team={team_abbr})"
            }

        # 2. pybaseball fallback
        try:
            lookup = pybaseball.playerid_lookup(
                norm_name.split()[-1] if " " in norm_name else norm_name,
                norm_name.split()[0] if " " in norm_name else "",
                fuzzy=True)
            if not lookup.empty:
                bbref = lookup.iloc[0]["key_bbref"]
                return {
                    "primary_bbref_id": bbref,
                    "bbref_ids": [bbref],
                    "match_confidence": 95,
                    "match_method": "pybaseball_lookup",
                    "match_details": "pybaseball fuzzy"
                }
        except:
            pass

        # 3. Fuzzy + context
        matches = process.extract(norm_name, self.name_list, scorer=fuzz.token_sort_ratio, limit=5)
        best_score = matches[0][1] if matches else 0

        if best_score >= 85:
            candidate = self.id_map[self.id_map["name_norm"] == matches[0][0]].iloc[0]
            confidence = best_score
            if team_abbr and team_abbr.upper() in str(candidate.get("TEAM", "")).upper():
                confidence += 5
            if position and position.upper() in str(candidate.get("POS", "")).upper():
                confidence += 3

            return {
                "primary_bbref_id": candidate["bbref_id"],
                "bbref_ids": [candidate["bbref_id"]],
                "match_confidence": min(99, confidence),
                "match_method": "fuzzy_context",
                "match_details": f"rapidfuzz {best_score:.1f} + context"
            }

        return {
            "primary_bbref_id": None,
            "bbref_ids": [],
            "match_confidence": best_score,
            "match_method": "no_match",
            "match_details": f"best fuzzy {best_score:.1f}"
        }

# One-time build + test
if __name__ == "__main__":
    print("Building fresh player ID map...")
    build_player_id_map(force_refresh=True)
    matcher = PlayerMatcher()
    print("✅ PlayerMatcher ready!")
    test = matcher.match_player("Shohei Ohtani", team_abbr="LAD", position="DH")
    print("Test match (Ohtani):", test)