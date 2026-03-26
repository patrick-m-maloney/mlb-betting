"""
Player projection engine — estimates true talent wOBA for each player.

Layer 0 (Career Average): PA-weighted wOBA from 2017-2025 game logs.
Layer 1 (KNN Comps):      PlayerComps finds historical comps from FG leaderboards,
                          produces adjusted wRC+ projection, converted to wOBA.
Layer 2 (Kalman Blend):   Placeholder — will blend KNN prior with in-season actuals.

The get_player_woba() interface stays the same regardless of active layer.

Usage:
  python src/models/projections.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import duckdb
from config.settings import GAME_BY_GAME_DIR, REFERENCE_DIR, FANGRAPHS_DIR


# Fallback if no data is available
_DEFAULT_LEAGUE_AVG_WOBA = 0.315

# wRC+ to wOBA conversion constants (derived from league averages)
# wOBA ≈ league_wOBA * (wRC+ / 100)
# This is an approximation — exact conversion needs wOBA scale + league R/PA


class ProjectionEngine:
    def __init__(self):
        print("📊 Building player projections...")
        con = duckdb.connect()

        self._load_linear_weights(con)
        self._build_woba_lookup(con)
        self._build_bbref_map(con)
        self._build_fg_crosswalk(con)
        self._load_fg_season_stats(con)

        con.close()

        # Layer 1: KNN comps
        self._init_knn_comps()

        print(f"✅ Projections ready: {len(self._player_woba):,} players (L0), "
              f"{len(self._knn_woba):,} KNN projections (L1), "
              f"league avg wOBA = {self.league_avg_woba:.3f}")

    # ------------------------------------------------------------------
    # Data loading (Layer 0 — career averages)
    # ------------------------------------------------------------------

    def _load_linear_weights(self, con):
        """Load and clean the quote-wrapped linear weights parquet."""
        lw_path = REFERENCE_DIR / "linear_weights.parquet"
        rows = con.execute(f"SELECT * FROM read_parquet('{lw_path}')").fetchall()
        col_names = [d[0].strip('"') for d in con.description]

        self._weights: dict[int, dict[str, float]] = {}
        for row in rows:
            vals = {col_names[i]: row[i] for i in range(len(col_names))}
            try:
                season = int(str(vals["Season"]).strip('"'))
                self._weights[season] = {
                    "wBB":  float(str(vals["wBB"]).strip('"')),
                    "wHBP": float(str(vals["wHBP"]).strip('"')),
                    "w1B":  float(str(vals["w1B"]).strip('"')),
                    "w2B":  float(str(vals["w2B"]).strip('"')),
                    "w3B":  float(str(vals["w3B"]).strip('"')),
                    "wHR":  float(str(vals["wHR"]).strip('"')),
                }
            except (ValueError, KeyError):
                continue
        print(f"   {len(self._weights)} seasons of linear weights loaded")

    def _build_woba_lookup(self, con):
        """Calculate PA-weighted career wOBA per player from game logs + linear weights."""
        bat_pattern = str(GAME_BY_GAME_DIR / "batting_*.parquet")

        # Insert cleaned linear weights into a temp table for the join
        con.execute("""
            CREATE TEMP TABLE _lw (
                season INT, wBB DOUBLE, wHBP DOUBLE,
                w1B DOUBLE, w2B DOUBLE, w3B DOUBLE, wHR DOUBLE
            )
        """)
        for season, w in self._weights.items():
            con.execute(
                "INSERT INTO _lw VALUES (?, ?, ?, ?, ?, ?, ?)",
                [season, w["wBB"], w["wHBP"], w["w1B"],
                 w["w2B"], w["w3B"], w["wHR"]],
            )

        results = con.execute(f"""
            SELECT
                b.mlbID,
                SUM(
                    lw.wBB  * COALESCE(b.BB,  0) +
                    lw.wHBP * COALESCE(b.HBP, 0) +
                    lw.w1B  * (COALESCE(b.H, 0) - COALESCE(b."2B", 0)
                               - COALESCE(b."3B", 0) - COALESCE(b.HR, 0)) +
                    lw.w2B  * COALESCE(b."2B", 0) +
                    lw.w3B  * COALESCE(b."3B", 0) +
                    lw.wHR  * COALESCE(b.HR,  0)
                ) / NULLIF(SUM(b.PA), 0) AS woba,
                SUM(b.PA) AS total_pa
            FROM read_parquet('{bat_pattern}') b
            JOIN _lw lw ON b.game_year = lw.season
            WHERE b.PA > 0 AND b.mlbID IS NOT NULL
            GROUP BY b.mlbID
        """).fetchall()

        self._player_woba: dict[int, float] = {}
        total_woba_num = 0.0
        total_pa = 0
        for mlb_id, woba, pa in results:
            if woba is not None:
                self._player_woba[int(mlb_id)] = woba
                total_woba_num += woba * pa
                total_pa += pa

        self.league_avg_woba = (
            total_woba_num / total_pa if total_pa > 0 else _DEFAULT_LEAGUE_AVG_WOBA
        )
        print(f"   {len(self._player_woba):,} players with career wOBA (L0)")

    def _build_bbref_map(self, con):
        """Build bbref_id → mlbID crosswalk from player_id_map."""
        id_map_path = REFERENCE_DIR / "player_id_map.parquet"
        rows = con.execute(f"""
            SELECT BREFID, CAST(MLBID AS INTEGER) AS mlb_id
            FROM read_parquet('{id_map_path}')
            WHERE BREFID IS NOT NULL AND MLBID IS NOT NULL
        """).fetchall()
        self._bbref_to_mlbid: dict[str, int] = {r[0]: r[1] for r in rows}
        print(f"   {len(self._bbref_to_mlbid):,} BBRef → mlbID mappings")

    # ------------------------------------------------------------------
    # Layer 1 — KNN comps via FanGraphs leaderboard data
    # ------------------------------------------------------------------

    def _build_fg_crosswalk(self, con):
        """Build bbref_id → idfg crosswalk from player_id_map."""
        id_map_path = REFERENCE_DIR / "player_id_map.parquet"
        rows = con.execute(f"""
            SELECT BREFID, IDFANGRAPHS
            FROM read_parquet('{id_map_path}')
            WHERE BREFID IS NOT NULL AND IDFANGRAPHS IS NOT NULL
        """).fetchall()
        self._bbref_to_idfg: dict[str, str] = {r[0]: str(r[1]) for r in rows}
        print(f"   {len(self._bbref_to_idfg):,} BBRef → IDfg mappings")

    def _load_fg_season_stats(self, con):
        """Load most recent FG season stats per player for KNN input.

        For each IDfg, keeps the most recent season with PA >= 50.
        Returns dict of idfg → {clean stat dict for PlayerComps}.
        """
        bat_pattern = str(FANGRAPHS_DIR / "batting_game_logs_*.parquet")

        rows = con.execute(f"""
            SELECT
                CAST("IDfg" AS VARCHAR) AS idfg,
                "Season" AS season,
                "Name" AS player_name,
                "Age" AS age,
                "PA" AS pa,
                "wRC+" AS wrc_plus,
                "ISO" AS iso,
                "K%" AS k_pct,
                "BB%" AS bb_pct,
                "BABIP" AS babip,
                "HardHit%" AS hardhit_pct,
                "Barrel%" AS barrel_pct,
                "Spd" AS spd,
                "wOBA" AS fg_woba
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY "IDfg"
                    ORDER BY "Season" DESC
                ) AS rn
                FROM read_parquet('{bat_pattern}')
                WHERE "PA" >= 50
                  AND "Season" >= 2015
                  AND "wRC+" IS NOT NULL
                  AND "Barrel%" IS NOT NULL
            )
            WHERE rn = 1
        """).fetchall()
        col_names = [d[0] for d in con.description]

        self._fg_stats: dict[str, dict] = {}
        for row in rows:
            d = dict(zip(col_names, row))
            idfg = d.pop("idfg")
            self._fg_stats[idfg] = d

        # Compute debut_year per player from all FG data
        debut_rows = con.execute(f"""
            SELECT CAST("IDfg" AS VARCHAR) AS idfg, MIN("Season") AS debut_year
            FROM read_parquet('{bat_pattern}')
            WHERE "PA" >= 50
            GROUP BY "IDfg"
        """).fetchall()
        debut_map = {r[0]: r[1] for r in debut_rows}

        for idfg, stats in self._fg_stats.items():
            debut = debut_map.get(idfg, stats.get("season", 2020))
            stats["debut_year"] = debut
            stats["service_time"] = stats.get("season", 2025) - debut

        print(f"   {len(self._fg_stats):,} players with FG season stats for KNN")

    def _init_knn_comps(self):
        """Fit the KNN model and compute projections for all players.

        For each player, find 15 nearest historical comps and take their
        weighted-average wOBA as the comp-based projection. Then blend
        with the player's actual most-recent wOBA:
          projected = blend_weight * comp_woba + (1 - blend_weight) * actual_woba

        This regresses extreme performers toward what similar players
        typically produce, while still respecting their actual output.
        """
        from src.features.player_fingerprinting import PlayerComps

        self._comps = PlayerComps(n_neighbors=15)
        self._comps.fit(is_batter=True)

        # Pre-compute KNN-adjusted wOBA for every player we have FG stats for
        self._knn_woba: dict[str, float] = {}  # bbref_id → projected wOBA

        # How much to weight the comp average vs the player's actual stats.
        # Higher = more regression toward comps (good early season / small samples).
        # Lower  = trust the player's actual performance more.
        comp_blend_weight = 0.35

        for bbref_id, idfg in self._bbref_to_idfg.items():
            if idfg not in self._fg_stats:
                continue

            stats = self._fg_stats[idfg]
            fg_woba = stats.get("fg_woba")
            wrc_plus = stats.get("wrc_plus")

            if wrc_plus is None or fg_woba is None:
                continue

            try:
                # Get comps: comp_avg is weighted average of comp stat values
                _comps, comp_avg = self._comps.get_comps(stats)

                # Convert comp average wRC+ to wOBA using league relationship:
                # wOBA ≈ league_avg_woba * (wRC+ / 100)
                comp_wrc_plus = comp_avg.get("wrc_plus")
                if comp_wrc_plus is None:
                    continue
                comp_woba = self.league_avg_woba * (comp_wrc_plus / 100.0)

                # Blend: actual wOBA + comp-based wOBA
                projected_woba = (
                    (1 - comp_blend_weight) * fg_woba
                    + comp_blend_weight * comp_woba
                )

                # Clamp to reasonable range
                projected_woba = max(0.200, min(0.500, projected_woba))
                self._knn_woba[bbref_id] = projected_woba
            except Exception:
                # Silently skip players where KNN fails
                continue

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_player_woba(self, bbref_id: str) -> float:
        """Best current talent estimate (wOBA) for a player.

        Priority: Layer 1 (KNN) → Layer 0 (career avg) → league average.
        """
        if not bbref_id:
            return self.league_avg_woba

        # Layer 1: KNN comp-based projection
        if bbref_id in self._knn_woba:
            return self._knn_woba[bbref_id]

        # Layer 0: career PA-weighted average
        mlb_id = self._bbref_to_mlbid.get(bbref_id)
        if mlb_id is not None and mlb_id in self._player_woba:
            return self._player_woba[mlb_id]

        return self.league_avg_woba

    def get_lineup_wobas(self, bbref_ids: list) -> list[float]:
        """Get wOBA for each player in a lineup (in batting-order)."""
        return [self.get_player_woba(bid) for bid in (bbref_ids or [])]


if __name__ == "__main__":
    engine = ProjectionEngine()
    # Quick test: look up known players and show which layer is used
    test_ids = ["bettsmo01", "troutmi01", "julgery01", "ohtansh01"]
    print("\nPlayer projections:")
    for bid in test_ids:
        w = engine.get_player_woba(bid)
        layer = "L1-KNN" if bid in engine._knn_woba else "L0-career"
        print(f"  {bid}: wOBA = {w:.3f} ({layer})")
