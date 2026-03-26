"""
Edge detection: compare model win probabilities to market odds.

Normalizes all odds sources to implied win probability:
  - The Odds API: American odds → probability
  - Kalshi: yes_bid / yes_ask midpoint
  - Polymarket: yes_price (already 0-1)

Flags +EV opportunities where model disagrees with market.

Usage:
  python src/models/edge_detection.py               # today's edges
  python src/models/edge_detection.py 2026-03-17     # specific date
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import glob
import duckdb
from datetime import datetime, timezone

from config.settings import BRONZE_ODDS_DIR, SIMULATIONS_DIR


# ========================= TEAM NAME MAP =========================

TEAM_ABBREV_TO_FULL: dict[str, str] = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC":  "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD":  "San Diego Padres",
    "SF":  "San Francisco Giants",
    "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals",
    "TB":  "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}

TEAM_FULL_TO_ABBREV: dict[str, str] = {v: k for k, v in TEAM_ABBREV_TO_FULL.items()}


# ========================= CONVERSIONS =========================

def american_to_implied(odds: float) -> float | None:
    """Convert American odds to implied probability (no-vig raw)."""
    if odds is None:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    elif odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 0.5


def kalshi_to_implied(yes_bid: float, yes_ask: float) -> float | None:
    """Kalshi midpoint of yes_bid/yes_ask as implied probability."""
    if yes_bid is None or yes_ask is None:
        return None
    return (yes_bid + yes_ask) / 2.0


# ========================= DATA LOADERS =========================

def _load_sim_results(game_date: str) -> list[dict]:
    """Load the most recent simulation results file."""
    files = sorted(
        glob.glob(str(SIMULATIONS_DIR / "simulation_results_*.parquet")),
        reverse=True,
    )
    if not files:
        return []

    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT * FROM read_parquet('{files[0]}')
        WHERE game_date = '{game_date}'
    """).fetchall()
    cols = [d[0] for d in con.description]
    con.close()
    return [dict(zip(cols, row)) for row in rows]


def _load_odds_api(game_date: str, year: str) -> dict:
    """Load h2h odds → {(away_abbrev, home_abbrev): {book: {away_prob, home_prob}}}."""
    path = BRONZE_ODDS_DIR / f"the_odds_api_{year}.parquet"
    if not path.exists():
        return {}

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT home_team, away_team, bookmaker, outcome_name, odds
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY home_team, away_team, bookmaker, outcome_name
                    ORDER BY snapshot_timestamp DESC
                ) AS rn
                FROM read_parquet('{path}')
                WHERE market = 'h2h'
                  AND snapshot_date = '{game_date}'
            )
            WHERE rn = 1
        """).fetchall()
    except Exception:
        rows = []
    finally:
        con.close()

    games: dict = {}
    for home_full, away_full, bookmaker, outcome_name, odds_val in rows:
        if not home_full or not away_full:
            continue
        away_abbr = TEAM_FULL_TO_ABBREV.get(away_full, away_full)
        home_abbr = TEAM_FULL_TO_ABBREV.get(home_full, home_full)
        key = (away_abbr, home_abbr)

        if key not in games:
            games[key] = {}
        if bookmaker not in games[key]:
            games[key][bookmaker] = {}

        implied = american_to_implied(odds_val)
        team_abbr = TEAM_FULL_TO_ABBREV.get(outcome_name, outcome_name)
        if team_abbr == away_abbr:
            games[key][bookmaker]["away_prob"] = implied
        elif team_abbr == home_abbr:
            games[key][bookmaker]["home_prob"] = implied

    return games


def _load_kalshi(game_date: str, year: str) -> dict:
    """Load Kalshi game_winner → {(away, home): {away_prob, home_prob}}."""
    path = BRONZE_ODDS_DIR / f"kalshi_{year}.parquet"
    if not path.exists():
        return {}

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT away_team, home_team, winner_side, yes_bid, yes_ask
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY away_team, home_team, winner_side
                    ORDER BY snapshot_timestamp DESC
                ) AS rn
                FROM read_parquet('{path}')
                WHERE market_type = 'game_winner'
                  AND game_date = '{game_date}'
                  AND status = 'active'
                  AND (yes_ask - yes_bid) < 0.40
            )
            WHERE rn = 1
        """).fetchall()
    except Exception:
        rows = []
    finally:
        con.close()

    games: dict = {}
    for away, home, winner_side, yes_bid, yes_ask in rows:
        key = (away, home)
        if key not in games:
            games[key] = {}
        implied = kalshi_to_implied(yes_bid, yes_ask)
        if implied is None or implied < 0.10 or implied > 0.90:
            continue
        if winner_side == away:
            games[key]["away_prob"] = implied
        elif winner_side == home:
            games[key]["home_prob"] = implied

    return games


def _load_polymarket(game_date: str, year: str) -> dict:
    """Load Polymarket moneyline → {(away_abbrev, home_abbrev): {away_prob, home_prob}}."""
    path = BRONZE_ODDS_DIR / f"polymarket_{year}.parquet"
    if not path.exists():
        return {}

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT away_team, home_team, yes_price, no_price
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY away_team, home_team
                    ORDER BY snapshot_timestamp DESC
                ) AS rn
                FROM read_parquet('{path}')
                WHERE market_type = 'game_winner'
                  AND group_item_title = 'moneyline'
                  AND event_date = '{game_date}'
                  AND accepting_orders = TRUE
                  AND yes_price > 0.05 AND yes_price < 0.95
            )
            WHERE rn = 1
        """).fetchall()
    except Exception:
        rows = []
    finally:
        con.close()

    games: dict = {}
    for away_full, home_full, yes_price, no_price in rows:
        if not away_full or not home_full:
            continue
        away_abbr = TEAM_FULL_TO_ABBREV.get(away_full, away_full)
        home_abbr = TEAM_FULL_TO_ABBREV.get(home_full, home_full)
        key = (away_abbr, home_abbr)
        # yes_price = outcomePrices[0] = away team probability
        # no_price  = outcomePrices[1] = home team probability
        games[key] = {
            "away_prob": yes_price if yes_price else None,
            "home_prob": no_price if no_price else None,
        }

    return games


# ========================= EDGE DETECTION =========================

def find_edges(game_date: str | None = None, min_edge: float = 0.03) -> list[dict]:
    """Compare model win probabilities to all market odds sources.

    Args:
        game_date: YYYY-MM-DD (default: today UTC)
        min_edge:  minimum |model_prob - market_prob| to flag (default 3%)

    Returns: list of edge dicts, sorted by |edge| descending.
    """
    if game_date is None:
        game_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    year = game_date[:4]

    sim_results = _load_sim_results(game_date)
    if not sim_results:
        print(f"⚠️  No simulation results for {game_date}")
        return []

    odds_api = _load_odds_api(game_date, year)
    kalshi = _load_kalshi(game_date, year)
    polymarket = _load_polymarket(game_date, year)

    print(f"\n📊 Edge detection for {game_date}")
    print(f"   Model: {len(sim_results)} games")
    print(f"   Odds API: {len(odds_api)} games, "
          f"Kalshi: {len(kalshi)} games, "
          f"Polymarket: {len(polymarket)} games")

    edges: list[dict] = []
    comparisons: list[dict] = []

    for sim in sim_results:
        away = sim["away_team"]
        home = sim["home_team"]
        model_away = sim["away_win_prob"]
        model_home = sim["home_win_prob"]
        key = (away, home)

        row = {
            "away_team": away,
            "home_team": home,
            "model_away": model_away,
            "model_home": model_home,
        }

        def _check_edge(source: str, side: str, model_prob: float, mkt_prob: float | None):
            if mkt_prob is None:
                return
            edge = model_prob - mkt_prob
            if abs(edge) >= min_edge:
                edges.append({
                    "game_date": game_date,
                    "away_team": away,
                    "home_team": home,
                    "source": source,
                    "side": side,
                    "model_prob": round(model_prob, 4),
                    "market_prob": round(mkt_prob, 4),
                    "edge": round(edge, 4),
                })

        # The Odds API (multiple bookmakers)
        if key in odds_api:
            for book, probs in odds_api[key].items():
                a = probs.get("away_prob")
                h = probs.get("home_prob")
                if a is not None:
                    row[f"{book}_away"] = round(a, 4)
                if h is not None:
                    row[f"{book}_home"] = round(h, 4)
                _check_edge(book, away, model_away, a)
                _check_edge(book, home, model_home, h)

        # Kalshi
        if key in kalshi:
            a = kalshi[key].get("away_prob")
            h = kalshi[key].get("home_prob")
            if a is not None:
                row["kalshi_away"] = round(a, 4)
            if h is not None:
                row["kalshi_home"] = round(h, 4)
            _check_edge("kalshi", away, model_away, a)
            _check_edge("kalshi", home, model_home, h)

        # Polymarket
        if key in polymarket:
            a = polymarket[key].get("away_prob")
            h = polymarket[key].get("home_prob")
            if a is not None:
                row["polymarket_away"] = round(a, 4)
            if h is not None:
                row["polymarket_home"] = round(h, 4)
            _check_edge("polymarket", away, model_away, a)
            _check_edge("polymarket", home, model_home, h)

        comparisons.append(row)

    # ── Print comparison table ──
    print(f"\n{'Away':<6} {'Home':<6} {'Model':>7}  ", end="")
    has_books = any(k for row in comparisons for k in row if "_away" in k and "model" not in k and "kalshi" not in k and "polymarket" not in k)
    if has_books:
        print(f"{'Book':>7}  ", end="")
    print(f"{'Kalshi':>7}  {'Poly':>7}")
    print("-" * 55)

    for row in comparisons:
        model_str = f"{row['model_away']:.1%}"

        # Pick first available book line
        book_str = "   -  "
        for k, v in row.items():
            if "_away" in k and k not in ("model_away", "kalshi_away", "polymarket_away"):
                book_str = f"{v:.1%}"
                break

        kalshi_str = f"{row['kalshi_away']:.1%}" if "kalshi_away" in row else "   -  "
        poly_str = f"{row['polymarket_away']:.1%}" if "polymarket_away" in row else "   -  "

        print(f"{row['away_team']:<6} {row['home_team']:<6} {model_str:>7}  ", end="")
        if has_books:
            print(f"{book_str:>7}  ", end="")
        print(f"{kalshi_str:>7}  {poly_str:>7}")

    # ── Print edges ──
    edges.sort(key=lambda e: abs(e["edge"]), reverse=True)
    if edges:
        print(f"\n🎯 {len(edges)} edges found (>{min_edge:.0%} threshold):")
        for e in edges[:15]:
            direction = "OVER" if e["edge"] > 0 else "UNDER"
            print(
                f"  {e['side']:<5} ({e['away_team']}@{e['home_team']}): "
                f"model={e['model_prob']:.1%} vs {e['source']}={e['market_prob']:.1%} → "
                f"{direction} {abs(e['edge']):.1%}"
            )
    else:
        print(f"\nNo edges above {min_edge:.0%} threshold.")

    return edges


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    find_edges(date_arg)
