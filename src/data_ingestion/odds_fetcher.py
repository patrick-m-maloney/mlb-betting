import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timezone
import pandas as pd
import requests
from config.settings import THE_ODDS_API_KEY, BASE_URL, RAW_ODDS_PATH

def normalize_odds_response(data: list, fetch_timestamp: datetime) -> pd.DataFrame:
    rows = []
    for game in data:
        game_id = game.get("id")
        commence_time = game.get("commence_time")
        home = game.get("home_team")
        away = game.get("away_team")
        
        for book in game.get("bookmakers", []):
            bookmaker = book["key"]
            last_update = book["last_update"]
            
            for market in book.get("markets", []):
                mkt_key = market["key"]
                for outcome in market.get("outcomes", []):
                    row = {
                        "fetch_timestamp": fetch_timestamp,
                        "game_id": game_id,
                        "commence_time": commence_time,
                        "home_team": home,
                        "away_team": away,
                        "bookmaker": bookmaker,
                        "market": mkt_key,
                        "outcome_name": outcome["name"],
                        "odds": outcome["price"],
                        "point": outcome.get("point"),
                        "last_update": last_update,
                    }
                    rows.append(row)
    return pd.DataFrame(rows)

def fetch_odds(sport_key: str, markets: str) -> pd.DataFrame | None:
    url = f"{BASE_URL}/sports/{sport_key}/odds"
    params = {
        "apiKey": THE_ODDS_API_KEY,
        "regions": "us",
        "markets": markets,
        "oddsFormat": "american",
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"❌ Error {resp.status_code} for {sport_key}: {resp.text}")
        return None
    
    print(f"✅ {sport_key} → Credits used: {resp.headers.get('x-requests-last')}")
    return normalize_odds_response(resp.json(), datetime.now(timezone.utc))

def fetch_futures() -> pd.DataFrame | None:
    """Dedicated futures (season win totals, World Series, division winners, etc.)"""
    futures_sports = [
        "baseball_mlb_world_series_winner",
        "baseball_mlb_al_winner",
        "baseball_mlb_nl_winner",
        # add more as needed: al_east_winner, etc.
    ]
    all_futures = []
    for sport in futures_sports:
        df = fetch_odds(sport, "outrights")
        if df is not None and not df.empty:
            all_futures.append(df)
    if all_futures:
        return pd.concat(all_futures, ignore_index=True)
    return None

def save_snapshot(df: pd.DataFrame, subfolder: str = "games"):
    if df.empty:
        return
    date_str = df["fetch_timestamp"].iloc[0].strftime("%Y-%m-%d")
    path = RAW_ODDS_PATH / date_str / subfolder
    path.mkdir(parents=True, exist_ok=True)
    filename = path / f"odds_{datetime.now(timezone.utc).strftime('%H%M%S')}.parquet"
    df.to_parquet(filename, compression="snappy")
    print(f"✅ Saved {len(df)} rows → {filename}")

if __name__ == "__main__":
    print("🚀 Fetching MLB odds (robust preseason/in-season/futures mode)...")
    
    # 1. Regular + Spring Training games
    for sport in ["baseball_mlb", "baseball_mlb_preseason"]:
        df_games = fetch_odds(sport, "h2h,spreads,totals")
        if df_games is not None:
            save_snapshot(df_games, "games")
    
    # 2. Futures (your win totals / division futures)
    df_futures = fetch_futures()
    if df_futures is not None:
        save_snapshot(df_futures, "futures")
    
    print("🎉 Run complete! Check data/raw/odds/")