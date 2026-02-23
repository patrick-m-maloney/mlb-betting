import sys
from pathlib import Path

# === PATH FIX (so config/ is always found) ===
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import time
from datetime import datetime
import pandas as pd # type: ignore
import requests     # type: ignore
from config.settings import THE_ODDS_API_KEY, BASE_URL, SPORT, RAW_ODDS_PATH

def normalize_odds_response(data: list, fetch_timestamp: datetime) -> pd.DataFrame:
    rows = []
    for game in data:
        game_id = game["id"]
        commence_time = game["commence_time"]
        home = game["home_team"]
        away = game["away_team"]
        
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

def fetch_current_odds() -> pd.DataFrame | None:
    url = f"{BASE_URL}/sports/{SPORT}/odds"
    params = {
        "apiKey": THE_ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,spreads,totals",   # ← removed outrights (this fixes the 422)
        # "markets": "h2h,spreads,totals,outrights",
        "oddsFormat": "american",
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"❌ Error {resp.status_code}: {resp.text}")
        return None
    
    print(f"✅ Credits used: {resp.headers.get('x-requests-last')}")
    return normalize_odds_response(resp.json(), datetime.utcnow())

def save_snapshot(df: pd.DataFrame):
    if df.empty:
        return
    date_str = df["fetch_timestamp"].iloc[0].strftime("%Y-%m-%d")
    path = RAW_ODDS_PATH / date_str
    path.mkdir(parents=True, exist_ok=True)
    filename = path / f"odds_{datetime.utcnow().strftime('%H%M%S')}.parquet"
    df.to_parquet(filename, compression="snappy")
    print(f"✅ Saved {len(df)} rows → {filename}")

if __name__ == "__main__":
    print("🚀 Fetching current MLB odds (preseason/futures included)...")
    df = fetch_current_odds()
    if df is not None:
        print(df.head())
        save_snapshot(df)
        print("🎉 Success! Check data/raw/odds/ folder")