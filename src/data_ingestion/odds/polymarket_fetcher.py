import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from config.settings import RAW_ODDS_PATH

def fetch_polymarket_mlb() -> pd.DataFrame | None:
    """Public Polymarket Gamma API (no key needed). Filters for MLB markets."""
    url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1000"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()  # list of markets
        if not data:
            print("⚠️  Polymarket: no markets returned")
            return None

        df = pd.json_normalize(data)
        # Filter to MLB-related (games, futures, awards, etc.)
        mlb_mask = df.get("question", "").str.contains("MLB|baseball", case=False, na=False) | \
                   df.get("tags", "").astype(str).str.contains("MLB|baseball", case=False, na=False)
        df = df[mlb_mask]

        if df.empty:
            print("⚠️  No active MLB markets on Polymarket right now")
            return None

        df["fetch_timestamp"] = datetime.now(timezone.utc)
        df["source"] = "Polymarket"
        save_snapshot(df, "polymarket")
        print(f"✅ Polymarket: saved {len(df)} MLB markets")
        return df
    except Exception as e:
        print(f"⚠️  Polymarket skipped: {e}")
        return None

def save_snapshot(df: pd.DataFrame, subfolder: str):
    if df is None or df.empty:
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = RAW_ODDS_PATH / date_str / subfolder
    path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    df.to_parquet(path / f"markets_{ts}.parquet", compression="snappy")
    print(f"   → Saved to {path}/markets_{ts}.parquet")