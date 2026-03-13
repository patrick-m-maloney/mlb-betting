# src/data_ingestion/rundown_fetcher.py
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from config.settings import RAW_ODDS_PATH, THE_RUNDOWN_API_KEY

BASE_URL = "https://api.therundown.io/api/v2"

def fetch_rundown_mlb(date_str: str = None) -> pd.DataFrame | None:
    if not THE_RUNDOWN_API_KEY or THE_RUNDOWN_API_KEY.strip() == "":
        print("⚠️  TheRundown skipped (no API key set — activate when ready)")
        return None

    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    url = f"{BASE_URL}/sports/3/events/{date_str}"  # 3 = MLB
    headers = {"X-TheRundown-Key": THE_RUNDOWN_API_KEY}
    
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 401:
            print("⚠️  TheRundown skipped (401 — account not activated yet)")
            return None
        resp.raise_for_status()
        
        data = resp.json().get("events", [])
        df = pd.json_normalize(data, sep="_")
        df["fetch_timestamp"] = datetime.now(timezone.utc)
        df["source"] = "TheRundown"
        save_snapshot(df, "rundown")
        return df
    except Exception as e:
        print(f"⚠️  TheRundown skipped: {e}")
        return None

def save_snapshot(df: pd.DataFrame, subfolder: str):
    if df is None or df.empty:
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = RAW_ODDS_PATH / date_str / subfolder
    path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    df.to_parquet(path / f"odds_{ts}.parquet", compression="snappy")
    print(f"✅ Saved TheRundown snapshot → {path}/odds_{ts}.parquet")