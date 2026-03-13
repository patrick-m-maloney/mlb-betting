# import requests
# import pandas as pd
# from datetime import datetime, timezone
# from pathlib import Path
# from config.settings import RAW_ODDS_PATH

# def fetch_kalshi_mlb() -> pd.DataFrame | None:
#     """Public Kalshi markets (no key needed). Filters for any MLB/Pro Baseball markets."""
#     url = "https://api.elections.kalshi.com/trade-api/v2/markets?status=open&limit=1000"
#     try:
#         resp = requests.get(url, timeout=15)
#         resp.raise_for_status()
#         data = resp.json().get("markets", [])
#         if not data:
#             print("⚠️  Kalshi: no markets returned")
#             return None

#         df = pd.json_normalize(data)
#         # Filter to MLB-related markets (mostly futures/awards/season stats in 2026)
#         mlb_mask = df.get("title", "").str.contains("MLB|baseball|Pro Baseball", case=False, na=False)
#         df = df[mlb_mask]

#         if df.empty:
#             print("⚠️  No active MLB markets on Kalshi right now")
#             return None

#         df["fetch_timestamp"] = datetime.now(timezone.utc)
#         df["source"] = "Kalshi"
#         save_snapshot(df, "kalshi")
#         print(f"✅ Kalshi: saved {len(df)} MLB markets")
#         return df
#     except Exception as e:
#         print(f"⚠️  Kalshi skipped: {e}")
#         return None

# def save_snapshot(df: pd.DataFrame, subfolder: str):
#     if df is None or df.empty:
#         return
#     date_str = datetime.now().strftime("%Y-%m-%d")
#     path = RAW_ODDS_PATH / date_str / subfolder
#     path.mkdir(parents=True, exist_ok=True)
#     ts = datetime.now().strftime("%H%M%S")
#     df.to_parquet(path / f"markets_{ts}.parquet", compression="snappy")
#     print(f"   → Saved to {path}/markets_{ts}.parquet")

import requests
import pandas as pd
from datetime import datetime, timezone
from src.database.db_manager import append_to_table

def fetch_kalshi_mlb() -> pd.DataFrame | None:
    url = "https://api.elections.kalshi.com/trade-api/v2/markets?status=open&limit=1000"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("markets", [])
        df = pd.json_normalize(data)
        
        # Broad MLB filter (all you see on the site + futures)
        keywords = ["MLB", "baseball", "Pro Baseball", "World Series", "MVP", "Cy Young"]
        mask = pd.Series([False] * len(df))
        for col in ['title', 'subtitle', 'description', 'category', 'event_title']:
            if col in df.columns:
                mask |= df[col].astype(str).str.contains('|'.join(keywords), case=False, na=False)
        df = df[mask]
        
        if df.empty:
            print("⚠️  Kalshi: no MLB markets right now")
            return None
        
        df["source"] = "Kalshi"
        append_to_table(df, "raw_kalshi")
        print(f"✅ Kalshi: appended {len(df)} MLB markets")
        return df
    except Exception as e:
        print(f"⚠️  Kalshi skipped: {e}")
        return None

if __name__ == "__main__":
    fetch_kalshi_mlb()