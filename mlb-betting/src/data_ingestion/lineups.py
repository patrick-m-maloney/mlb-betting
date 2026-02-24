import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re

RAW_LINEUPS_PATH = Path("data/raw/lineups")
RAW_LINEUPS_PATH.mkdir(parents=True, exist_ok=True)

def fetch_lineups(live=True, test_html=None) -> pd.DataFrame | None:
    fetch_timestamp = datetime.utcnow()
    
    if not live and test_html:
        with open(test_html, "r", encoding="utf-8") as f:
            html = f.read()
        print(f"✅ Parsing local test HTML: {test_html}")
    else:
        url = "https://www.rotowire.com/baseball/daily-lineups.php"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            print(f"❌ HTTP {resp.status_code} from Rotowire")
            return None
        html = resp.text
        print("✅ Fetched live Rotowire daily lineups")

    return parse_rotowire(html, fetch_timestamp)

def parse_float(text):
    if not text or text.strip() in ["–", "-", ""]:
        return None
    try:
        # Extract number like "11.5" from "11.5 Runs"
        num = re.search(r'[-+]?\d*\.?\d+', text.strip())
        return float(num.group(0)) if num else None
    except:
        return None

def parse_rotowire(html: str, fetch_timestamp: datetime) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    games = []

    main_container = soup.find("div", class_="lineups")
    if not main_container:
        print("❌ Could not find main .lineups container")
        return pd.DataFrame()

    lineup_divs = main_container.find_all("div", class_=re.compile(r"lineup.*is-mlb"))

    for div in lineup_divs:
        try:
            classes = " ".join(div.get("class", []))
            if any(skip in classes for skip in ["is-tools", "hide-until", "gdc"]):
                continue

            # Game time
            time_div = div.find("div", class_="lineup__time")
            game_time = time_div.get_text(strip=True) if time_div else None

            # Teams
            abbrs = div.find_all("div", class_="lineup__abbr")
            if len(abbrs) < 2:
                continue
            away_team = abbrs[0].get_text(strip=True)
            home_team = abbrs[1].get_text(strip=True)

            # Starters (name + hand)
            away_starter_name = away_starter_hand = home_starter_name = home_starter_hand = None
            for lst in div.find_all("ul", class_="lineup__list"):
                highlight = lst.find("li", class_="lineup__player-highlight")
                if highlight:
                    name_tag = highlight.find("a")
                    hand_tag = highlight.find("span", class_="lineup__throws")
                    name = name_tag.get_text(strip=True) if name_tag else None
                    hand = hand_tag.get_text(strip=True) if hand_tag else None
                    if "is-visit" in lst.get("class", []):
                        away_starter_name, away_starter_hand = name, hand
                    else:
                        home_starter_name, home_starter_hand = name, hand

            # Lineups as ordered lists
            away_lineup = away_lineup_pos = away_lineup_bats = []
            home_lineup = home_lineup_pos = home_lineup_bats = []
            for lst in div.find_all("ul", class_="lineup__list"):
                is_away = "is-visit" in lst.get("class", [])
                names, poss, bats = [], [], []
                for li in lst.find_all("li", class_="lineup__player"):
                    pos = li.find("div", class_="lineup__pos")
                    name = li.find("a")
                    bat = li.find("span", class_="lineup__bats")
                    if name:
                        names.append(name.get_text(strip=True))
                        poss.append(pos.get_text(strip=True) if pos else None)
                        bats.append(bat.get_text(strip=True) if bat else None)
                if is_away:
                    away_lineup, away_lineup_pos, away_lineup_bats = names[:9], poss[:9], bats[:9]
                else:
                    home_lineup, home_lineup_pos, home_lineup_bats = names[:9], poss[:9], bats[:9]

            # Status → boolean
            status_li = div.find("li", class_="lineup__status")
            is_confirmed = bool(status_li and "Confirmed" in status_li.get_text(strip=True))

            # Weather
            weather = div.find("div", class_="lineup__weather-text")
            weather_text = weather.get_text(strip=True) if weather else None

            # Umpire
            umpire_div = div.find("div", class_="lineup__umpire")
            umpire = umpire_div.get_text(strip=True).replace("Umpire:", "").strip() if umpire_div else None
            if umpire and "Not announced yet" in umpire:
                umpire = None

            # === ALL ODDS (LINE + O/U) as dicts ===
            odds_line = {"composite": None, "fanduel": None, "draftkings": None, "betmgm": None, "pointsbet": None}
            odds_ou = {"composite": None, "fanduel": None, "draftkings": None, "betmgm": None, "pointsbet": None}

            odds_div = div.find("div", class_="lineup__odds")
            if odds_div:
                for item in odds_div.find_all("div", class_="lineup__odds-item"):
                    text = item.get_text(strip=True)
                    spans = item.find_all("span")
                    for span in spans:
                        book = span.get("class", [""])[0]  # composite, fanduel, etc.
                        val = span.get_text(strip=True)
                        if book in odds_line:
                            if "LINE" in text:
                                odds_line[book] = parse_float(val)
                            elif "O/U" in text:
                                odds_ou[book] = parse_float(val)

            games.append({
                "fetch_timestamp": fetch_timestamp,
                "game_time": game_time,
                "away_team": away_team,
                "home_team": home_team,
                "away_starter_name": away_starter_name,
                "away_starter_hand": away_starter_hand,
                "home_starter_name": home_starter_name,
                "home_starter_hand": home_starter_hand,
                "away_lineup": away_lineup,
                "away_lineup_pos": away_lineup_pos,
                "away_lineup_bats": away_lineup_bats,
                "home_lineup": home_lineup,
                "home_lineup_pos": home_lineup_pos,
                "home_lineup_bats": home_lineup_bats,
                "is_confirmed": is_confirmed,
                "weather": weather_text,
                "umpire": umpire,
                "odds_line": odds_line,      # dict with all books
                "odds_ou": odds_ou,          # dict with all books
            })
        except Exception:
            continue

    df = pd.DataFrame(games)
    print(f"✅ Parsed {len(df)} games — all books now in odds_line / odds_ou dicts")
    return df

def parse_float(text):
    if not text or text.strip() in ["–", "-", ""]:
        return None
    try:
        num = re.search(r'[-+]?\d*\.?\d+', text.strip())
        return float(num.group(0)) if num else None
    except:
        return None

def save_lineups(df: pd.DataFrame):
    if df.empty:
        return
    date_str = df["fetch_timestamp"].iloc[0].strftime("%Y-%m-%d")
    time_str = df["fetch_timestamp"].iloc[0].strftime("%H%M%S")
    path = RAW_LINEUPS_PATH / date_str
    path.mkdir(parents=True, exist_ok=True)
    filename = path / f"lineups_{time_str}.parquet"
    df.to_parquet(filename, compression="snappy")
    print(f"✅ Saved → {filename}")

if __name__ == "__main__":
    print("🚀 Fetching live Rotowire daily lineups...")
    df = fetch_lineups(live=True)
    if df is not None and not df.empty:
        save_lineups(df)
        print("\nSample first game:")
        sample = df.iloc[0]
        print(sample[["game_time", "away_team", "home_team", "away_starter_name", "away_starter_hand", "is_confirmed"]])
        print("odds_ou dict:", sample["odds_ou"])
        print("away_lineup_bats:", sample["away_lineup_bats"])