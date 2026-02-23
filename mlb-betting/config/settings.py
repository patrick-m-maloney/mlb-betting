import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# API Keys
THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY")
THERUNDOWN_API_KEY = os.getenv("THERUNDOWN_API_KEY")  # future

# The Odds API
BASE_URL = "https://api.the-odds-api.com/v4"
SPORT = "baseball_mlb"

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_ODDS_PATH = BASE_DIR / "data" / "raw" / "odds"
PROCESSED_PATH = BASE_DIR / "data" / "processed"

RAW_ODDS_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)