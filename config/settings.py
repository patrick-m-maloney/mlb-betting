import os
from dotenv import load_dotenv
from pathlib import Path

import yaml
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

# new
# BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"

# Data lake paths (bronze/silver/gold)
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
RAW_DIR = DATA_DIR / "raw"          # legacy, keep for now
REFERENCE_DIR = DATA_DIR / "reference"
DB_PATH = DATA_DIR / "db" / "mlb_betting.duckdb"

# Named subdirectory paths
BRONZE_ODDS_DIR = BRONZE_DIR / "odds"
BRONZE_LINEUPS_DIR = BRONZE_DIR / "lineups"
SILVER_LINEUPS_DIR = SILVER_DIR / "lineups"
SCHEDULES_DIR = DATA_DIR / "schedules"
PLAYER_LOGS_DIR = DATA_DIR / "player_logs"
GAME_BY_GAME_DIR = PLAYER_LOGS_DIR / "game_by_game"
FANGRAPHS_DIR = PLAYER_LOGS_DIR / "fangraphs_leaderboards"
SIMULATIONS_DIR = DATA_DIR / "simulations"

# Create directories if missing
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, RAW_DIR, REFERENCE_DIR, DB_PATH.parent,
          BRONZE_ODDS_DIR, BRONZE_LINEUPS_DIR, SILVER_LINEUPS_DIR,
          SCHEDULES_DIR, PLAYER_LOGS_DIR, GAME_BY_GAME_DIR, FANGRAPHS_DIR, SIMULATIONS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

RAW_ODDS_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

# Reference files
PLAYER_ID_MAP_PATH = REFERENCE_DIR / "player_id_map.parquet"
LINEAR_WEIGHTS_PATH = REFERENCE_DIR / "linear_weights.parquet"

# Scraping / API settings
ROT_WIRE_DELAY = 2.0          # seconds between requests
THE_RUNDOWN_API_KEY = os.getenv("THE_RUNDOWN_API_KEY")

# Matching thresholds
MATCH_CONFIDENCE_THRESHOLD = 85
FUZZY_THRESHOLD = 90

DATA_SCHEMAS = yaml.safe_load((BASE_DIR / "config" / "data_schemas.yaml").read_text(encoding="utf-8"))