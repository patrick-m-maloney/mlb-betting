from src.data_ingestion.lineups import fetch_lineups, save_lineups
from src.data_ingestion.odds.the_odds_api_fetcher import fetch_odds, fetch_futures
from src.data_ingestion.odds.kalshi_fetcher import fetch_kalshi_mlb
from src.data_ingestion.odds.polymarket_fetcher import fetch_polymarket_mlb
from src.data_ingestion.odds.rundown_fetcher import fetch_rundown_mlb
from src.database.db_manager import append_to_table

print("🚀 Manual scrape started — appending to central DuckDB only")

df_lineups = fetch_lineups(live=True)
if df_lineups is not None:
    save_lineups(df_lineups)
    append_to_table(df_lineups, "raw_lineups")

fetch_odds("baseball_mlb", "h2h,spreads,totals")
fetch_futures()
fetch_kalshi_mlb()
fetch_polymarket_mlb()
fetch_rundown_mlb()

print("\n✅ Manual run complete! Check the DB with:")
print("   duckdb data/mlb_betting.db")
print("   .tables")
print("   SELECT COUNT(*) FROM raw_odds;")
print("   SELECT * FROM raw_odds LIMIT 5;")