"""
Polymarket MLB market explorer — uses Gamma Events API with tag_slug=mlb.
Filters to game markets (Team A vs. Team B) and win total markets only.
Writes output to notebooks/polymarket_examples_clob.txt.
No parquet writes — discovery script only.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import requests
from datetime import datetime, timezone

EVENTS_URL  = "https://gamma-api.polymarket.com/events"
OUTPUT_FILE = Path(__file__).resolve().parent.parent / "notebooks" / "polymarket_examples_clob.txt"


def fetch_mlb_events() -> list[dict]:
    all_events: list[dict] = []
    offset = 0
    limit  = 100
    page   = 1

    while True:
        resp = requests.get(
            EVENTS_URL,
            params={"tag_slug": "mlb", "active": "true", "closed": "false",
                    "limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_events.extend(batch)
        print(f"  page {page}: {len(batch)} events — running total: {len(all_events)}")
        if len(batch) < limit:
            break
        offset += limit
        page   += 1

    return all_events


def is_game_event(event: dict) -> bool:
    return " vs. " in event.get("title", "")


def is_win_total_event(event: dict) -> bool:
    title = event.get("title", "")
    return "Pro Baseball" in title and "Regular Season" in title


def fmt_prices(market: dict) -> str:
    raw_prices   = market.get("outcomePrices", [])
    raw_outcomes = market.get("outcomes", [])
    try:
        prices   = json.loads(raw_prices)   if isinstance(raw_prices, str)   else raw_prices
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
    except (json.JSONDecodeError, TypeError):
        return "n/a"
    parts = []
    for i, price in enumerate(prices):
        label = outcomes[i] if i < len(outcomes) else f"Outcome{i}"
        try:
            parts.append(f"{label}: {float(price):.3f}")
        except (ValueError, TypeError):
            parts.append(f"{label}: n/a")
    return " / ".join(parts)


def main():
    lines: list[str] = []

    def p(s=""):
        print(s)
        lines.append(s)

    p("Polymarket — MLB Market Explorer (Gamma Events API)")
    p(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    p()

    print("Fetching active MLB events...")
    events = fetch_mlb_events()

    game_events = [e for e in events if is_game_event(e)]
    wt_events   = [e for e in events if is_win_total_event(e)]
    game_mkts   = sum(len(e.get("markets", [])) for e in game_events)
    wt_mkts     = sum(len(e.get("markets", [])) for e in wt_events)

    p(f"Total events fetched:   {len(events)}")
    p(f"Game events:            {len(game_events)} ({game_mkts} markets)")
    p(f"Win total events:       {len(wt_events)} ({wt_mkts} markets)")
    p()

    # -----------------------------------------------------------------------
    # Raw JSON sample
    # -----------------------------------------------------------------------
    if game_events:
        p("=" * 80)
        p("RAW JSON — first game event:")
        p("=" * 80)
        p(json.dumps(game_events[0], indent=2))
        p()

    # -----------------------------------------------------------------------
    # Game markets table
    # -----------------------------------------------------------------------
    p("=" * 160)
    p("GAME MARKETS")
    p(f"{'Event':<35} {'Question':<48} {'Type':<14} {'End Date':<12} {'Prices':<50}  {'Volume':>10}")
    p("-" * 160)

    for event in game_events:
        event_title = event.get("title", "")[:33]
        end_date    = str(event.get("endDate", "") or "")[:10]
        for m in event.get("markets", []):
            question = str(m.get("question", ""))[:46]
            group    = str(m.get("groupItemTitle") or "moneyline")[:12]
            prices   = fmt_prices(m)[:48]
            volume   = m.get("liquidityNum") or m.get("liquidity") or 0
            try:
                vol_str = f"${float(volume):,.0f}"
            except (TypeError, ValueError):
                vol_str = "-"
            p(f"{event_title:<35} {question:<48} {group:<14} {end_date:<12} {prices:<50}  {vol_str:>10}")

    p()

    # -----------------------------------------------------------------------
    # Win total markets table
    # -----------------------------------------------------------------------
    p("=" * 130)
    p("WIN TOTAL MARKETS")
    p(f"{'Question':<70} {'End Date':<12} {'Over(Yes)':>10} {'Under(No)':>10}  {'Volume':>10}")
    p("-" * 130)

    for event in wt_events:
        for m in event.get("markets", []):
            question = str(m.get("question", ""))[:68]
            end_date = str(m.get("endDateIso") or m.get("endDate") or "")[:10]
            raw = m.get("outcomePrices", [])
            try:
                prices = json.loads(raw) if isinstance(raw, str) else raw
                yes_p = f"{float(prices[0]):.3f}" if len(prices) > 0 else "n/a"
                no_p  = f"{float(prices[1]):.3f}" if len(prices) > 1 else "n/a"
            except (ValueError, TypeError, IndexError):
                yes_p = no_p = "n/a"
            volume = m.get("liquidityNum") or m.get("liquidity") or 0
            try:
                vol_str = f"${float(volume):,.0f}"
            except (TypeError, ValueError):
                vol_str = "-"
            p(f"{question:<70} {end_date:<12} {yes_p:>10} {no_p:>10}  {vol_str:>10}")

    p()
    p(f"Total: {game_mkts} game markets + {wt_mkts} win total markets across {len(events)} MLB events.")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nWritten to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
