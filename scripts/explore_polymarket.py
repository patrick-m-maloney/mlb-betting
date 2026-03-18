"""
Polymarket MLB market explorer — discovery script, no writes.
Paginates the Gamma API and prints all baseball-related markets.
"""

import requests

BASE_URL = "https://gamma-api.polymarket.com/markets"

MLB_KEYWORDS = [
    "mlb", "baseball", "pitcher",
    # Team names
    "yankees", "red sox", "blue jays", "rays", "orioles",
    "white sox", "guardians", "tigers", "royals", "twins",
    "astros", "athletics", "rangers", "mariners", "angels",
    "braves", "marlins", "mets", "phillies", "nationals",
    "cubs", "reds", "brewers", "pirates", "cardinals",
    "dodgers", "padres", "giants", "rockies", "diamondbacks",
    "world series", "cy young", "mvp",
]


def fetch_all_markets() -> list[dict]:
    """Paginate through all active Gamma API markets."""
    all_markets = []
    offset = 0
    limit = 500
    page = 1

    while True:
        resp = requests.get(
            BASE_URL,
            params={
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            },
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break

        all_markets.extend(batch)
        print(f"  fetched page {page} ({len(batch)} markets, {len(all_markets)} total so far)...")

        if len(batch) < limit:
            break  # last page

        offset += limit
        page += 1

    return all_markets


def is_mlb(market: dict) -> bool:
    haystack = " ".join([
        str(market.get("question", "")),
        str(market.get("description", "")),
        str(market.get("category", "")),
        str(market.get("tags", "")),
        str(market.get("slug", "")),
    ]).lower()
    return any(kw in haystack for kw in MLB_KEYWORDS)


def fmt(val, width: int) -> str:
    s = str(val or "")
    return s[:width].ljust(width)


def main():
    print("Fetching all active Polymarket markets...\n")
    all_markets = fetch_all_markets()
    print(f"\nTotal active markets fetched: {len(all_markets)}")

    mlb = [m for m in all_markets if is_mlb(m)]
    print(f"MLB/baseball matches: {len(mlb)}\n")

    if not mlb:
        print("No MLB markets found. Try loosening the keyword filter.")
        return

    # Print raw JSON of first match so we can see all available fields
    print("=" * 80)
    print("RAW JSON — first MLB market:")
    print("=" * 80)
    import json
    print(json.dumps(mlb[0], indent=2))
    print()

    # Print clean table
    print("=" * 120)
    print(f"{'Question':<60} {'Volume':>12}  {'End Date':<12}  Tags/Category")
    print("-" * 120)

    for m in mlb:
        question = str(m.get("question") or m.get("title") or "")[:58]
        volume   = m.get("volume") or m.get("usdcVolume") or m.get("volumeNum") or ""
        try:
            volume = f"${float(volume):,.0f}"
        except (TypeError, ValueError):
            volume = str(volume or "-")

        end_date = str(m.get("endDate") or m.get("end_date") or "")[:10]

        tags = m.get("tags") or m.get("category") or ""
        if isinstance(tags, list):
            tags = ", ".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in tags)
        tags = str(tags)[:35]

        print(f"{question:<60} {volume:>12}  {end_date:<12}  {tags}")

    print(f"\n{len(mlb)} MLB markets found across {len(all_markets)} total active markets.")


if __name__ == "__main__":
    main()
