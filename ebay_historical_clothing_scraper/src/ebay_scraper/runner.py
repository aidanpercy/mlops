from __future__ import annotations

import sys

from .config import load_settings
from .ebay_client import EbayAccessError, EbayClient, EbayListing
from .storage import ListingStorage


def run_once() -> dict:
    settings = load_settings()
    client = EbayClient(settings)
    storage = ListingStorage(settings.db_path)

    total_fetched = 0
    all_listings: list[EbayListing] = []
    try:
        for query in settings.queries:
            listings = list(client.fetch_sold_listings(query=query))
            total_fetched += len(listings)
            all_listings.extend(listings)

        inserted = storage.save_listings(all_listings)
        csv_path = storage.export_all_to_csv(settings.export_dir)
        total_stored = total_fetched - inserted
        return {
            "queries": settings.queries,
            "fetched": total_fetched,
            "inserted_new": inserted,
            "duplicates_ignored": total_stored,
            "csv_path": str(csv_path),
            "db_path": str(settings.db_path),
        }
    finally:
        storage.close()


if __name__ == "__main__":
    try:
        result = run_once()
    except EbayAccessError as err:
        print(err, file=sys.stderr)
        raise SystemExit(1) from err
    print("Run complete")
    print(f"Queries: {', '.join(result['queries'])}")
    print(f"Fetched: {result['fetched']}")
    print(f"Inserted new: {result['inserted_new']}")
    print(f"Duplicates ignored: {result['duplicates_ignored']}")
    print(f"Database: {result['db_path']}")
    print(f"CSV export: {result['csv_path']}")
