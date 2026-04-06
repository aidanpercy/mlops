from __future__ import annotations

import csv
import sqlite3
from datetime import datetime
from pathlib import Path

from .ebay_client import EbayListing


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ebay_historical_clothing (
    item_id TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    title TEXT NOT NULL,
    price_text TEXT,
    price_value REAL,
    shipping_text TEXT,
    condition_text TEXT,
    sold_date_text TEXT,
    item_url TEXT NOT NULL,
    page_number INTEGER,
    scraped_at_utc TEXT NOT NULL
);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO ebay_historical_clothing (
    item_id,
    query,
    title,
    price_text,
    price_value,
    shipping_text,
    condition_text,
    sold_date_text,
    item_url,
    page_number,
    scraped_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


class ListingStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(CREATE_TABLE_SQL)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def save_listings(self, listings: list[EbayListing]) -> int:
        if not listings:
            return 0

        before = self._total_count()
        self.conn.executemany(
            INSERT_SQL,
            [
                (
                    x.item_id,
                    x.query,
                    x.title,
                    x.price_text,
                    x.price_value,
                    x.shipping_text,
                    x.condition_text,
                    x.sold_date_text,
                    x.item_url,
                    x.page_number,
                    x.scraped_at_utc,
                )
                for x in listings
            ],
        )
        self.conn.commit()
        after = self._total_count()
        return after - before

    def export_all_to_csv(self, export_dir: Path) -> Path:
        export_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = export_dir / f"ebay_historical_{stamp}.csv"

        cursor = self.conn.execute(
            """
            SELECT
                item_id,
                query,
                title,
                price_text,
                price_value,
                shipping_text,
                condition_text,
                sold_date_text,
                item_url,
                page_number,
                scraped_at_utc
            FROM ebay_historical_clothing
            ORDER BY scraped_at_utc DESC;
            """
        )
        columns = [x[0] for x in cursor.description]
        rows = cursor.fetchall()

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        return out_path

    def _total_count(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(1) FROM ebay_historical_clothing;"
        ).fetchone()
        return int(row[0]) if row else 0
