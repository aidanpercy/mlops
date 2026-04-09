from __future__ import annotations

import csv
from pathlib import Path


def _norm_key(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def load_catalog_queries(csv_path: Path) -> list[str]:
    """Read clothing.csv-style rows and return eBay search strings (brand + item)."""
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        field_map = {_norm_key(h): h for h in reader.fieldnames if h}
        item_col = field_map.get("item")
        brand_col = field_map.get("brand")
        if not item_col:
            return []

        out: list[str] = []
        for row in reader:
            item = (row.get(item_col) or "").strip()
            if not item:
                continue
            brand = (row.get(brand_col) or "").strip() if brand_col else ""
            q = f"{brand} {item}".strip() if brand else item
            if q:
                out.append(q)
        return out


def read_cursor(cursor_path: Path) -> int:
    if not cursor_path.is_file():
        return 0
    try:
        raw = cursor_path.read_text(encoding="utf-8").strip()
        return max(0, int(raw))
    except (OSError, ValueError):
        return 0


def write_cursor(cursor_path: Path, value: int) -> None:
    cursor_path.parent.mkdir(parents=True, exist_ok=True)
    cursor_path.write_text(str(value), encoding="utf-8")


def next_query_batch(
    csv_path: Path,
    cursor_path: Path,
    batch_size: int,
) -> tuple[list[str], int, int]:
    """
    Return (queries, start_index, new_cursor) for the next run.
    Rotates through the catalog; cursor advances by batch_size modulo len.
    """
    queries = load_catalog_queries(csv_path)
    if not queries or batch_size <= 0:
        return [], 0, 0

    n = len(queries)
    start = read_cursor(cursor_path) % n
    picked = [queries[(start + i) % n] for i in range(batch_size)]
    new_cursor = (start + batch_size) % n
    write_cursor(cursor_path, new_cursor)
    return picked, start, new_cursor
