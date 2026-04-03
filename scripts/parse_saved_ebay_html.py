#!/usr/bin/env python3
"""Parse a saved eBay item HTML file into a normalized JSON record."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from clothing_mlops.data_pipeline import parse_saved_ebay_item_html


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/parse_saved_ebay_html.py /path/to/file.html")

    html_path = Path(sys.argv[1])
    record = parse_saved_ebay_item_html(html_path)
    print(json.dumps(record, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
