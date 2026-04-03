#!/usr/bin/env python3
"""Download HTML pages from a list of URLs into a local folder."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from clothing_mlops.data_pipeline import download_html_pages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("url_file", help="Path to a text file with one URL per line")
    parser.add_argument(
        "--output-dir",
        default="data/raw/downloads",
        help="Directory where HTML files and the manifest will be written",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.0,
        help="Delay between requests",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="Per-request timeout",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    url_file = Path(args.url_file)
    urls = [
        line.strip()
        for line in url_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    manifest = download_html_pages(
        urls=urls,
        output_dir=Path(args.output_dir),
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps({"downloaded": manifest}, indent=2))


if __name__ == "__main__":
    main()
