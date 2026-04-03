#!/usr/bin/env python3
"""Build raw, bronze, and silver sample data artifacts for the project."""

from __future__ import annotations

import json

from clothing_mlops.data_pipeline import build_dataset_artifacts


def main() -> None:
    summary = build_dataset_artifacts()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
