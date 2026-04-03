#!/usr/bin/env python3
"""Train a baseline model from the silver dataset and log it to MLflow."""

from __future__ import annotations

import json
from pathlib import Path

from clothing_mlops.modeling import train_and_log_model

DEFAULT_DATASET = Path("data/silver/sold_listings_training.csv")


def main() -> None:
    result = train_and_log_model(DEFAULT_DATASET)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
