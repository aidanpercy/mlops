#!/usr/bin/env python3
"""
Predict sold price from brand, item type, and condition using the trained RF pipeline.

Usage (from repo root):
  python scripts/predict_price_rf.py --brand "Nike" --item-type sneaker --condition new

Requires models/ebay_price_rf.joblib from scripts/train_price_rf.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = ROOT / "models/ebay_price_rf.joblib"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict price with trained ebay_price_rf pipeline.")
    p.add_argument("--model", type=Path, default=DEFAULT_MODEL, help="Path to joblib pipeline.")
    p.add_argument("--brand", required=True, help="brand_name (e.g. Nike, Levi's, Unknown)")
    p.add_argument("--item-type", required=True, dest="item_type", help="item_type (e.g. sneaker, hoodie)")
    p.add_argument("--condition", required=True, help="normalized condition (e.g. new, used)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    model_path = args.model if args.model.is_absolute() else ROOT / args.model
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found: {model_path}. Train first: python scripts/train_price_rf.py"
        )

    pipeline = joblib.load(model_path)
    row = pd.DataFrame(
        [
            {
                "brand_name": args.brand.strip() or "unknown",
                "item_type": args.item_type.strip() or "unknown",
                "condition": args.condition.strip() or "unknown",
            }
        ]
    )
    pred = float(pipeline.predict(row)[0])
    print(f"Predicted sold price: ${pred:.2f}")


if __name__ == "__main__":
    main()
