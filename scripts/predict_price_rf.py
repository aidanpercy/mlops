#!/usr/bin/env python3
"""
Predict sold price using the trained XGBoost pipeline (ColumnTransformer + XGBRegressor).

Usage (from repo root):
  python scripts/predict_price_rf.py --brand "Nike" --item-type sneaker --condition new --initial-price 185

Requires models/ebay_price_xgb.joblib from scripts/train_price_rf.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = ROOT / "models/ebay_price_xgb.joblib"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict price with trained ebay price XGBoost pipeline.")
    p.add_argument("--model", type=Path, default=DEFAULT_MODEL, help="Path to joblib pipeline.")
    p.add_argument("--brand", required=True, help="brand_name (e.g. Nike, Levi's, Unknown)")
    p.add_argument("--item-type", required=True, dest="item_type", help="item_type (e.g. sneaker, hoodie)")
    p.add_argument("--condition", required=True, help="normalized condition (e.g. new, used)")
    p.add_argument(
        "--initial-price",
        type=float,
        default=None,
        help="Retail MSRP from catalog if known; omit so the pipeline imputes like missing training rows.",
    )
    p.add_argument(
        "--catalog-item",
        default=None,
        help="Matched clothing.csv catalog item name if known; omit or empty for unknown (same as training).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    model_path = args.model if args.model.is_absolute() else ROOT / args.model
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found: {model_path}. Train first: python scripts/train_price_rf.py"
        )

    pipeline = joblib.load(model_path)
    cat_item = (
        "unknown"
        if args.catalog_item is None or not str(args.catalog_item).strip()
        else str(args.catalog_item).strip()
    )
    row = pd.DataFrame(
        [
            {
                "brand_name": args.brand.strip() or "unknown",
                "item_type": args.item_type.strip() or "unknown",
                "condition": args.condition.strip() or "unknown",
                "initial_price_catalog_item": cat_item,
                "initial_price": float("nan")
                if args.initial_price is None
                else float(args.initial_price),
            }
        ]
    )
    pred = float(pipeline.predict(row)[0])
    print(f"Predicted sold price: ${pred:.2f}")


if __name__ == "__main__":
    main()
