"""Shared request contract helpers for training and serving."""

from __future__ import annotations


def feature_columns() -> list[str]:
    return [
        "brand",
        "category",
        "size",
        "condition",
        "color",
        "material",
        "listing_price",
        "shipping_price",
    ]


def target_column() -> str:
    return "sale_price"


def prediction_example() -> dict[str, str | float]:
    return {
        "brand": "Patagonia",
        "category": "Jacket",
        "size": "M",
        "condition": "used_very_good",
        "color": "Blue",
        "material": "Polyester",
        "listing_price": 129.0,
        "shipping_price": 12.5,
    }


def pricing_request_example() -> dict[str, str | float]:
    return {
        "description": "Patagonia Synchilla fleece pullover in navy, men's medium, lightly worn with no stains or holes.",
        "retail_price": 139.0,
    }
