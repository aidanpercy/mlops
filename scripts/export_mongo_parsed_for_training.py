#!/usr/bin/env python3
"""Export MongoDB parsed listings to a CSV compatible with train_price_rf.py (XGBoost).

Typical flow:

1. Populate Mongo (e.g. ``extract_features_from_mongo.py`` with
   ``MONGODB_TARGET_COLL`` set so rows land in the ``parsed`` collection).
2. Run this script (requires ``MONGODB_URI``).
3. Train: ``python scripts/train_price_rf.py --data <output.csv>``
   Or use ``--train`` on this script to export and train in one step.

Environment (same family as ``extract_features_from_mongo.py``):

    MONGODB_URI           Required.
    MONGODB_DATABASE      Optional, default ``historical``.
    MONGODB_PARSED_COLL   Optional, default ``parsed`` (falls back to
                          ``MONGODB_TARGET_COLL`` if set and parsed is empty).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import certifi
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

ROOT = Path(__file__).resolve().parents[1]


def _build_mongo_client(uri: str) -> MongoClient:
    return MongoClient(
        uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=20000,
    )


def _load_clean_ebay():
    spec = importlib.util.spec_from_file_location(
        "clean_ebay_exports",
        ROOT / "scripts" / "clean_ebay_exports.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def fetch_parsed_rows(
    *,
    uri: str,
    database: str,
    collection: str,
    limit: int,
    require_parse_ok: bool,
) -> list[dict[str, Any]]:
    client = _build_mongo_client(uri)
    try:
        client.admin.command("ping")
        coll = client[database][collection]
        if require_parse_ok:
            query: dict[str, Any] = {"parse_status": "ok"}
        else:
            query = {
                "parsed_price": {"$nin": ["", None]},
                "$or": [
                    {"parse_status": {"$exists": False}},
                    {"parse_status": {"$nin": ["error", "dry_run"]}},
                ],
            }
        cursor = coll.find(query).sort("_id", -1)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)
    finally:
        client.close()


EXTRA_CATEGORICAL_FIELDS: tuple[str, ...] = (
    "item_subtype",
    "department",
    "gender",
    "age_group",
    "size",
    "size_type",
    "color_primary",
    "color_secondary",
    "material_primary",
    "material_secondary",
    "pattern",
    "closure",
    "fit",
    "sleeve_length",
    "neckline",
    "style",
    "occasion",
    "season",
    "sport",
    "model_name",
    "product_line",
    "style_code",
    "has_box",
    "condition_detail",
)

EXTRA_NUMERIC_FIELDS: tuple[str, ...] = (
    "release_year",
    "original_price",
)


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _coerce_float(value: Any) -> float:
    text = _coerce_str(value).replace(",", "").replace("$", "")
    if not text:
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def documents_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for doc in rows:
        title = _coerce_str(doc.get("title_clean") or doc.get("title"))
        brand = _coerce_str(doc.get("brand_name")) or "unknown"
        itype = _coerce_str(doc.get("item_type")) or "unknown"
        cond = _coerce_str(doc.get("condition")) or "unknown"
        price = _coerce_float(doc.get("parsed_price"))
        if not (price > 0):  # also catches NaN
            continue

        record: dict[str, Any] = {
            "title": title,
            "brand_name": brand,
            "item_type": itype,
            "condition": cond,
            "price": price,
        }
        for field in EXTRA_CATEGORICAL_FIELDS:
            record[field] = _coerce_str(doc.get(field))
        for field in EXTRA_NUMERIC_FIELDS:
            record[field] = _coerce_float(doc.get(field))
        records.append(record)
    return pd.DataFrame.from_records(records)


def resolve_mongo_settings(
    *,
    uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> tuple[str, str, str]:
    """Resolve Mongo connection settings from args + environment with same defaults
    used by ``extract_features_from_mongo.py``.

    Order of precedence: explicit arg > env var > script default.
    Returns ``(uri, database, collection)``. Raises ``ValueError`` if no URI can
    be resolved.
    """
    resolved_uri = (uri or os.getenv("MONGODB_URI") or "").strip()
    if not resolved_uri:
        raise ValueError(
            "MONGODB_URI is not set. Export it (or place it in .env) or pass --mongo-uri."
        )

    resolved_db = (database or os.getenv("MONGODB_DATABASE") or "historical").strip() or "historical"

    if collection:
        resolved_coll = collection.strip()
    else:
        parsed_coll = (os.getenv("MONGODB_PARSED_COLL") or "").strip()
        target_coll = (os.getenv("MONGODB_TARGET_COLL") or "").strip()
        resolved_coll = parsed_coll or target_coll or "parsed"
    return resolved_uri, resolved_db, resolved_coll


def load_training_frame_from_mongo(
    *,
    uri: str,
    database: str,
    collection: str,
    limit: int,
    clothing_csv: Path | None,
    relaxed_parse_filter: bool,
) -> tuple[pd.DataFrame, str]:
    """Pull parsed listings from Mongo, attach catalog initial prices, return DataFrame.

    Returns ``(df, filter_mode)`` where ``filter_mode`` is ``"strict"`` (only
    parse_status=='ok'), ``"strict_fallback_relaxed"`` (strict was empty so we
    fell back to non-error rows with numeric parsed_price), or ``"relaxed"``.
    """
    if relaxed_parse_filter:
        rows = fetch_parsed_rows(
            uri=uri,
            database=database,
            collection=collection,
            limit=limit,
            require_parse_ok=False,
        )
        df = documents_to_frame(rows)
        filter_mode = "relaxed"
    else:
        rows = fetch_parsed_rows(
            uri=uri,
            database=database,
            collection=collection,
            limit=limit,
            require_parse_ok=True,
        )
        df = documents_to_frame(rows)
        filter_mode = "strict"
        if df.empty:
            rows = fetch_parsed_rows(
                uri=uri,
                database=database,
                collection=collection,
                limit=limit,
                require_parse_ok=False,
            )
            df = documents_to_frame(rows)
            if not df.empty:
                print(
                    "Note: no rows with parse_status='ok'; used relaxed filter "
                    "(non-error docs with numeric parsed_price).",
                    file=sys.stderr,
                )
                filter_mode = "strict_fallback_relaxed"

    cee = _load_clean_ebay()
    if clothing_csv is not None:
        clothing_path = clothing_csv.expanduser().resolve()
    else:
        clothing_path = cee.resolve_clothing_csv(ROOT / "ebay_historical_clothing_scraper/data")

    if not df.empty:
        df = cee.attach_initial_prices(df, clothing_path)
    return df, filter_mode


def export_mongo_training_csv(
    *,
    uri: str,
    database: str,
    collection: str,
    limit: int,
    out: Path,
    clothing_csv: Path | None,
    relaxed_parse_filter: bool,
) -> tuple[pd.DataFrame, Path]:
    """Pull from Mongo, attach catalog prices, write CSV. Returns (df, absolute_out_path)."""
    df, _filter_mode = load_training_frame_from_mongo(
        uri=uri,
        database=database,
        collection=collection,
        limit=limit,
        clothing_csv=clothing_csv,
        relaxed_parse_filter=relaxed_parse_filter,
    )

    out_abs = out if out.is_absolute() else ROOT / out
    out_abs.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_abs, index=False)
    return df, out_abs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--out",
        type=Path,
        default=ROOT / "ebay_historical_clothing_scraper/data/processed/mongo_parsed_for_training.csv",
        help="Output CSV path.",
    )
    p.add_argument(
        "--clothing-csv",
        type=Path,
        default=None,
        help="Retail catalog CSV for initial_price matching (same as clean_ebay_exports).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max documents to export (0 = no limit).",
    )
    p.add_argument(
        "--relaxed-filter",
        action="store_true",
        help="Do not require parse_status='ok' (use any non-error row with valid parsed_price).",
    )
    p.add_argument(
        "--train",
        action="store_true",
        help="After a successful export, run scripts/train_price_rf.py (XGBoost) on the CSV.",
    )
    p.add_argument(
        "--min-rows",
        type=int,
        default=50,
        help="With --train, fail if fewer than this many usable rows (train script requirement).",
    )
    p.add_argument(
        "--no-mlflow",
        action="store_true",
        help="With --train, pass --no-mlflow to the training script.",
    )
    p.add_argument(
        "--model-out",
        type=Path,
        default=None,
        help="With --train, pass --model-out PATH to the training script.",
    )
    return p.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    try:
        uri, database, collection = resolve_mongo_settings()
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        df, out = export_mongo_training_csv(
            uri=uri,
            database=database,
            collection=collection,
            limit=args.limit,
            out=args.out,
            clothing_csv=args.clothing_csv,
            relaxed_parse_filter=args.relaxed_filter,
        )
    except PyMongoError as exc:
        print(f"ERROR: MongoDB failed: {exc}", file=sys.stderr)
        return 2

    if df.empty:
        print(
            f"No usable rows with valid parsed_price in {database}.{collection}. "
            "Ensure extract_features_from_mongo has written parsed documents, "
            "or try --relaxed-filter.",
            file=sys.stderr,
        )
        return 3

    print(f"Wrote {len(df)} row(s) to {out}")

    if args.train:
        if len(df) < args.min_rows:
            print(
                f"ERROR: {len(df)} rows is below --min-rows ({args.min_rows}). "
                "Train anyway by lowering --min-rows or ingest more Mongo documents.",
                file=sys.stderr,
            )
            return 4
        train_cmd = [
            sys.executable,
            str(ROOT / "scripts" / "train_price_rf.py"),
            "--data",
            str(out),
        ]
        if args.no_mlflow:
            train_cmd.append("--no-mlflow")
        if args.model_out is not None:
            train_cmd.extend(["--model-out", str(args.model_out)])
        print("Running:", " ".join(train_cmd))
        proc = subprocess.run(train_cmd, cwd=str(ROOT))
        return int(proc.returncode)

    print("Train with:")
    print(f"  python scripts/train_price_rf.py --data {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
