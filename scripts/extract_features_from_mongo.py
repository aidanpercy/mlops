"""Pull documents from MongoDB and extract structured features via NVIDIA LLM.

For each document pulled from the configured MongoDB collection, this script
builds an extraction prompt (title / query / condition text / price fields) and
calls an NVIDIA-hosted chat completions model. The parsed JSON features are
written to a local JSONL file and optionally back to a MongoDB collection.

Environment variables (loaded from `.env` via python-dotenv when present):

    NVIDIA_API_KEY        Required. API key for https://integrate.api.nvidia.com.
    NVIDIA_MODEL          Optional. Defaults to "meta/llama-3.3-70b-instruct".
    MONGODB_URI           Optional. Defaults to the read-only service_account URI
                          used in scripts/data_processing.ipynb.
    MONGODB_DATABASE      Optional. Defaults to "historical".
    MONGODB_SOURCE_COLL   Optional. Defaults to "raw".
    MONGODB_TARGET_COLL   Optional. If set, parsed features are upserted here.

Usage:

    python scripts/extract_features_from_mongo.py --limit 100
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
import certifi
import pandas as pd

DEFAULT_DATABASE = "historical"
DEFAULT_SOURCE_COLLECTION = "raw"
DEFAULT_MODEL = "meta/llama-3.3-70b-instruct"
DEFAULT_LIMIT = 100

NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
REQUEST_TIMEOUT_SECONDS = 120
INTER_REQUEST_DELAY_SECONDS = 5

FEATURE_KEYS = [
    "brand_name",
    "item_type",
    "item_subtype",
    "department",
    "gender",
    "age_group",
    "size",
    "size_type",
    "us_shoe_size",
    "uk_shoe_size",
    "eu_shoe_size",
    "waist_size",
    "inseam",
    "length",
    "fit",
    "color_primary",
    "color_secondary",
    "material_primary",
    "material_secondary",
    "pattern",
    "closure",
    "occasion",
    "season",
    "style",
    "theme",
    "model_name",
    "product_line",
    "style_code",
    "sport",
    "heel_height",
    "sleeve_length",
    "neckline",
    "embellishment",
    "performance_activity",
    "made_in",
    "vintage_era",
    "release_year",
    "release_year_min",
    "release_year_max",
    "release_year_confidence",
    "condition",
    "condition_detail",
    "has_box",
    "parsed_price",
    "original_price",
]

SYSTEM_PROMPT = (
    "You extract structured apparel resale features from eBay listing text. "
    "Return only valid JSON with exactly these keys: "
    + ", ".join(FEATURE_KEYS)
    + ". "
    "Every value must be a string. Use an empty string when a field is not explicit or not reliable. "
    "Do not invent missing details. "
    "Normalize item_type to a concise lowercase product category such as sneakers, boots, jeans, jacket, hoodie, sweater, shirt, pants, shorts, dress, skirt, leggings, blazer, coat, cleats, sandals, loafers, or shoes. "
    "Use item_subtype for the more specific subtype when available, such as bomber jacket, straight leg jeans, crewneck sweater, running shoes, ankle boots, maxi dress, or polo shirt. "
    "department should be one of footwear, tops, bottoms, outerwear, dresses, accessories, or ''. "
    "Normalize gender to one of: men, women, unisex, boys, girls, infant, toddler, or ''. "
    "Normalize age_group to one of: adult, kids, or ''. "
    "Normalize condition to one of: new, used, or ''. "
    "has_box must be one of: yes, no, or ''. "
    "For year fields, first use an explicit year in the title if one is present. "
    "Only estimate release_year, release_year_min, release_year_max, and release_year_confidence when the title does not explicitly provide a year. "
    "Return release_year as a single best estimated year when there is enough evidence, otherwise ''. "
    "Return release_year_min and release_year_max as the plausible production year range when you can infer a range, otherwise ''. "
    "Return release_year_confidence as a numeric string from 0.00 to 1.00 based on confidence in the year value or estimate, otherwise ''. "
    "Use title, query, and condition text together. "
    "Capture obvious tokens such as size systems, measurements, style code, product line, sport, colors, materials, fit, neckline, sleeve length, and model name when present. "
    "parsed_price should be the numeric sale/listing price taken from price_value or price_text as a plain decimal string (e.g. '49.99'), with no currency symbols or thousands separators, otherwise ''. "
    "original_price should be best estimate of item value if purchased new. Don't leave original_price empty"
)

ROW_INPUT_FIELDS = [
    "item_id",
    "query_clean",
    "title_clean",
    "condition_text_clean",
    "price_value",
    "price_text",
    "sold_date_text",
]

RAW_TO_CLEAN_FIELDS: dict[str, str] = {
    "title": "title_clean",
    "query": "query_clean",
    "condition_text": "condition_text_clean",
}

NULLABLE_TEXT_COLUMNS = [
    "query",
    "title",
    "condition_text",
    "price_text",
    "sold_date_text",
]

def collapse_whitespace(value: object, *, preserve_nulls: bool = False) -> object:
    """Trim a string value and collapse repeated whitespace."""
    if pd.isna(value):
        return value if preserve_nulls else pd.NA

    text = re.sub(r"\s+", " ", str(value).strip())
    if text == "":
        return value if preserve_nulls else pd.NA
    return text

def clean_nullable_text_columns(df: pd.DataFrame) -> None:
    """Trim target text columns and convert empty strings to null."""
    for column in NULLABLE_TEXT_COLUMNS:
        df[column] = df[column].map(collapse_whitespace)

def _clean_text(value: Any, *, preserve_nulls: bool = False) -> str:
    """Apply the shared `collapse_whitespace` rule and coerce to a JSON-safe str."""
    cleaned = collapse_whitespace(value, preserve_nulls=preserve_nulls)
    # collapse_whitespace returns pandas.NA / None for empty values; normalize to "".
    if cleaned is None:
        return ""
    try:
        # pandas.NA is truthy-ambiguous; isna handles it cleanly.
        import pandas as pd
        if pd.isna(cleaned):
            return ""
    except Exception:
        pass
    return str(cleaned)


def clean_document(document: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of `document` with the same cleaning rules used in step 1.

    Mirrors `aidan_data_parsing.parse_latest_exports_csv.build_prepared_dataframe`:
      * Trim & collapse whitespace on NULLABLE_TEXT_COLUMNS.
      * Materialize `title_clean`, `query_clean`, `condition_text_clean`
        (the last preserves nulls, matching the CSV pipeline).
    """
    cleaned = dict(document)

    for column in NULLABLE_TEXT_COLUMNS:
        if column in cleaned:
            cleaned[column] = _clean_text(cleaned[column])

    cleaned["title_clean"] = _clean_text(cleaned.get("title"))
    cleaned["query_clean"] = _clean_text(cleaned.get("query"))
    cleaned["condition_text_clean"] = _clean_text(
        cleaned.get("condition_text"), preserve_nulls=True
    )
    return cleaned

def _json_safe(value: Any) -> Any:
    """Coerce Mongo/pandas-ish values into JSON-serializable primitives."""
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_messages(document: dict[str, Any]) -> list[dict[str, str]]:
    """Build the chat messages payload for a single source document."""
    user_payload = {key: _json_safe(document.get(key, "")) for key in ROW_INPUT_FIELDS}
    user_prompt = (
        "Extract the fields from this row and return JSON only.\n"
        f"{json.dumps(user_payload, ensure_ascii=True)}"
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]


def call_nvidia_chat(
    *, api_key: str, model: str, messages: list[dict[str, str]]
) -> dict[str, Any]:
    """Call the NVIDIA chat/completions endpoint and return the parsed JSON body."""
    response = requests.post(
        f"{NVIDIA_BASE_URL}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": messages,
            "temperature": 0.2,
            "top_p": 0.7,
            "max_tokens": 900,
            "stream": False,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_llm_json(content: str) -> dict[str, str]:
    """Extract a JSON object from the LLM response, stripping code fences."""
    stripped = content.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        match = _JSON_BLOCK_RE.search(stripped)
        if not match:
            raise
        parsed = json.loads(match.group(0))

    if not isinstance(parsed, dict):
        raise ValueError("LLM response was not a JSON object")

    return {key: str(parsed.get(key, "") or "") for key in FEATURE_KEYS}

def _build_mongo_client(uri: str) -> MongoClient:
    """Create a MongoClient that trusts the certifi CA bundle.
    macOS / Homebrew / pyenv Python builds frequently lack a system CA bundle that
    can validate MongoDB Atlas certificates, which surfaces as
    "SSL: TLSV1_ALERT_INTERNAL_ERROR" during the TLS handshake. Forcing
    `tlsCAFile=certifi.where()` resolves that on every platform we run on.
    """
    return MongoClient(
        uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=20000,
    )

# def fetch_documents(
#     *, uri: str, database: str, collection: str, limit: int
# ) -> list[dict[str, Any]]:
#     """Return the most recent `limit` documents from the configured collection."""
#     client = _build_mongo_client(uri)
#     try:
#         client.admin.command("ping")
#         cursor = (
#             client[database][collection]
#             .find()
#             .sort("_id", -1)
#             .limit(limit)
#         )
#         return list(cursor)
#     finally:
#         client.close()

def fetch_documents(
    *,
    uri: str,
    database: str,
    collection: str,
    limit: int,
    retry_errors: bool = False,
) -> list[dict[str, Any]]:
    query = (
        {"parse_status": {"$in": ["pending", "error", None]}}
        if retry_errors
        else {"$or": [
            {"parse_status": {"$exists": False}},
            {"parse_status": "pending"},
        ]}
    )
    client = _build_mongo_client(uri)
    try:
        client.admin.command("ping")
        cursor = (
            client[database][collection]
            .find(query)
            .sort("_id", -1)
            .limit(limit)
        )
        return list(cursor)
    finally:
        client.close()

def mark_processed(
    *, uri: str, database: str, collection: str, updates: list[dict[str, Any]]
) -> None:
    """updates = [{"_id": ObjectId(...), "status": "parsed"|"error", "error": str}]"""
    if not updates:
        return
    ops = [
        UpdateOne(
            {"_id": u["_id"]},
            {"$set": {
                "parse_status": u["status"],
                "parse_error": u.get("error", ""),
                "parsed_at_utc": datetime.now(timezone.utc).isoformat(),
                "llm_model": u.get("model", ""),
            }},
        )
        for u in updates
    ]
    client = _build_mongo_client(uri)
    try:
        client[database][collection].bulk_write(ops, ordered=False)
    finally:
        client.close()

def write_results(path: Path, rows: list[dict[str, Any]]) -> None:
    """Persist extracted feature rows to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def upsert_to_mongo(
    *, uri: str, database: str, collection: str, rows: list[dict[str, Any]]
) -> int:
    """Upsert parsed feature rows into a target Mongo collection by source _id."""
    if not rows:
        return 0
    client = MongoClient(uri)
    try:
        ops = [
            UpdateOne(
                {"_id": row["source_id"]},
                {"$set": row},
                upsert=True,
            )
            for row in rows
            if row.get("source_id") is not None
        ]
        if not ops:
            return 0
        result = client[database][collection].bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Number of documents to pull (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs" / "mongo_llm_features.jsonl",
        help="Path to write extracted features as JSONL",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=INTER_REQUEST_DELAY_SECONDS,
        help="Seconds to sleep between LLM requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip calling the LLM; just pull documents and report.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    api_key = os.getenv("NVIDIA_API_KEY", "").strip()
    model = os.getenv("NVIDIA_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
    mongodb_uri = os.getenv("MONGODB_URI", None).strip()
    database = os.getenv("MONGODB_DATABASE", DEFAULT_DATABASE).strip() or DEFAULT_DATABASE
    source_collection = (
        os.getenv("MONGODB_SOURCE_COLL", DEFAULT_SOURCE_COLLECTION).strip()
        or DEFAULT_SOURCE_COLLECTION
    )
    target_collection = os.getenv("MONGODB_TARGET_COLL", "").strip()

    if not args.dry_run and not api_key:
        print("ERROR: NVIDIA_API_KEY is not set in the environment.", file=sys.stderr)
        return 2

    print(f"Connecting to MongoDB database '{database}', collection '{source_collection}'...")
    try:
        documents = fetch_documents(
            uri=mongodb_uri,
            database=database,
            collection=source_collection,
            limit=args.limit,
        )
    except PyMongoError as exc:
        print(f"ERROR: MongoDB request failed: {exc}", file=sys.stderr)
        return 3

    print(f"Pulled {len(documents)} document(s) from MongoDB.")
    if not documents:
        print("Nothing to process. Exiting.")
        return 0

    results: list[dict[str, Any]] = []
    raw_updates: list[dict[str, Any]] = []
    
    for idx, document in enumerate(documents, start=1):
        source_id = document.get("_id")
        item_id = document.get("item_id", "")
        cleaned_doc = clean_document(document)
        base_row: dict[str, Any] = {
        "source_id": str(source_id) if source_id is not None else None,
        "item_id": _json_safe(item_id),
        "llm_model": model,
        "parsed_at_utc": datetime.now(timezone.utc).isoformat(),
        # Optional: store the cleaned text alongside the parsed features so
        # downstream consumers can audit what was actually sent to the LLM.
        "title_clean": cleaned_doc.get("title_clean", ""),
        "query_clean": cleaned_doc.get("query_clean", ""),
        "condition_text_clean": cleaned_doc.get("condition_text_clean", ""),
    }

        if args.dry_run:
            results.append({**base_row, "parse_status": "dry_run"})
            continue

        try:
            messages = build_messages(cleaned_doc)
            time.sleep(INTER_REQUEST_DELAY_SECONDS)
            payload = call_nvidia_chat(api_key=api_key, model=model, messages=messages)
            content = payload["choices"][0]["message"]["content"]
            features = parse_llm_json(content)
            row = {
                **base_row,
                **features,
                "llm_response_text": content,
                "parse_status": "ok",
                "parse_error": "",
            }
            if source_id is not None:
                raw_updates.append({"_id": source_id, "status": "parsed", "model": model})
        
        except Exception as exc:  # noqa: BLE001 - want to record any failure
            row = {
                **base_row,
                "parse_status": "error",
                "parse_error": f"{type(exc).__name__}: {exc}",
            }
            print(f"  [{idx}/{len(documents)}] item_id={item_id} FAILED: {exc}", file=sys.stderr)
            if source_id is not None:
                raw_updates.append({
                    "_id": source_id,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "model": model,
                })
        else:
            print(
                f"  [{idx}/{len(documents)}] item_id={item_id} -> "
                f"brand='{features.get('brand_name','')}' "
                f"type='{features.get('item_type','')}'"
            )

        results.append(row)

        if idx < len(documents) and args.delay > 0:
            time.sleep(args.delay)

    write_results(args.output, results)
    print(f"Wrote {len(results)} row(s) to {args.output}")

    if target_collection and not args.dry_run:
        try:
            written = upsert_to_mongo(
                uri=mongodb_uri,
                database=database,
                collection=target_collection,
                rows=results,
            )
            print(f"Upserted {written} row(s) into '{database}.{target_collection}'")
        except PyMongoError as exc:
            print(f"WARNING: Failed to upsert results to MongoDB: {exc}", file=sys.stderr)

    if raw_updates and not args.dry_run:
        try:
            mark_processed(
                uri=mongodb_uri,
                database=database,
                collection=source_collection,
                updates=raw_updates,
            )
            ok_count = sum(1 for u in raw_updates if u["status"] == "parsed")
            err_count = len(raw_updates) - ok_count
            print(
                f"Marked {len(raw_updates)} raw row(s): "
                f"{ok_count} parsed, {err_count} error"
            )
        except PyMongoError as exc:
            print(f"WARNING: Failed to mark raw rows as processed: {exc}", file=sys.stderr)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
