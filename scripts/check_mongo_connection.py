#!/usr/bin/env python3
"""Quick connectivity smoke test for the MongoDB training source.

Reads MONGODB_URI / MONGODB_DATABASE / MONGODB_PARSED_COLL from either the
environment or a repo-root ``.env`` file (auto-loaded via python-dotenv) and
prints just enough information to confirm the trainer's ``--from-mongo`` path
will work. Never echoes the password back to stdout.

Run from the repo root:

    python scripts/check_mongo_connection.py
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlsplit, urlunsplit

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError


def _redact(uri: str) -> str:
    """Return the URI with the password replaced by ``***`` for safe printing."""
    try:
        parts = urlsplit(uri)
        netloc = parts.netloc
        if "@" in netloc:
            creds, host = netloc.rsplit("@", 1)
            if ":" in creds:
                user, _pw = creds.split(":", 1)
                netloc = f"{user}:***@{host}"
            else:
                netloc = f"{creds}@{host}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "mongodb+srv://***"


def main() -> int:
    load_dotenv()

    uri = (os.getenv("MONGODB_URI") or "").strip()
    if not uri:
        print(
            "ERROR: MONGODB_URI is not set. Add it to .env or export it.",
            file=sys.stderr,
        )
        return 1

    database = (os.getenv("MONGODB_DATABASE") or "historical").strip() or "historical"
    parsed_coll = (os.getenv("MONGODB_PARSED_COLL") or "").strip()
    target_coll = (os.getenv("MONGODB_TARGET_COLL") or "").strip()
    collection = parsed_coll or target_coll or "parsed"

    print(f"URI:        {_redact(uri)}")
    print(f"Database:   {database}")
    print(f"Collection: {collection}")

    try:
        client = MongoClient(
            uri,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=20000,
        )
        client.admin.command("ping")
    except OperationFailure as exc:
        print(f"AUTH FAILED: {exc}", file=sys.stderr)
        print(
            "Most likely causes: wrong DB username, wrong DB password, or the "
            "user does not have access to this cluster. Re-check Atlas → "
            "Database Access.",
            file=sys.stderr,
        )
        return 2
    except PyMongoError as exc:
        print(f"CONNECT FAILED: {exc}", file=sys.stderr)
        print(
            "Check Atlas → Network Access (your IP must be allowlisted) and "
            "that the cluster is not paused.",
            file=sys.stderr,
        )
        return 3

    print("ping: ok")
    coll = client[database][collection]
    total = coll.estimated_document_count()
    ok_rows = coll.count_documents({"parse_status": "ok"})
    any_price = coll.count_documents(
        {"parsed_price": {"$nin": ["", None]}}
    )
    print(f"documents in {database}.{collection}: {total}")
    print(f"  parse_status='ok':                   {ok_rows}")
    print(f"  any non-empty parsed_price:          {any_price}")

    if ok_rows == 0 and any_price == 0:
        print(
            "\nNo usable training rows yet. Run "
            "scripts/extract_features_from_mongo.py with "
            "MONGODB_TARGET_COLL=parsed (or another writer) to populate this "
            "collection.",
            file=sys.stderr,
        )
        return 4

    if ok_rows == 0:
        print(
            "\nNo rows have parse_status='ok' yet, but some have a parsed_price. "
            "Train with: python scripts/train_price_rf.py --from-mongo "
            "--mongo-relaxed-filter"
        )
    else:
        print(
            "\nReady to train: python scripts/train_price_rf.py --from-mongo"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
