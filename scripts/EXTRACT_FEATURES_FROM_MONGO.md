# `extract_features_from_mongo.py`

Pulls unprocessed listing documents from MongoDB, cleans them using the shared
`aidan_data_parsing` helpers, calls an NVIDIA-hosted LLM to extract structured
apparel resale features per row, writes the results to a JSONL file, and
optionally upserts them into a target MongoDB collection. On success or failure,
the corresponding source row in MongoDB is marked with a `parse_status` so the
next run only picks up rows that haven't been processed yet.

## Pipeline at a glance

```
MongoDB (raw) --> clean_document() --> NVIDIA chat/completions
                                           |
                                           v
                           parse_llm_json() -> FEATURE_KEYS dict
                                           |
                 +-------------------------+-------------------------+
                 v                         v                         v
       JSONL file on disk      Upsert into target collection   Mark raw row
    (scripts/outputs/*.jsonl)  (when MONGODB_TARGET_COLL set)   parsed/error
```

Cleaning logic is imported directly from
`aidan_data_parsing/parse_latest_exports_csv.py` (`collapse_whitespace`,
`NULLABLE_TEXT_COLUMNS`) so the prompt inputs match the CSV-based pipeline.

## Requirements

Install from the repo root:

```bash
pip install -r requirements.txt
```

This script specifically needs `pymongo`, `certifi`, `requests`, `python-dotenv`,
and `pandas` (for the cleaning helpers).

## Environment variables

Loaded from the repo-root `.env` via `python-dotenv`.

| Variable              | Required                 | Default                                  | Notes |
|-----------------------|--------------------------|------------------------------------------|-------|
| `NVIDIA_API_KEY`      | Yes (unless `--dry-run`) | —                                        | API key for `https://integrate.api.nvidia.com`. |
| `NVIDIA_MODEL`        | No                       | `meta/llama-3.3-70b-instruct`            | Any NVIDIA-hosted chat model. |
| `MONGODB_URI`         | No                       | read-only `service_account` Atlas URI    | Use a `readWrite` user if you want `parse_status` / target-collection writes. |
| `MONGODB_DATABASE`    | No                       | `historical`                             | Database name. |
| `MONGODB_SOURCE_COLL` | No                       | `raw`                                    | Collection the script reads from. |
| `MONGODB_TARGET_COLL` | No                       | *(unset)*                                | When set, parsed features are upserted here keyed by source `_id`. |

## What counts as "unprocessed"

Default query:

```python
{"$or": [
    {"parse_status": {"$exists": False}},
    {"parse_status": "pending"},
]}
```

With `retry_errors=True` inside `fetch_documents`, the query widens to:

```python
{"parse_status": {"$in": ["pending", "error", None]}}
```

Backfill existing raw docs so they become eligible:

```js
use historical
db.raw.updateMany(
  { parse_status: { $exists: false } },
  { $set: { parse_status: "pending" } }
)
```

## CLI

```bash
python scripts/extract_features_from_mongo.py [--limit N] [--output PATH] [--delay SECONDS] [--dry-run]
```

| Flag        | Default                                             | Purpose |
|-------------|-----------------------------------------------------|---------|
| `--limit`   | `100`                                               | Max documents to pull / process per run. |
| `--output`  | `scripts/outputs/mongo_llm_features.jsonl`          | JSONL file written locally. |
| `--delay`   | `2.5`                                               | Seconds to sleep between LLM calls (rate limiting). |
| `--dry-run` | off                                                 | Skip LLM calls and any Mongo writes; just pull and report. |

### Examples

```bash
# Smallest possible smoke test (5 rows, no LLM, no writes)
python scripts/extract_features_from_mongo.py --limit 5 --dry-run

# Default: 100 unprocessed rows, JSONL output, raw rows marked `parsed` / `error`
python scripts/extract_features_from_mongo.py

# Full pipeline with writeback into historical.parsed
MONGODB_TARGET_COLL=parsed python scripts/extract_features_from_mongo.py --limit 200
```

## Outputs

### Local JSONL

Written to `--output` (default `scripts/outputs/mongo_llm_features.jsonl`). Each
line is one row with, at minimum:

- `source_id` — string form of the MongoDB `_id` for the raw doc.
- `item_id` — eBay item id.
- `title_clean`, `query_clean`, `condition_text_clean` — cleaned input sent to the LLM.
- All `FEATURE_KEYS` (brand, item type, size, colors, etc.), each as a string.
- `llm_model`, `llm_response_text`, `parsed_at_utc`.
- `parse_status` ∈ `{ok, error, dry_run}`, `parse_error`.

### Target MongoDB collection (optional)

When `MONGODB_TARGET_COLL` is set, the same rows are upserted into
`<MONGODB_DATABASE>.<MONGODB_TARGET_COLL>` with `_id == source_id` (string).

### Source collection status updates

On a non-dry-run, each processed raw row is updated with:

```js
{
  parse_status: "parsed" | "error",
  parse_error:  "" | "<exception message>",
  parsed_at_utc: "<ISO 8601 UTC>",
  llm_model:    "<model name>"
}
```

This is what makes subsequent runs only fetch rows you haven't seen before.

## Verification after a run

```js
use historical

// Per-status counts
db.raw.countDocuments({ parse_status: "pending" })
db.raw.countDocuments({ parse_status: "parsed" })
db.raw.countDocuments({ parse_status: "error" })

// Spot check the latest parsed rows
db.raw.find({ parse_status: "parsed" }).sort({ parsed_at_utc: -1 }).limit(3).pretty()

// If using MONGODB_TARGET_COLL=parsed:
db.parsed.estimatedDocumentCount()
db.parsed.find().sort({ parsed_at_utc: -1 }).limit(3).pretty()
```

## Common issues

- **`SSL: TLSV1_ALERT_INTERNAL_ERROR` on connect.** The client ships with
  `tlsCAFile=certifi.where()`; if it still fails, check the Atlas IP allowlist
  and that the cluster isn't paused (free-tier clusters auto-pause).
- **All features come back empty.** The LLM only has what `clean_document()`
  gives it. If `title_clean` / `query_clean` are empty, the source doc doesn't
  have `title` / `query` either — fix upstream, not here.
- **`parse_status` is never updated.** The configured `MONGODB_URI` is
  read-only. Switch to a credential with `readWrite` on `historical`.
- **Duplicate work across concurrent runs.** `find().limit(N)` + "update after"
  is racy across workers. For multi-worker setups, swap in a
  `find_one_and_update` claim loop that transitions `pending -> in_progress`
  atomically.

## File layout

```
scripts/
  extract_features_from_mongo.py
  outputs/
    mongo_llm_features.jsonl   # default output path
aidan_data_parsing/
  parse_latest_exports_csv.py  # source of shared cleaning helpers
```

## Related scripts

- `aidan_data_parsing/parse_latest_exports_csv.py` — step 1 of the CSV parsing
  pipeline; defines the canonical cleaning rules that this script reuses.
- `aidan_data_parsing/parse_one_pending_row.py` — step 2 of the CSV pipeline;
  same FEATURE_KEYS, same NVIDIA model, but driven by CSV state instead of
  MongoDB state.
