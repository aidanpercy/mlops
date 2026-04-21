from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    queries: list[str]
    clothing_csv: Path | None
    clothing_items_per_run: int
    clothing_cursor_path: Path
    github_push_enabled: bool
    github_push_remote: str
    github_push_branch: str
    ebay_domain: str
    ebay_location: str
    max_pages_per_query: int
    request_timeout_seconds: int
    sleep_between_requests_seconds: float
    schedule_hour: int
    schedule_minute: int
    db_path: Path
    export_dir: Path
    cookies_file: Path | None


def _parse_queries(value: str) -> list[str]:
    values = [q.strip() for q in value.split(",")]
    return [q for q in values if q]


def load_settings() -> Settings:
    load_dotenv()

    project_root = Path(__file__).resolve().parents[2]
    db_path = project_root / "data" / "ebay_historical.db"
    export_dir = project_root / "data" / "exports"

    queries = _parse_queries(
        os.getenv(
            "EBAY_QUERIES",
            "women dress,men jacket,vintage jeans,sneakers",
        )
    )
    if not queries:
        queries = ["women dress"]

    cookies_raw = os.getenv("EBAY_COOKIES_FILE", "").strip()
    cookies_file: Path | None = None
    if cookies_raw:
        p = Path(cookies_raw).expanduser()
        if not p.is_absolute():
            p = project_root / p
        cookies_file = p

    use_catalog = os.getenv("EBAY_USE_CLOTHING_CATALOG", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )
    clothing_csv: Path | None = None
    catalog_raw = os.getenv("CLOTHING_CSV", "").strip()
    if use_catalog:
        # Try explicit path first; if invalid, fall back to default catalog paths.
        if catalog_raw:
            cp = Path(catalog_raw).expanduser()
            candidates = [cp] if cp.is_absolute() else [project_root / cp, Path.cwd() / cp]
            for candidate in candidates:
                if candidate.is_file():
                    clothing_csv = candidate
                    break

        if clothing_csv is None:
            for candidate in (project_root / "clothing.csv", project_root / "data" / "clothing.csv"):
                if candidate.is_file():
                    clothing_csv = candidate
                    break

    clothing_items_per_run = max(
        1,
        int(os.getenv("CLOTHING_ITEMS_PER_RUN", "5")),
    )
    clothing_cursor_path = project_root / "data" / "clothing_scrape_cursor.txt"
    github_push_enabled = os.getenv("GITHUB_PUSH_EXPORTS", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    github_push_remote = os.getenv("GITHUB_PUSH_REMOTE", "origin").strip() or "origin"
    github_push_branch = os.getenv("GITHUB_PUSH_BRANCH", "main").strip() or "main"

    return Settings(
        queries=queries,
        clothing_csv=clothing_csv,
        clothing_items_per_run=clothing_items_per_run,
        clothing_cursor_path=clothing_cursor_path,
        github_push_enabled=github_push_enabled,
        github_push_remote=github_push_remote,
        github_push_branch=github_push_branch,
        ebay_domain=os.getenv("EBAY_DOMAIN", "www.ebay.com"),
        ebay_location=os.getenv("EBAY_LOCATION", "1"),
        max_pages_per_query=int(os.getenv("MAX_PAGES_PER_QUERY", "3")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        sleep_between_requests_seconds=float(
            os.getenv("SLEEP_BETWEEN_REQUESTS_SECONDS", "2")
        ),
        schedule_hour=int(os.getenv("SCHEDULE_HOUR", "2")),
        schedule_minute=int(os.getenv("SCHEDULE_MINUTE", "0")),
        db_path=db_path,
        export_dir=export_dir,
        cookies_file=cookies_file,
    )
