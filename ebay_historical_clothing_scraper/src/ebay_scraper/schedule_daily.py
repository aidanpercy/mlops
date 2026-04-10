from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

from .config import load_settings
from .github_sync import commit_and_push_csv_export
from .runner import run_once


def _next_run_time(hour: int, minute: int) -> datetime:
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def run_daily_forever() -> None:
    settings = load_settings()
    print(
        f"Scheduler started. Will run daily at "
        f"{settings.schedule_hour:02d}:{settings.schedule_minute:02d}."
    )
    while True:
        next_run = _next_run_time(settings.schedule_hour, settings.schedule_minute)
        sleep_seconds = max(1.0, (next_run - datetime.now()).total_seconds())
        print(f"Next run at {next_run.isoformat()}")
        time.sleep(sleep_seconds)

        try:
            result = run_once()
            extra = ""
            if result.get("clothing_catalog"):
                extra = (
                    f" | clothing_csv batch "
                    f"start_idx={result['clothing_catalog_start_index']} "
                    f"next_cursor={result['clothing_catalog_next_cursor']}"
                )
            print(
                "Run complete | "
                f"fetched={result['fetched']} "
                f"inserted_new={result['inserted_new']} "
                f"duplicates_ignored={result['duplicates_ignored']}"
                f"{extra}"
            )
            print("Queries: " + "; ".join(result["queries"]))
            if settings.github_push_enabled:
                try:
                    status = commit_and_push_csv_export(
                        csv_path=Path(result["csv_path"]),
                        remote=settings.github_push_remote,
                        branch=settings.github_push_branch,
                    )
                    print(
                        "GitHub export sync complete | "
                        f"status={status} "
                        f"remote={settings.github_push_remote} "
                        f"branch={settings.github_push_branch}"
                    )
                except Exception as push_exc:
                    print(f"GitHub export sync failed: {push_exc}")
        except Exception as exc:
            print(f"Run failed: {exc}")


if __name__ == "__main__":
    run_daily_forever()
