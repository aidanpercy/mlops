from __future__ import annotations

import time
from datetime import datetime, timedelta

from .config import load_settings
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
            print(
                "Run complete | "
                f"fetched={result['fetched']} "
                f"inserted_new={result['inserted_new']} "
                f"duplicates_ignored={result['duplicates_ignored']}"
            )
        except Exception as exc:
            print(f"Run failed: {exc}")


if __name__ == "__main__":
    run_daily_forever()
