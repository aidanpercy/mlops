from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _run_git(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def commit_and_push_csv_export(
    csv_path: Path,
    remote: str,
    branch: str,
) -> str:
    """
    Commit a newly exported CSV and push it to GitHub.
    Returns one of:
      - "pushed"
      - "no_changes"
    """
    repo_root = Path(__file__).resolve().parents[2]
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV export not found: {csv_path}")
    resolved_csv = csv_path.resolve()
    try:
        relative_csv = resolved_csv.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(
            f"CSV export is outside repository: {resolved_csv}"
        ) from exc

    _run_git(repo_root, ["add", str(relative_csv)])
    diff_result = subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "diff",
            "--cached",
            "--quiet",
            "--",
            str(relative_csv),
        ],
        capture_output=True,
        text=True,
    )
    if diff_result.returncode == 0:
        return "no_changes"
    if diff_result.returncode != 1:
        raise RuntimeError(diff_result.stderr.strip() or "git diff failed")

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    message = f"chore(data): add nightly ebay export {stamp}"
    _run_git(repo_root, ["commit", "-m", message, "--", str(relative_csv)])
    _run_git(repo_root, ["push", remote, f"HEAD:{branch}"])
    return "pushed"
