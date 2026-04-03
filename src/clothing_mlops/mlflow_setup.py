"""Central place for MLflow tracking URI and experiment name."""

from __future__ import annotations

import os
from pathlib import Path

import mlflow

# Default: file store under project root (./mlruns). Override with MLFLOW_TRACKING_URI.
_DEFAULT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRACKING_URI = f"file:{_DEFAULT_ROOT / 'mlruns'}"

EXPERIMENT_NAME = os.environ.get(
    "MLFLOW_EXPERIMENT_NAME",
    "item-value-prediction",
)


def get_tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)


def set_experiment(name: str | None = None) -> mlflow.entities.Experiment:
    """Point MLflow at the backend and ensure the experiment exists."""
    mlflow.set_tracking_uri(get_tracking_uri())
    return mlflow.set_experiment(name or EXPERIMENT_NAME)
