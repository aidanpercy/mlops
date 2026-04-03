#!/usr/bin/env python3
"""
Smoke test: logs a run to MLflow. Replace with real training when eBay data
is collected.

Usage (from repo root):
  python scripts/train_placeholder.py

In another terminal, browse runs:
  mlflow ui --backend-store-uri file:./mlruns
"""

from __future__ import annotations

import random

import mlflow

from clothing_mlops.mlflow_setup import set_experiment


def main() -> None:
    set_experiment()
    with mlflow.start_run(run_name="placeholder-baseline"):
        mlflow.set_tags(
            {
                "stage": "placeholder",
                "data_source": "none_yet",
                "task": "clothing_depreciation_appreciation",
            }
        )
        mlflow.log_params(
            {
                "model": "dummy_linear_placeholder",
                "random_seed": 42,
            }
        )
        # Fake metrics until you have a train/val split from eBay listings
        mae = round(random.uniform(8.0, 15.0), 4)
        rmse = round(mae * 1.2, 4)
        mlflow.log_metrics({"mae": mae, "rmse": rmse})
    print("Logged one run. Start UI: mlflow ui --backend-store-uri file:./mlruns")


if __name__ == "__main__":
    main()
