#!/usr/bin/env python3
"""
Train a Random Forest regressor on cleaned eBay data to predict sold price
from brand_name, item_type, and condition.

Usage (from repo root):
  python scripts/train_price_rf.py
  python scripts/train_price_rf.py --data ebay_historical_clothing_scraper/data/processed/ebay_historical_cleaned.parquet

Outputs:
  - Logs MAE / RMSE to MLflow (same experiment as clothing_mlops)
  - Saves sklearn Pipeline to models/ebay_price_rf.joblib
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import mlflow
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from clothing_mlops.mlflow_setup import set_experiment  # noqa: E402

FEATURE_COLUMNS = ["brand_name", "item_type", "condition"]
TARGET_COLUMN = "price"
DEFAULT_DATA_CSV = ROOT / "ebay_historical_clothing_scraper/data/processed/ebay_historical_cleaned.csv"
DEFAULT_MODEL_OUT = ROOT / "models/ebay_price_rf.joblib"


def build_pipeline(n_estimators: int, max_depth: int | None, random_state: int) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                FEATURE_COLUMNS,
            ),
        ],
        remainder="drop",
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                RandomForestRegressor(
                    n_estimators=n_estimators,
                    max_depth=max_depth,
                    random_state=random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def load_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Dataset missing column {col!r}: {path}")
    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Dataset missing target {TARGET_COLUMN!r}: {path}")
    return df


def prepare_xy(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    work = df[FEATURE_COLUMNS + [TARGET_COLUMN]].copy()
    work[TARGET_COLUMN] = pd.to_numeric(work[TARGET_COLUMN], errors="coerce")
    work = work.dropna(subset=[TARGET_COLUMN])
    work = work[work[TARGET_COLUMN] > 0]
    for col in FEATURE_COLUMNS:
        work[col] = work[col].fillna("unknown").astype(str).str.strip()
        work.loc[work[col] == "", col] = "unknown"
    X = work[FEATURE_COLUMNS]
    y = work[TARGET_COLUMN]
    return X, y


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train RF price model on cleaned eBay data.")
    p.add_argument(
        "--data",
        type=Path,
        default=DEFAULT_DATA_CSV,
        help="Path to cleaned CSV or Parquet.",
    )
    p.add_argument(
        "--model-out",
        type=Path,
        default=DEFAULT_MODEL_OUT,
        help="Where to save the fitted sklearn Pipeline (joblib).",
    )
    p.add_argument("--test-size", type=float, default=0.2)
    p.add_argument("--random-state", type=int, default=42)
    p.add_argument("--n-estimators", type=int, default=200)
    p.add_argument("--max-depth", type=int, default=None, help="Optional tree max_depth.")
    p.add_argument("--no-mlflow", action="store_true", help="Skip MLflow logging.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    data_path = args.data
    if not data_path.is_absolute():
        data_path = ROOT / data_path
    if not data_path.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found: {data_path}. Run: python scripts/clean_ebay_exports.py"
        )

    df = load_dataset(data_path)
    X, y = prepare_xy(df)
    if len(X) < 50:
        raise ValueError(f"Too few rows with valid price after cleaning: {len(X)}")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=args.random_state,
    )

    pipeline = build_pipeline(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=args.random_state,
    )
    pipeline.fit(X_train, y_train)
    predictions = pipeline.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    rmse = mean_squared_error(y_test, predictions) ** 0.5

    model_out = args.model_out
    if not model_out.is_absolute():
        model_out = ROOT / model_out
    model_out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, model_out)

    if not args.no_mlflow:
        set_experiment()
        with mlflow.start_run(run_name="ebay-price-random-forest") as run:
            mlflow.set_tags(
                {
                    "task": "sold_price_regression",
                    "data_source": "ebay_historical_cleaned",
                    "model_family": "random_forest",
                }
            )
            mlflow.log_params(
                {
                    "dataset_path": str(data_path),
                    "n_rows_train": len(X_train),
                    "n_rows_test": len(X_test),
                    "n_estimators": args.n_estimators,
                    "max_depth": args.max_depth if args.max_depth is not None else "None",
                    "features": ",".join(FEATURE_COLUMNS),
                    "target": TARGET_COLUMN,
                }
            )
            mlflow.log_metrics({"mae": float(mae), "rmse": float(rmse), "r2_holdout": float(pipeline.score(X_test, y_test))})
            mlflow.log_artifact(str(model_out), artifact_path="local_model")
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                input_example=X.head(3),
            )

    print(f"Rows used: {len(X)} (train {len(X_train)}, test {len(X_test)})")
    print(f"MAE:  {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"Saved pipeline: {model_out}")


if __name__ == "__main__":
    main()
