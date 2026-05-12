"""Model training and loading helpers for the clothing value service."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import mlflow
import mlflow.pyfunc
import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

from clothing_mlops.data_pipeline import feature_columns, target_column
from clothing_mlops.mlflow_setup import set_experiment

DEFAULT_MODEL_NAME = "clothing-value-model"


def _tracking_root_from_uri(tracking_uri: str | None) -> Path | None:
    if not tracking_uri:
        return None
    parsed = urlparse(tracking_uri)
    if parsed.scheme != "file":
        return None
    return Path(parsed.path)


def _registry_version_meta_path(model_name: str, version: str, tracking_root: Path) -> Path:
    return tracking_root / "models" / model_name / f"version-{version}" / "meta.yaml"


def _resolve_registry_model_version(model_name: str, model_version: str, tracking_root: Path) -> str | None:
    if model_version != "latest":
        meta_path = _registry_version_meta_path(model_name, model_version, tracking_root)
        return model_version if meta_path.exists() else None

    versions_dir = tracking_root / "models" / model_name
    version_numbers = sorted(
        int(path.name.removeprefix("version-"))
        for path in versions_dir.glob("version-*")
        if path.is_dir() and path.name.removeprefix("version-").isdigit()
    )
    if not version_numbers:
        return None
    return str(version_numbers[-1])


def _rebased_local_registry_model_uri(model_uri: str, tracking_root: Path) -> str | None:
    if not model_uri.startswith("models:/"):
        return None

    model_ref = model_uri.removeprefix("models:/")
    try:
        model_name, model_version = model_ref.rsplit("/", 1)
    except ValueError:
        return None

    resolved_version = _resolve_registry_model_version(model_name, model_version, tracking_root)
    if resolved_version is None:
        return None

    meta_path = _registry_version_meta_path(model_name, resolved_version, tracking_root)
    if not meta_path.exists():
        return None

    metadata = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
    source = metadata.get("source") or metadata.get("storage_location")
    if not isinstance(source, str) or not source.startswith("file://"):
        return None

    source_path = Path(urlparse(source).path)
    if source_path.exists():
        return source

    marker = f"/{tracking_root.name}/"
    source_text = source_path.as_posix()
    if marker not in source_text:
        return None

    relative_suffix = source_text.split(marker, 1)[1]
    rebased_path = tracking_root / relative_suffix
    return rebased_path.as_uri() if rebased_path.exists() else None


def build_training_pipeline() -> Pipeline:
    numeric_features = ["listing_price", "shipping_price"]
    categorical_features = [
        "brand",
        "category",
        "size",
        "condition",
        "color",
        "material",
    ]
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                categorical_features,
            ),
            ("numeric", "passthrough", numeric_features),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                XGBRegressor(
                    n_estimators=300,
                    max_depth=6,
                    learning_rate=0.05,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    random_state=42,
                    n_jobs=-1,
                    tree_method="hist",
                ),
            ),
        ]
    )


def train_and_log_model(
    dataset_path: Path,
    registered_model_name: str | None = DEFAULT_MODEL_NAME,
) -> dict[str, Any]:
    dataset_path = Path(dataset_path)
    df = pd.read_csv(dataset_path)
    X = df[feature_columns()]
    y = df[target_column()]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    set_experiment()
    pipeline = build_training_pipeline()

    with mlflow.start_run(run_name="sold-listings-xgboost") as run:
        pipeline.fit(X_train, y_train)
        predictions = pipeline.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        rmse = mean_squared_error(y_test, predictions) ** 0.5
        model_registry_status = "registered"

        mlflow.log_params(
            {
                "dataset_path": str(dataset_path),
                "feature_count": len(feature_columns()),
                "feature_set_version": "v2_xgboost",
                "registered_model_name": registered_model_name or "none",
                "record_count": len(df),
                "date_min": df["sold_date"].min(),
                "date_max": df["sold_date"].max(),
            }
        )
        mlflow.log_metrics({"mae": mae, "rmse": rmse})
        mlflow.log_dict(
            {
                "feature_columns": feature_columns(),
                "target_column": target_column(),
            },
            "feature_contract.json",
        )

        try:
            model_info = mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                registered_model_name=registered_model_name,
                input_example=X.head(3),
            )
        except Exception:
            model_registry_status = "artifact_only"
            model_info = mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                input_example=X.head(3),
            )

    model_uri = model_info.model_uri
    if registered_model_name:
        model_uri = os.environ.get(
            "MLFLOW_SERVING_MODEL_URI",
            f"models:/{registered_model_name}/latest",
        )
        if model_registry_status != "registered":
            model_uri = model_info.model_uri

    return {
        "run_id": run.info.run_id,
        "dataset_path": str(dataset_path),
        "mae": float(mae),
        "rmse": float(rmse),
        "model_uri": model_uri,
        "model_registry_status": model_registry_status,
    }


def load_serving_model() -> tuple[Any | None, str, str | None]:
    model_uri = os.environ.get(
        "MLFLOW_SERVING_MODEL_URI",
        f"models:/{os.environ.get('MLFLOW_MODEL_NAME', DEFAULT_MODEL_NAME)}/latest",
    )
    try:
        model = mlflow.pyfunc.load_model(model_uri)
        return model, "ok", None
    except Exception as exc:  # pragma: no cover - exercised in runtime health path
        tracking_root = _tracking_root_from_uri(mlflow.get_tracking_uri())
        rebased_model_uri = (
            _rebased_local_registry_model_uri(model_uri, tracking_root)
            if tracking_root is not None
            else None
        )
        if rebased_model_uri is not None:
            try:
                model = mlflow.pyfunc.load_model(rebased_model_uri)
                return model, "ok", None
            except Exception as rebased_exc:  # pragma: no cover - exercised in runtime health path
                return None, "error", str(rebased_exc)
        return None, "error", str(exc)
