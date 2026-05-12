#!/usr/bin/env python3
"""
Train an XGBoost regressor on cleaned eBay data to predict sold price from
brand_name, item_type, condition, initial_price, and initial_price_catalog_item
(catalog match from clean_ebay_exports.attach_initial_prices).

Usage (from repo root):
  # Train on the cleaned CSV (default)
  python scripts/train_price_rf.py

  # Train on a specific cleaned parquet/CSV
  python scripts/train_price_rf.py --data ebay_historical_clothing_scraper/data/processed/ebay_historical_cleaned.parquet

  # Train directly on MongoDB-parsed listings (larger dataset).
  # Requires MONGODB_URI in the environment or via --mongo-uri.
  python scripts/train_price_rf.py --from-mongo
  python scripts/train_price_rf.py --from-mongo --mongo-limit 5000

Outputs:
  - Logs MAE / RMSE to MLflow (same experiment as clothing_mlops)
  - Saves sklearn Pipeline to models/ebay_price_xgb.joblib
  - With --from-mongo, optionally caches the assembled training frame to
    --mongo-cache-csv so you can inspect / reuse the exact rows used.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import joblib
import mlflow
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from clothing_mlops.mlflow_setup import set_experiment  # noqa: E402


def _load_mongo_export_module():
    """Import scripts/export_mongo_parsed_for_training.py without requiring a
    package layout under ``scripts/``."""
    spec = importlib.util.spec_from_file_location(
        "export_mongo_parsed_for_training",
        ROOT / "scripts" / "export_mongo_parsed_for_training.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


CAT_LOW_CARD_FEATURES = [
    "brand_name",
    "item_type",
    "item_subtype",
    "department",
    "gender",
    "age_group",
    "size_type",
    "color_primary",
    "color_secondary",
    "material_primary",
    "material_secondary",
    "pattern",
    "closure",
    "fit",
    "sleeve_length",
    "neckline",
    "style",
    "occasion",
    "season",
    "sport",
    "has_box",
    "condition",
    "condition_detail",
]
CAT_HIGH_CARD_FEATURES = [
    "size",
    "model_name",
    "product_line",
    "style_code",
    "initial_price_catalog_item",
]
CAT_FEATURES = CAT_LOW_CARD_FEATURES + CAT_HIGH_CARD_FEATURES
NUM_FEATURES = ["initial_price", "original_price", "release_year"]
FEATURE_COLUMNS = CAT_FEATURES + NUM_FEATURES
TARGET_COLUMN = "price"
DEFAULT_DATA_CSV = ROOT / "ebay_historical_clothing_scraper/data/processed/ebay_historical_cleaned.csv"
DEFAULT_MODEL_OUT = ROOT / "models/ebay_price_xgb.joblib"

HIGH_CARD_MIN_FREQUENCY = 10


def build_pipeline(
    *,
    n_estimators: int,
    max_depth: int | None,
    learning_rate: float,
    subsample: float,
    colsample_bytree: float,
    random_state: int,
    objective: str,
    min_child_weight: float,
    reg_alpha: float,
    reg_lambda: float,
    early_stopping_rounds: int | None,
) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical_low",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CAT_LOW_CARD_FEATURES,
            ),
            (
                "categorical_high",
                OneHotEncoder(
                    handle_unknown="infrequent_if_exist",
                    sparse_output=False,
                    min_frequency=HIGH_CARD_MIN_FREQUENCY,
                ),
                CAT_HIGH_CARD_FEATURES,
            ),
            (
                "numeric",
                SimpleImputer(strategy="median"),
                NUM_FEATURES,
            ),
        ],
        remainder="drop",
    )
    xgb_kwargs: dict[str, object] = dict(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        random_state=random_state,
        n_jobs=-1,
        tree_method="hist",
        objective=objective,
        min_child_weight=min_child_weight,
        reg_alpha=reg_alpha,
        reg_lambda=reg_lambda,
    )
    if early_stopping_rounds is not None and early_stopping_rounds > 0:
        xgb_kwargs["early_stopping_rounds"] = early_stopping_rounds
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", XGBRegressor(**xgb_kwargs)),
        ]
    )


def load_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    for col in ("brand_name", "item_type", "condition"):
        if col not in df.columns:
            raise ValueError(f"Dataset missing column {col!r}: {path}")
    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Dataset missing target {TARGET_COLUMN!r}: {path}")
    df = df.copy()
    for col in CAT_FEATURES:
        if col not in df.columns:
            df[col] = pd.NA
    for col in NUM_FEATURES:
        if col not in df.columns:
            df[col] = float("nan")
    return df


def prepare_xy(
    df: pd.DataFrame,
    *,
    clip_quantiles: tuple[float, float] | None = None,
) -> tuple[pd.DataFrame, pd.Series, dict[str, float]]:
    """Build (X, y, audit). ``clip_quantiles`` drops rows whose price falls
    outside ``[q_low, q_high]`` after dropping non-positive prices. ``audit``
    captures row counts at each step for MLflow."""
    work = df[FEATURE_COLUMNS + [TARGET_COLUMN]].copy()
    work[TARGET_COLUMN] = pd.to_numeric(work[TARGET_COLUMN], errors="coerce")
    work = work.dropna(subset=[TARGET_COLUMN])
    work = work[work[TARGET_COLUMN] > 0]
    rows_after_positive_price = len(work)

    price_floor = float(work[TARGET_COLUMN].min()) if rows_after_positive_price else 0.0
    price_ceiling = float(work[TARGET_COLUMN].max()) if rows_after_positive_price else 0.0
    if clip_quantiles is not None and rows_after_positive_price > 0:
        q_low, q_high = clip_quantiles
        if not (0.0 <= q_low < q_high <= 1.0):
            raise ValueError(
                f"clip_quantiles must satisfy 0 <= q_low < q_high <= 1, got {clip_quantiles}"
            )
        if q_low > 0 or q_high < 1:
            price_floor = float(work[TARGET_COLUMN].quantile(q_low))
            price_ceiling = float(work[TARGET_COLUMN].quantile(q_high))
            work = work[
                (work[TARGET_COLUMN] >= price_floor)
                & (work[TARGET_COLUMN] <= price_ceiling)
            ]

    for col in CAT_FEATURES:
        work[col] = work[col].fillna("unknown").astype(str).str.strip()
        work.loc[work[col] == "", col] = "unknown"
        work.loc[work[col].str.lower() == "nan", col] = "unknown"
    for col in NUM_FEATURES:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    X = work[FEATURE_COLUMNS]
    y = work[TARGET_COLUMN]
    audit = {
        "rows_after_positive_price": float(rows_after_positive_price),
        "rows_after_clip": float(len(work)),
        "price_floor": price_floor,
        "price_ceiling": price_ceiling,
    }
    return X, y, audit


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train XGBoost price model on cleaned eBay data.")
    p.add_argument(
        "--data",
        type=Path,
        default=DEFAULT_DATA_CSV,
        help="Path to cleaned CSV or Parquet (ignored when --from-mongo is set).",
    )
    p.add_argument(
        "--model-out",
        type=Path,
        default=DEFAULT_MODEL_OUT,
        help="Where to save the fitted sklearn Pipeline (joblib).",
    )
    p.add_argument("--test-size", type=float, default=0.2)
    p.add_argument("--random-state", type=int, default=42)
    p.add_argument("--n-estimators", type=int, default=2000)
    p.add_argument("--max-depth", type=int, default=8, help="Tree max_depth (XGBoost).")
    p.add_argument("--learning-rate", type=float, default=0.04)
    p.add_argument("--subsample", type=float, default=0.9)
    p.add_argument("--colsample-bytree", type=float, default=0.9)
    p.add_argument("--min-child-weight", type=float, default=4.0)
    p.add_argument("--reg-alpha", type=float, default=0.0)
    p.add_argument("--reg-lambda", type=float, default=1.0)
    p.add_argument(
        "--objective",
        default="reg:absoluteerror",
        choices=["reg:absoluteerror", "reg:squarederror", "reg:pseudohubererror"],
        help="XGBoost loss. reg:absoluteerror directly optimizes MAE.",
    )
    p.add_argument(
        "--clip-low",
        type=float,
        default=0.005,
        help="Lower quantile for price outlier clipping (0 disables).",
    )
    p.add_argument(
        "--clip-high",
        type=float,
        default=0.995,
        help="Upper quantile for price outlier clipping (1 disables).",
    )
    p.add_argument(
        "--early-stopping-rounds",
        type=int,
        default=50,
        help="Stop boosting if val MAE doesn't improve for N rounds (0 disables).",
    )
    p.add_argument(
        "--val-fraction",
        type=float,
        default=0.1,
        help="Fraction of the train split held out for early-stopping eval.",
    )
    p.add_argument("--no-mlflow", action="store_true", help="Skip MLflow logging.")

    mongo = p.add_argument_group(
        "MongoDB data source",
        "Use --from-mongo to pull training rows directly from the MongoDB "
        "'parsed' collection populated by extract_features_from_mongo.py. "
        "Requires MONGODB_URI in the environment or via --mongo-uri.",
    )
    mongo.add_argument(
        "--from-mongo",
        action="store_true",
        help="Load training data from MongoDB instead of --data.",
    )
    mongo.add_argument(
        "--mongo-uri",
        default=None,
        help="MongoDB connection string. Defaults to MONGODB_URI env var.",
    )
    mongo.add_argument(
        "--mongo-database",
        default=None,
        help="MongoDB database name. Defaults to MONGODB_DATABASE env var or 'historical'.",
    )
    mongo.add_argument(
        "--mongo-collection",
        default=None,
        help=(
            "MongoDB collection name. Defaults to MONGODB_PARSED_COLL or "
            "MONGODB_TARGET_COLL env var, or 'parsed'."
        ),
    )
    mongo.add_argument(
        "--mongo-limit",
        type=int,
        default=0,
        help="Max documents to pull from MongoDB (0 = no limit).",
    )
    mongo.add_argument(
        "--mongo-relaxed-filter",
        action="store_true",
        help=(
            "Do not require parse_status='ok'; accept any non-error doc with "
            "a numeric parsed_price."
        ),
    )
    mongo.add_argument(
        "--clothing-csv",
        type=Path,
        default=None,
        help="Retail catalog CSV used to attach initial_price (same as clean_ebay_exports).",
    )
    mongo.add_argument(
        "--mongo-cache-csv",
        type=Path,
        default=None,
        help=(
            "Optional path to write a CSV snapshot of the assembled MongoDB "
            "training frame (the exact rows handed to the trainer)."
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    data_source_label: str
    dataset_path_str: str
    mongo_filter_mode: str | None = None
    mongo_settings: tuple[str, str, str] | None = None

    if args.from_mongo:
        try:
            from dotenv import load_dotenv
        except ImportError:
            load_dotenv = None
        if load_dotenv is not None:
            load_dotenv()

        export_mod = _load_mongo_export_module()
        try:
            uri, database, collection = export_mod.resolve_mongo_settings(
                uri=args.mongo_uri,
                database=args.mongo_database,
                collection=args.mongo_collection,
            )
        except ValueError as exc:
            raise SystemExit(f"ERROR: {exc}")
        mongo_settings = (uri, database, collection)
        print(
            f"Loading training data from MongoDB: {database}.{collection} "
            f"(limit={args.mongo_limit or 'none'}, "
            f"relaxed_filter={args.mongo_relaxed_filter})"
        )
        df, mongo_filter_mode = export_mod.load_training_frame_from_mongo(
            uri=uri,
            database=database,
            collection=collection,
            limit=args.mongo_limit,
            clothing_csv=args.clothing_csv,
            relaxed_parse_filter=args.mongo_relaxed_filter,
        )
        if df.empty:
            raise SystemExit(
                f"ERROR: No usable rows found in {database}.{collection}. "
                "Run extract_features_from_mongo.py first, or pass "
                "--mongo-relaxed-filter to widen the query."
            )
        if args.mongo_cache_csv is not None:
            cache_path = args.mongo_cache_csv
            if not cache_path.is_absolute():
                cache_path = ROOT / cache_path
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(cache_path, index=False)
            print(f"Cached MongoDB training frame -> {cache_path}")
            dataset_path_str = str(cache_path)
        else:
            dataset_path_str = f"mongodb://{database}.{collection}"
        data_source_label = f"mongo:{database}.{collection}"
    else:
        data_path = args.data
        if not data_path.is_absolute():
            data_path = ROOT / data_path
        if not data_path.exists():
            raise FileNotFoundError(
                f"Cleaned dataset not found: {data_path}. Run: python scripts/clean_ebay_exports.py"
            )
        df = load_dataset(data_path)
        data_source_label = "ebay_historical_cleaned"
        dataset_path_str = str(data_path)

    rows_loaded = len(df)
    clip_quantiles: tuple[float, float] | None = None
    if not (args.clip_low <= 0.0 and args.clip_high >= 1.0):
        clip_quantiles = (float(args.clip_low), float(args.clip_high))
    X, y, audit = prepare_xy(df, clip_quantiles=clip_quantiles)
    if len(X) < 50:
        raise ValueError(f"Too few rows with valid price after cleaning: {len(X)}")
    print(
        f"Outlier clip @ ({args.clip_low}, {args.clip_high}): "
        f"price floor={audit['price_floor']:.2f}, ceiling={audit['price_ceiling']:.2f}; "
        f"{int(audit['rows_after_positive_price'])} -> {int(audit['rows_after_clip'])} rows"
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=args.random_state,
    )

    early_stopping_rounds = args.early_stopping_rounds if args.early_stopping_rounds > 0 else None
    pipeline = build_pipeline(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        random_state=args.random_state,
        objective=args.objective,
        min_child_weight=args.min_child_weight,
        reg_alpha=args.reg_alpha,
        reg_lambda=args.reg_lambda,
        early_stopping_rounds=early_stopping_rounds,
    )

    used_early_stopping = False
    best_iteration: int | None = None
    if early_stopping_rounds is not None and args.val_fraction > 0:
        X_inner_train, X_val, y_inner_train, y_val = train_test_split(
            X_train,
            y_train,
            test_size=args.val_fraction,
            random_state=args.random_state,
        )
        preprocessor = pipeline.named_steps["preprocessor"]
        model = pipeline.named_steps["model"]
        X_inner_train_t = preprocessor.fit_transform(X_inner_train, y_inner_train)
        X_val_t = preprocessor.transform(X_val)
        model.fit(
            X_inner_train_t,
            y_inner_train,
            eval_set=[(X_val_t, y_val)],
            verbose=False,
        )
        used_early_stopping = True
        best_iteration = getattr(model, "best_iteration", None)
        if best_iteration is not None:
            print(
                f"Early stopping: best_iteration={best_iteration} "
                f"(of n_estimators={args.n_estimators})"
            )
    else:
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
        run_name = "ebay-price-xgboost-mongo" if args.from_mongo else "ebay-price-xgboost"
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.set_tags(
                {
                    "task": "sold_price_regression",
                    "data_source": data_source_label,
                    "model_family": "xgboost",
                }
            )
            params: dict[str, object] = {
                "dataset_path": dataset_path_str,
                "n_rows_loaded": rows_loaded,
                "n_rows_used": len(X),
                "n_rows_train": len(X_train),
                "n_rows_test": len(X_test),
                "n_estimators": args.n_estimators,
                "max_depth": args.max_depth,
                "learning_rate": args.learning_rate,
                "subsample": args.subsample,
                "colsample_bytree": args.colsample_bytree,
                "min_child_weight": args.min_child_weight,
                "reg_alpha": args.reg_alpha,
                "reg_lambda": args.reg_lambda,
                "objective": args.objective,
                "clip_low": args.clip_low,
                "clip_high": args.clip_high,
                "price_floor": audit["price_floor"],
                "price_ceiling": audit["price_ceiling"],
                "early_stopping_rounds": args.early_stopping_rounds,
                "val_fraction": args.val_fraction,
                "early_stopping_used": str(used_early_stopping),
                "best_iteration": "" if best_iteration is None else int(best_iteration),
                "n_cat_low_card": len(CAT_LOW_CARD_FEATURES),
                "n_cat_high_card": len(CAT_HIGH_CARD_FEATURES),
                "n_numeric": len(NUM_FEATURES),
                "features": ",".join(FEATURE_COLUMNS),
                "target": TARGET_COLUMN,
            }
            if args.from_mongo and mongo_settings is not None:
                _uri, mongo_db, mongo_coll = mongo_settings
                params.update(
                    {
                        "mongo_database": mongo_db,
                        "mongo_collection": mongo_coll,
                        "mongo_limit": args.mongo_limit,
                        "mongo_filter_mode": mongo_filter_mode or "unknown",
                        "mongo_relaxed_filter": str(bool(args.mongo_relaxed_filter)),
                    }
                )
            mlflow.log_params(params)
            mlflow.log_metrics(
                {"mae": float(mae), "rmse": float(rmse), "r2_holdout": float(pipeline.score(X_test, y_test))}
            )
            mlflow.log_artifact(str(model_out), artifact_path="local_model")
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                input_example=X.head(3),
            )

    print(f"Rows loaded: {rows_loaded}")
    print(f"Rows used:   {len(X)} (train {len(X_train)}, test {len(X_test)})")
    print(f"MAE:  {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"Saved pipeline: {model_out}")


if __name__ == "__main__":
    main()
