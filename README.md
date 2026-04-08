# Clothing value prediction (MLOps)

Predict clothing resale value using sold-listing data. The repo now includes:

- a scraper-aligned raw/bronze/silver data pipeline
- MLflow-backed training and model registration
- a FastAPI prediction service for the deployment milestone
- a one-page `Spiffy` web UI served from the same app for Cloud Run deployment

## Quick start

```bash
cd mlops
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -U pip
pip install -e .
```

Build the sample sold-listings dataset:

```bash
python scripts/build_sample_dataset.py
```

Train and log a baseline model to MLflow:

```bash
python scripts/train_model.py
```

Browse experiments in the UI:

```bash
mlflow ui --backend-store-uri file:./mlruns
```

## Run the App Locally

The service loads the model from the local MLflow Model Registry in `./mlruns` by default. If
you have already trained the baseline model in this repo, the latest registered version is loaded
from `models:/clothing-value-model/latest`.

Start the web app and API:

```bash
export MLFLOW_SERVING_MODEL_URI="models:/clothing-value-model/latest"
uvicorn clothing_mlops.service:app --reload
```

If you are working only with a local run artifact before registry setup, you can override the URI
with a direct run-based model URI.

Verify the required milestone endpoints in another terminal:

```bash
open http://127.0.0.1:8000/
curl http://127.0.0.1:8000/api
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "Patagonia",
    "category": "Jacket",
    "size": "M",
    "condition": "used_very_good",
    "color": "Blue",
    "material": "Polyester",
    "listing_price": 129.0,
    "shipping_price": 12.5
  }'
```

Generate the placeholder lifetime value curve used by the UI:

```bash
curl -X POST http://127.0.0.1:8000/api/lifetime-curve \
  -H "Content-Type: application/json" \
  -d '{
    "item_name": "fear of god essentials hoodie",
    "purchase_price": 140
  }'
```

## Configuration

| Env var | Purpose |
|--------|---------|
| `MLFLOW_TRACKING_URI` | Defaults to `file:<repo>/mlruns`. Set to a remote server when you have one. |
| `MLFLOW_EXPERIMENT_NAME` | Defaults to `item-value-prediction`. |
| `MLFLOW_MODEL_NAME` | Model Registry name used by the service. Defaults to `clothing-value-model`. |
| `MLFLOW_SERVING_MODEL_URI` | Explicit model URI for the service, such as `models:/clothing-value-model/latest`. |

## Data pipeline

The ingestion side of the project is organized into three layers:

- `data/raw/`: one HTML snapshot per listing plus a manifest
- `data/bronze/`: parsed sold-listing records
- `data/silver/`: normalized training dataset and summary metadata

Each record includes traceability fields such as `listing_id`, `source_url`, `scrape_timestamp`, `parser_version`, and `sold_date`.

To test against a manually saved eBay item page without hitting the live site:

```bash
python scripts/parse_saved_ebay_html.py "/absolute/path/to/item.html"
```

This emits one normalized JSON record extracted from the saved HTML, using page metadata, structured product schema, and visible item details.

To download a batch of generic HTML pages from a URL list into a local folder:

```bash
python scripts/download_html_pages.py urls.txt --output-dir data/raw/downloads
```

Where `urls.txt` contains one URL per line. The script saves each HTML file plus `download_manifest.csv` with status and output path.

## App Structure For Cloud Run

The current structure is intentionally a single service:

- `GET /` serves the `Spiffy` web app
- `GET /api` returns API metadata and an example prediction payload
- `GET /health` returns service readiness
- `POST /predict` keeps the model-backed prediction surface
- `POST /api/lifetime-curve` powers the placeholder depreciation chart for the UI

This is the simplest Cloud Run deployment shape right now because one container can serve both the
frontend and backend. If the UI later grows into a larger React/Vite app, it can still be split
out behind a load balancer or served as a static build artifact, but that complexity is not needed
yet.

## API endpoints

The FastAPI service exposes the milestone endpoints:

- `GET /api` returns a welcome payload and example request body
- `GET /health` confirms whether the MLflow model loaded successfully
- `POST /predict` validates input with Pydantic and returns a predicted sale price
- `POST /api/lifetime-curve` returns a generic medium-depreciation value curve for the UI

Example request:

```json
{
  "brand": "Patagonia",
  "category": "Jacket",
  "size": "M",
  "condition": "used_very_good",
  "color": "Blue",
  "material": "Polyester",
  "listing_price": 129.0,
  "shipping_price": 12.5
}
```

Example response:

```json
{
  "predicted_sale_price": 126.13,
  "model_status": "ok"
}
```

## Docker

Build and run the app container locally:

```bash
docker build -t clothing-value-api .
docker run -p 8000:8000 \
  -v "$(pwd)/mlruns:/app/mlruns" \
  -e MLFLOW_TRACKING_URI="file:/app/mlruns" \
  -e MLFLOW_SERVING_MODEL_URI="models:/clothing-value-model/latest" \
  clothing-value-api
```

Published Docker Hub image:

```bash
aidanpercy/clothing-value-api:latest
```

To publish the image to Docker Hub, tag and push the local image using the same published image
name:

```bash
docker login
docker tag clothing-value-api aidanpercy/clothing-value-api:latest
docker push aidanpercy/clothing-value-api:latest
```

To run the already-published image from Docker Hub:

```bash
docker run -p 8000:8000 \
  -v "$(pwd)/mlruns:/app/mlruns" \
  -e MLFLOW_TRACKING_URI="file:/app/mlruns" \
  -e MLFLOW_SERVING_MODEL_URI="models:/clothing-value-model/latest" \
  aidanpercy/clothing-value-api:latest
```

Use the same published image name in your report, screenshot, and submission PDF.

## Project flow

1. Build raw/bronze/silver sold-listings artifacts with `python scripts/build_sample_dataset.py`
2. Train a baseline model and log it to MLflow with `python scripts/train_model.py`
3. Register or reference the trained model in MLflow
4. Serve predictions through FastAPI and package the service with Docker

`mlruns/` is gitignored; commit code and configs, not local run stores (or export to a shared tracking server for the team).
