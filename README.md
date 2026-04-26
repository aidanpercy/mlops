# Clothing Value Prediction (MLOps)

This repo now serves a Vertex AI-ready pricing website for used clothing. The current web app,
`Spiffy`, accepts a free-text clothing description plus a retail price and returns three resale
price estimates:

- `like_new`
- `good`
- `used`

The app is designed to use a Vertex AI LLM now and to make a later swap to a custom Vertex AI
model straightforward. The pricing integration is isolated in
[`src/clothing_mlops/vertex_pricing.py`](/Users/aidanpercy/Desktop/603/mlops/src/clothing_mlops/vertex_pricing.py:1).

## What changed

- The old depreciation-curve UI was replaced with a text-plus-retail pricing workflow.
- `POST /api/condition-prices` is the new UI endpoint.
- `POST /predict` remains available as an alias for the same pricing request.
- Vertex AI is the primary inference path.
- A deterministic local heuristic fallback keeps the app usable when Vertex AI is not configured.
- The UI visualizes the result as a chart from retail to the three condition prices.

## Quick start

```bash
cd /Users/aidanpercy/Desktop/603/mlops
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .
```

Run the app:

```bash
uvicorn clothing_mlops.service:app --reload
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Vertex AI setup

Install the Google Cloud CLI and authenticate with Application Default Credentials:

```bash
gcloud init
gcloud auth application-default login
```

Set environment variables for Vertex AI:

```bash
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export GOOGLE_CLOUD_LOCATION="global"
export VERTEX_AI_MODEL="gemini-2.5-flash"
export GOOGLE_GENAI_USE_VERTEXAI=True
```

Optional tuning knobs:

```bash
export VERTEX_AI_LOCATION="global"
export VERTEX_AI_TEMPERATURE="0.2"
```

If these variables are missing, the service falls back to a local rule-based estimator and returns
that status in the API response.

## API

### `GET /`

Serves the `Spiffy` web app.

### `GET /api`

Returns API metadata and an example request body.

### `GET /health`

Returns service status plus whether Vertex AI is configured.

### `POST /api/condition-prices`

Primary pricing endpoint for the website.

Request:

```json
{
  "description": "Patagonia Synchilla fleece pullover in navy, men's medium, lightly worn with no stains or holes.",
  "retail_price": 139.0
}
```

Response:

```json
{
  "description": "Patagonia Synchilla fleece pullover in navy, men's medium, lightly worn with no stains or holes.",
  "retail_price": 139.0,
  "item_summary": "Patagonia Synchilla fleece pullover in navy, men's medium, lightly worn with no stains or holes.",
  "prices": {
    "like_new": 110.0,
    "good": 93.0,
    "used": 69.0
  },
  "provider": "vertex_ai",
  "model": "gemini-2.5-flash",
  "confidence_notes": "Pricing reflects inferred resale positioning for this category and brand."
}
```

### `POST /predict`

Alias for `POST /api/condition-prices`.

## Local verification

```bash
curl http://127.0.0.1:8000/api
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/api/condition-prices \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Fear of God Essentials hoodie in oatmeal, mens medium, minimal wear.",
    "retail_price": 120
  }'
```

## Future custom model path

When you move from Gemini to a custom Vertex AI model, keep the FastAPI contract stable and swap the
implementation behind the pricing adapter. The cleanest next step is to add a second backend in
`vertex_pricing.py` that targets your deployed Vertex endpoint while keeping the same output shape.

## Notes

- `google-genai` is included as the SDK dependency for the current Vertex AI LLM path.
- The rest of the repo still contains the original ML/data pipeline code for training and MLOps
  coursework, but the web UI is now centered on the Vertex AI pricing flow.
