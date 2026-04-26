# Deployment Milestone Submission Template

Fill in the placeholders below, export to PDF, and submit that PDF for the milestone.

## 1. GitHub Repository

- Repository URL: `PASTE_GITHUB_REPO_URL_HERE`
- Endpoint file: `PASTE_DIRECT_LINK_TO_src/clothing_mlops/service.py_HERE`

## 2. Docker Image Proof

- Published image name: `YOUR_DOCKERHUB_USERNAME/clothing-value-api:latest`
- Registry URL: `PASTE_DOCKER_IMAGE_URL_HERE`
- Screenshot: insert a screenshot of the published image page showing the exact image name above

## 3. Run Instructions Summary

Local API:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export GOOGLE_CLOUD_LOCATION="global"
export GOOGLE_GENAI_USE_VERTEXAI=True
export VERTEX_AI_MODEL="gemini-2.5-flash"
uvicorn clothing_mlops.service:app --reload
```

Docker:

```bash
docker build -t clothing-value-api .
docker run -p 8000:8000 \
  -e GOOGLE_CLOUD_PROJECT="your-gcp-project-id" \
  -e GOOGLE_CLOUD_LOCATION="global" \
  -e GOOGLE_GENAI_USE_VERTEXAI="True" \
  -e VERTEX_AI_MODEL="gemini-2.5-flash" \
  clothing-value-api
```

## 4. Example Predict Request / Response

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
