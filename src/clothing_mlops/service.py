"""FastAPI application for serving the trained clothing value model."""

from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from clothing_mlops.data_pipeline import prediction_example
from clothing_mlops.modeling import load_serving_model

app = FastAPI(title="Clothing Value Prediction API", version="0.1.0")

_model = None
_model_status = "not_loaded"
_model_error: str | None = None


class PredictRequest(BaseModel):
    brand: str = Field(..., examples=["Patagonia"])
    category: str = Field(..., examples=["Jacket"])
    size: str = Field(..., examples=["M"])
    condition: str = Field(..., examples=["used_very_good"])
    color: str = Field(..., examples=["Blue"])
    material: str = Field(..., examples=["Polyester"])
    listing_price: float = Field(..., gt=0)
    shipping_price: float = Field(..., ge=0)


class PredictResponse(BaseModel):
    predicted_sale_price: float
    model_status: str


def refresh_model() -> None:
    global _model, _model_status, _model_error
    _model, _model_status, _model_error = load_serving_model()


@app.on_event("startup")
def startup_event() -> None:
    refresh_model()


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "message": "Clothing value prediction service",
        "example_request": prediction_example(),
    }


@app.get("/health")
def health(response: Response) -> dict[str, Any]:
    if _model_status != "ok":
        response.status_code = 503
        return {
            "status": "unhealthy",
            "model_loaded": False,
            "error": _model_error,
        }
    return {"status": "ok", "model_loaded": True}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    if _model is None:
        raise HTTPException(status_code=503, detail=f"Model not loaded: {_model_error}")
    features = pd.DataFrame([request.model_dump()])
    prediction = float(_model.predict(features)[0])
    return PredictResponse(
        predicted_sale_price=round(prediction, 2),
        model_status=_model_status,
    )
