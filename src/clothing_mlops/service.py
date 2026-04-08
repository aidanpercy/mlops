"""FastAPI service for the Spiffy web UI and model-backed API routes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from clothing_mlops.data_pipeline import prediction_example
from clothing_mlops.mlflow_setup import set_experiment
from clothing_mlops.modeling import load_serving_model


ITEM_CATALOG = [
    {
        "name": "Supreme 20th Anniversary Box Logo Tee",
        "tag": "Archive tee",
        "description": "A landmark streetwear graphic tee with strong collector appeal and simple everyday wearability.",
        "image": "/static/images/supreme.webp",
    },
    {
        "name": "Levi's Women’s 501",
        "tag": "Denim staple",
        "description": "Classic straight-leg denim with broad demand, durable fabric, and a long resale shelf life.",
        "image": "/static/images/levis.avif",
    },
    {
        "name": "Balenciaga City Bag",
        "tag": "Designer bag",
        "description": "A soft leather icon from the early luxury handbag wave, driven by brand recognition and condition.",
        "image": "/static/images/balenciaga.jpg",
    },
    {
        "name": "Air Jordan 4 “Military Black” (2022)",
        "tag": "Sneaker release",
        "description": "A widely recognized retro sneaker with steady demand from both collectors and casual buyers.",
        "image": "/static/images/jordans.avif",
    },
    {
        "name": "Fear of God Essentials Hoodie",
        "tag": "Premium basics",
        "description": "A heavyweight basics piece with recognizable branding and a broad contemporary buyer base.",
        "image": "/static/images/foggg.jpeg",
    },
]

ITEM_OPTIONS = [item["name"] for item in ITEM_CATALOG]


class PredictionRequest(BaseModel):
    brand: str
    category: str
    size: str
    condition: str
    color: str
    material: str
    listing_price: float = Field(gt=0)
    shipping_price: float = Field(ge=0)


class LifetimeCurveRequest(BaseModel):
    item_name: str
    purchase_price: float = Field(gt=0)


@dataclass
class PlaceholderModel:
    """Fallback predictor used when the MLflow model is unavailable."""

    def predict(self, frame: pd.DataFrame) -> list[float]:
        totals = frame["listing_price"] + frame["shipping_price"]
        return [round(float(value) * 0.87, 2) for value in totals]


app = FastAPI(title="Spiffy", version="0.1.0")
_serving_model: Any | None = None
_model_loaded = False

STATIC_DIR = Path(__file__).resolve().parents[2] / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def refresh_model() -> None:
    global _serving_model, _model_loaded

    set_experiment()
    model, status, _ = load_serving_model()
    if status == "ok" and model is not None:
        _serving_model = model
        _model_loaded = True
        return

    _serving_model = PlaceholderModel()
    _model_loaded = True


@app.on_event("startup")
def _startup() -> None:
    refresh_model()


def _curve_points(purchase_price: float) -> list[dict[str, float | int | str]]:
    depreciation_profile = [1.0, 0.88, 0.79, 0.71, 0.64, 0.58, 0.53]
    return [
        {
            "month": index * 12,
            "label": f"Year {index}",
            "value": round(purchase_price * factor, 2),
        }
        for index, factor in enumerate(depreciation_profile)
    ]


@app.get("/", response_class=HTMLResponse)
def spiffy_home() -> str:
    items_json = json.dumps(ITEM_CATALOG)
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Spiffy</title>
    <style>
      :root {{
        --page: #e7e2da;
        --surface: #f4efe8;
        --panel: #faf7f2;
        --panel-alt: #eef6fb;
        --card: #ffffff;
        --ink: #15202b;
        --muted: #5f6d78;
        --line: rgba(21, 32, 43, 0.12);
        --blue: #8ec5e6;
        --blue-deep: #4f8fb7;
        --blue-soft: #dff0fb;
        --white: #ffffff;
      }}

      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        color: var(--ink);
        font-family: "Avenir Next", "Helvetica Neue", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(255, 255, 255, 0.45), transparent 30%),
          linear-gradient(180deg, #ece7e0 0%, var(--page) 100%);
      }}

      h1, h2, h3, p {{
        margin: 0;
      }}

      .page {{
        max-width: 1380px;
        margin: 0 auto;
        padding: 22px;
      }}

      .stack {{
        display: grid;
        gap: 14px;
      }}

      .band {{
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 26px;
        padding: 16px 22px;
        box-shadow: 0 8px 24px rgba(21, 32, 43, 0.04);
      }}

      .topbar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
      }}

      .brand {{
        display: inline-flex;
        align-items: center;
        gap: 12px;
        font-size: 1.7rem;
        font-weight: 800;
        letter-spacing: -0.05em;
      }}

      .brand-mark {{
        width: 40px;
        height: 40px;
        border-radius: 14px;
        background: linear-gradient(145deg, var(--blue) 0%, #b9def3 100%);
        position: relative;
        overflow: hidden;
      }}

      .brand-mark::before {{
        content: "";
        position: absolute;
        inset: 9px;
        border-radius: 999px;
        background: rgba(255,255,255,0.88);
      }}

      .brand-note {{
        color: var(--muted);
        font-size: 0.95rem;
        white-space: nowrap;
      }}

      .headline {{
        background: var(--panel-alt);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 18px;
      }}

      .headline-copy {{
        display: grid;
        gap: 10px;
      }}

      .eyebrow {{
        width: fit-content;
        padding: 7px 12px;
        border-radius: 999px;
        background: var(--blue-soft);
        color: var(--blue-deep);
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}

      .headline h1 {{
        font-size: clamp(2rem, 4vw, 3.2rem);
        line-height: 0.96;
        letter-spacing: -0.06em;
      }}

      .headline p {{
        color: var(--muted);
        font-size: 1rem;
        line-height: 1.45;
      }}

      .summary-chip {{
        min-width: 180px;
        padding: 16px 18px;
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 22px;
      }}

      .summary-chip span {{
        display: block;
      }}

      .summary-chip .label {{
        color: var(--muted);
        font-size: 0.78rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
      }}

      .summary-chip .value {{
        margin-top: 8px;
        font-size: 1.7rem;
        font-weight: 800;
        letter-spacing: -0.05em;
      }}

      .section-title {{
        font-size: 1.5rem;
        letter-spacing: -0.05em;
      }}

      .item-grid {{
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 14px;
        margin-top: 16px;
      }}

      .item-card {{
        appearance: none;
        width: 100%;
        text-align: left;
        padding: 12px;
        border-radius: 22px;
        border: 1px solid var(--line);
        background: var(--card);
        cursor: pointer;
        transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
        display: grid;
        gap: 12px;
        align-content: start;
      }}

      .item-card:hover,
      .item-card:focus-visible {{
        transform: translateY(-2px);
        border-color: rgba(79, 143, 183, 0.5);
        box-shadow: 0 14px 28px rgba(79, 143, 183, 0.12);
        outline: none;
      }}

      .item-card.active {{
        background: var(--blue-soft);
        border-color: var(--blue-deep);
        box-shadow: 0 14px 28px rgba(79, 143, 183, 0.16);
      }}

      .item-image {{
        width: 100%;
        aspect-ratio: 1 / 1;
        object-fit: contain;
        border-radius: 18px;
        border: 1px solid rgba(21, 32, 43, 0.08);
        background: #edf4f8;
        padding: 10px;
      }}

      .item-meta {{
        display: grid;
        gap: 8px;
      }}

      .item-name {{
        font-size: 1rem;
        line-height: 1.05;
        font-weight: 800;
        letter-spacing: -0.04em;
      }}

      .item-description {{
        color: var(--muted);
        font-size: 0.88rem;
        line-height: 1.45;
      }}

      .price-row {{
        display: flex;
        align-items: center;
        gap: 16px;
        flex-wrap: wrap;
      }}

      .price-row h3 {{
        font-size: 1.45rem;
        letter-spacing: -0.05em;
      }}

      .price-input {{
        display: inline-flex;
        align-items: center;
        gap: 10px;
        min-width: 220px;
        padding: 12px 16px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: var(--card);
      }}

      .price-input span {{
        font-size: 1.15rem;
        font-weight: 800;
      }}

      .price-input input {{
        width: 100%;
        border: 0;
        outline: none;
        background: transparent;
        color: var(--ink);
        font-size: 1.08rem;
        font-weight: 700;
        font-family: inherit;
      }}

      .price-note {{
        color: var(--muted);
        font-size: 0.94rem;
      }}

      .chart-head {{
        display: flex;
        justify-content: space-between;
        align-items: start;
        gap: 16px;
        margin-bottom: 16px;
      }}

      .chart-head h2 {{
        font-size: 1.8rem;
        line-height: 0.98;
        letter-spacing: -0.06em;
      }}

      .chart-head p {{
        margin-top: 8px;
        color: var(--muted);
        line-height: 1.45;
      }}

      .chart-box {{
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 18px;
      }}

      .chart-summary {{
        min-width: 170px;
        padding: 16px 18px;
        background: var(--panel-alt);
        border-radius: 22px;
        border: 1px solid rgba(79, 143, 183, 0.18);
      }}

      .chart-summary .label {{
        color: var(--muted);
        font-size: 0.76rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
      }}

      .chart-summary .value {{
        display: block;
        margin-top: 8px;
        font-size: 1.9rem;
        font-weight: 800;
        letter-spacing: -0.05em;
      }}

      .chart-svg {{
        display: block;
        width: 100%;
        height: auto;
      }}

      .empty {{
        border-radius: 20px;
        padding: 22px;
        background: linear-gradient(180deg, rgba(142, 197, 230, 0.14), rgba(255,255,255,0.7));
        color: var(--muted);
        line-height: 1.5;
      }}

      @media (max-width: 1180px) {{
        .item-grid {{
          grid-template-columns: repeat(3, minmax(0, 1fr));
        }}
      }}

      @media (max-width: 820px) {{
        .headline,
        .chart-head {{
          flex-direction: column;
        }}

        .item-grid {{
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }}
      }}

      @media (max-width: 560px) {{
        .page {{
          padding: 14px;
        }}

        .topbar {{
          flex-direction: column;
          align-items: start;
        }}

        .item-grid {{
          grid-template-columns: 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    <main class="page">
      <div class="stack">
        <section class="band topbar">
          <div class="brand">
            <span class="brand-mark" aria-hidden="true"></span>
            <span>Spiffy</span>
          </div>
        </section>

        <section class="band headline">
          <div class="headline-copy">
            <h1>See what your purchase will be worth.</h1>
            <p>
              Pick an item, enter the purchase price, and Spiffy generates a placeholder
              medium-depreciation value curve. The current curve is generic across all items.
            </p>
          </div>
        </section>

        <section class="band">
          <h2 class="section-title">What are you thinking of purchasing?</h2>
          <div class="item-grid" id="item-grid"></div>
        </section>

        <section class="band">
          <div class="price-row">
            <h3>How much is it?</h3>
            <label class="price-input" for="purchase-price">
              <span>$</span>
              <input id="purchase-price" type="number" min="0" step="0.01" placeholder="Enter purchase price" />
            </label>
          </div>
        </section>

        <section class="band">
          <div class="chart-head">
            <div>
              <h2>Estimated lifetime value</h2>
              <p id="selected-item">No item selected yet.</p>
            </div>
            <div class="chart-summary">
              <span class="label">Selected price</span>
              <strong class="value" id="selected-price">$0</strong>
            </div>
          </div>
          <div class="chart-box">
            <div class="empty" id="chart-empty">
              Choose an item and enter a valid price to generate the value curve. The chart spans the same content width as the rest of the layout, matching your sketch.
            </div>
            <svg class="chart-svg" id="chart-svg" viewBox="0 0 1100 360" role="img" aria-label="Estimated lifetime value curve" hidden></svg>
          </div>
        </section>
      </div>
    </main>

    <script>
      const ITEMS = {items_json};
      const itemGrid = document.getElementById("item-grid");
      const priceInput = document.getElementById("purchase-price");
      const selectedPrice = document.getElementById("selected-price");
      const selectedItem = document.getElementById("selected-item");
      const chartEmpty = document.getElementById("chart-empty");
      const chartSvg = document.getElementById("chart-svg");

      let activeItem = null;

      function money(value) {{
        return new Intl.NumberFormat("en-US", {{
          style: "currency",
          currency: "USD",
          maximumFractionDigits: 0
        }}).format(value);
      }}

      function buildCards() {{
        ITEMS.forEach((item) => {{
          const button = document.createElement("button");
          button.type = "button";
          button.className = "item-card";
          button.dataset.item = item.name;
          button.innerHTML = `
            <img class="item-image" src="${{item.image}}" alt="${{item.name}}" />
            <div class="item-meta">
              <span class="item-name">${{item.name}}</span>
              <span class="item-description">${{item.description}}</span>
            </div>
          `;
          button.addEventListener("click", () => {{
            activeItem = item;
            document.querySelectorAll(".item-card").forEach((card) => {{
              card.classList.toggle("active", card.dataset.item === item.name);
            }});
            maybeGenerateCurve();
          }});
          itemGrid.appendChild(button);
        }});
      }}

      function drawCurve(points) {{
        const width = 1100;
        const height = 360;
        const left = 68;
        const right = 28;
        const top = 24;
        const bottom = 48;
        const values = points.map((point) => point.value);
        const max = Math.max(...values);
        const min = Math.min(...values);
        const xStep = (width - left - right) / (points.length - 1);
        const yRange = Math.max(max - min, max * 0.28, 1);
        const toX = (index) => left + index * xStep;
        const toY = (value) => top + ((max - value) / yRange) * (height - top - bottom);

        const gridLines = Array.from({{ length: 4 }}, (_, index) => {{
          const y = top + index * ((height - top - bottom) / 3);
          return `<line x1="${{left}}" y1="${{y}}" x2="${{width - right}}" y2="${{y}}" stroke="rgba(21,32,43,0.08)" stroke-width="1" />`;
        }}).join("");

        const labels = points.map((point, index) => `
          <text x="${{toX(index)}}" y="${{height - 16}}" text-anchor="middle" font-size="12" fill="#5f6d78">${{point.label}}</text>
        `).join("");

        const area = [
          `${{left}},${{height - bottom}}`,
          ...points.map((point, index) => `${{toX(index)}},${{toY(point.value)}}`),
          `${{toX(points.length - 1)}},${{height - bottom}}`
        ].join(" ");

        const line = points
          .map((point, index) => `${{toX(index)}},${{toY(point.value)}}`)
          .join(" ");

        const dots = points.map((point, index) => `
          <circle cx="${{toX(index)}}" cy="${{toY(point.value)}}" r="6" fill="#4f8fb7" />
          <text x="${{toX(index)}}" y="${{toY(point.value) - 14}}" text-anchor="middle" font-size="12" font-weight="700" fill="#15202b">${{money(point.value)}}</text>
        `).join("");

        chartSvg.innerHTML = `
          <defs>
            <linearGradient id="curveFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="rgba(142,197,230,0.42)" />
              <stop offset="100%" stop-color="rgba(142,197,230,0.08)" />
            </linearGradient>
          </defs>
          ${{gridLines}}
          <polygon points="${{area}}" fill="url(#curveFill)" />
          <polyline points="${{line}}" fill="none" stroke="#4f8fb7" stroke-width="5" stroke-linecap="round" stroke-linejoin="round" />
          ${{dots}}
          ${{labels}}
        `;
      }}

      async function maybeGenerateCurve() {{
        const price = Number(priceInput.value);

        if (!activeItem || !Number.isFinite(price) || price <= 0) {{
          selectedPrice.textContent = "$0";
          selectedItem.textContent = activeItem
            ? `${{activeItem.name}} selected. Add a valid price to generate the curve.`
            : "No item selected yet.";
          chartSvg.hidden = true;
          chartEmpty.hidden = false;
          return;
        }}

        selectedPrice.textContent = money(price);
        selectedItem.textContent = `Showing the generic medium-depreciation curve for ${{activeItem.name}}.`;

        const response = await fetch("/api/lifetime-curve", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ item_name: activeItem.name, purchase_price: price }})
        }});

        if (!response.ok) {{
          chartSvg.hidden = true;
          chartEmpty.hidden = false;
          chartEmpty.textContent = "Unable to generate the curve right now.";
          return;
        }}

        const data = await response.json();
        drawCurve(data.points);
        chartEmpty.hidden = true;
        chartSvg.hidden = false;
      }}

      priceInput.addEventListener("input", maybeGenerateCurve);
      buildCards();
    </script>
  </body>
</html>"""


@app.get("/api")
def api_root() -> dict[str, Any]:
    return {
        "message": "Spiffy value prediction service",
        "example_request": prediction_example(),
        "curve_items": ITEM_OPTIONS,
    }


@app.get("/health")
def health() -> dict[str, bool | str]:
    return {"status": "ok", "model_loaded": _model_loaded}


@app.post("/predict")
def predict(payload: PredictionRequest) -> dict[str, float | str]:
    frame = pd.DataFrame([payload.model_dump()])
    assert _serving_model is not None
    prediction = float(_serving_model.predict(frame)[0])
    return {
        "predicted_sale_price": round(prediction, 2),
        "model_status": "ok",
    }


@app.post("/api/lifetime-curve")
def lifetime_curve(payload: LifetimeCurveRequest) -> dict[str, Any]:
    normalized_item = payload.item_name.strip().lower()
    if normalized_item not in ITEM_OPTIONS:
        return {
            "item_name": payload.item_name,
            "curve_type": "medium_depreciation",
            "points": _curve_points(payload.purchase_price),
            "warning": "Item not in preset list; generic curve returned.",
        }

    return {
        "item_name": payload.item_name,
        "curve_type": "medium_depreciation",
        "points": _curve_points(payload.purchase_price),
    }
