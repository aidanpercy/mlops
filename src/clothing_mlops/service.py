"""FastAPI service for the Spiffy UI and Vertex AI-backed pricing routes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from clothing_mlops.data_pipeline import pricing_request_example
from clothing_mlops.vertex_pricing import PricingResult, build_pricing_backend


load_dotenv()

ITEM_CATALOG = [
    {
        "name": "Supreme 20th Anniversary Box Logo Tee",
        "tag": "Archive tee",
        "description": "2000s-era Supreme box logo t-shirt, size large, bright red cotton, light wear with no cracking.",
        "image": "/static/images/supreme.webp",
    },
    {
        "name": "Levi's Women’s 501",
        "tag": "Denim staple",
        "description": "Levi's 501 jeans, women's 27, medium wash, broken-in denim, clean hems, gently worn.",
        "image": "/static/images/levis.avif",
    },
    {
        "name": "Balenciaga City Bag",
        "tag": "Designer bag",
        "description": "Balenciaga City bag in black leather with silver hardware, soft slouch, minor corner wear.",
        "image": "/static/images/balenciaga.jpg",
    },
    {
        "name": "Air Jordan 4 Military Black",
        "tag": "Sneaker release",
        "description": "Air Jordan 4 Military Black, size 10.5, 2022 release, worn a handful of times with clean uppers.",
        "image": "/static/images/jordans.avif",
    },
    {
        "name": "Fear of God Essentials Hoodie",
        "tag": "Premium basics",
        "description": "Fear of God Essentials hoodie in oatmeal, men's medium, heavyweight fleece, minimal wear.",
        "image": "/static/images/foggg.jpeg",
    },
]

ITEM_OPTIONS = [item["name"] for item in ITEM_CATALOG]


class PricingRequest(BaseModel):
    description: str = Field(min_length=8, max_length=1200)
    retail_price: float = Field(gt=0)


app = FastAPI(title="Spiffy", version="0.2.0")
_pricing_backend = build_pricing_backend()

STATIC_DIR = Path(__file__).resolve().parents[2] / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def refresh_backend() -> None:
    global _pricing_backend
    _pricing_backend = build_pricing_backend()


@app.on_event("startup")
def _startup() -> None:
    refresh_backend()


def _result_payload(description: str, retail_price: float, result: PricingResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "description": description,
        "retail_price": retail_price,
        "item_summary": result.item_summary,
        "prices": {
            "like_new": result.like_new,
            "good": result.good,
            "used": result.used,
        },
        "provider": result.provider,
        "model": result.model,
        "confidence_notes": result.confidence_notes,
    }
    if result.warning:
        payload["warning"] = result.warning
    return payload


@app.get("/", response_class=HTMLResponse)
def spiffy_home() -> str:
    samples_json = json.dumps(ITEM_CATALOG)
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
          radial-gradient(circle at top right, rgba(255, 255, 255, 0.5), transparent 24%),
          linear-gradient(180deg, #ece7e0 0%, var(--page) 100%);
      }}

      h1, h2, h3, p {{
        margin: 0;
      }}

      .page {{
        max-width: 1320px;
        margin: 0 auto;
        padding: 20px;
      }}

      .stack {{
        display: grid;
        gap: 16px;
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
        justify-content: space-between;
        align-items: center;
        gap: 12px;
      }}

      .brand {{
        display: inline-flex;
        align-items: center;
        gap: 12px;
        font-size: 1.8rem;
        font-weight: 800;
        letter-spacing: -0.06em;
      }}

      .brand-mark {{
        width: 42px;
        height: 42px;
        border-radius: 14px;
        background: linear-gradient(145deg, var(--blue) 0%, #b9def3 100%);
        position: relative;
      }}

      .brand-mark::before {{
        content: "";
        position: absolute;
        inset: 9px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.88);
      }}

      .topbar-note {{
        color: var(--muted);
        font-size: 0.95rem;
      }}

      .hero {{
        display: block;
        background: var(--panel-alt);
      }}

      .hero-copy {{
        display: grid;
        gap: 12px;
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

      .hero h1 {{
        font-size: clamp(2.1rem, 4.8vw, 4rem);
        line-height: 0.94;
        letter-spacing: -0.07em;
      }}

      .hero p {{
        color: var(--muted);
        line-height: 1.55;
        max-width: none;
        white-space: nowrap;
      }}

      .section-title {{
        font-size: 1.5rem;
        letter-spacing: -0.05em;
      }}

      .section-subtitle {{
        margin-top: 8px;
        color: var(--muted);
        line-height: 1.45;
      }}

      .sample-grid {{
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 14px;
        margin-top: 18px;
      }}

      .sample-card {{
        appearance: none;
        width: 100%;
        text-align: left;
        border: 1px solid var(--line);
        border-radius: 22px;
        background: var(--card);
        padding: 12px;
        cursor: pointer;
        display: grid;
        gap: 12px;
        transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
      }}

      .sample-card:hover,
      .sample-card:focus-visible {{
        transform: translateY(-2px);
        box-shadow: 0 14px 28px rgba(79, 143, 183, 0.12);
        border-color: rgba(79, 143, 183, 0.5);
        outline: none;
      }}

      .sample-image {{
        width: 100%;
        aspect-ratio: 1 / 1;
        object-fit: contain;
        border-radius: 18px;
        background: #f5efe7;
        border: 1px solid rgba(24, 33, 41, 0.08);
        padding: 12px;
      }}

      .sample-tag {{
        display: inline-flex;
        width: fit-content;
        padding: 6px 10px;
        border-radius: 999px;
        background: var(--blue-soft);
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}

      .sample-name {{
        font-size: 1rem;
        font-weight: 800;
        letter-spacing: -0.04em;
      }}

      .sample-description {{
        color: var(--muted);
        font-size: 0.9rem;
        line-height: 1.45;
      }}

      .composer {{
        display: grid;
        gap: 14px;
      }}

      .retail-row {{
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
      }}

      .retail-input {{
        display: inline-flex;
        align-items: center;
        gap: 10px;
        min-width: 230px;
        padding: 12px 16px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: var(--card);
      }}

      .retail-input span {{
        font-size: 1.1rem;
        font-weight: 800;
      }}

      .retail-input input {{
        width: 100%;
        border: 0;
        outline: none;
        background: transparent;
        color: var(--ink);
        font: inherit;
        font-weight: 700;
      }}

      .composer textarea {{
        width: 100%;
        min-height: 180px;
        resize: vertical;
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 18px 20px;
        font: inherit;
        color: var(--ink);
        background: var(--card);
        outline: none;
      }}

      .composer textarea:focus {{
        border-color: rgba(79, 143, 183, 0.55);
        box-shadow: 0 0 0 4px rgba(223, 240, 251, 0.9);
      }}

      .composer-row {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
      }}

      .composer-note {{
        color: var(--muted);
        font-size: 0.94rem;
      }}

      .action {{
        appearance: none;
        border: 0;
        border-radius: 999px;
        padding: 14px 20px;
        background: var(--blue-deep);
        color: white;
        font: inherit;
        font-weight: 800;
        letter-spacing: -0.02em;
        cursor: pointer;
      }}

      .action:disabled {{
        cursor: wait;
        opacity: 0.72;
      }}

      .results-head {{
        display: flex;
        justify-content: space-between;
        align-items: start;
        gap: 16px;
        flex-wrap: wrap;
      }}

      .results-summary {{
        color: var(--muted);
        line-height: 1.5;
        margin-top: 8px;
      }}

      .status-pill {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 9px 12px;
        border-radius: 999px;
        background: var(--blue-soft);
        color: var(--blue-deep);
        font-size: 0.82rem;
        font-weight: 800;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }}

      .price-grid {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 14px;
        margin-top: 18px;
      }}

      .price-card {{
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 18px;
        background: var(--card);
      }}

      .price-card .label {{
        color: var(--muted);
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
      }}

      .price-card .value {{
        display: block;
        margin-top: 10px;
        font-size: clamp(2rem, 4vw, 2.7rem);
        line-height: 0.92;
        letter-spacing: -0.07em;
        font-weight: 800;
      }}

      .price-card p {{
        margin-top: 10px;
        color: var(--muted);
        line-height: 1.5;
      }}

      .result-note {{
        margin-top: 14px;
        padding: 14px 16px;
        border-radius: 18px;
        background: linear-gradient(180deg, rgba(223, 240, 251, 0.6), rgba(255, 255, 255, 0.9));
        color: var(--muted);
        line-height: 1.5;
      }}

      .chart-wrap {{
        margin-top: 18px;
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--card);
        padding: 18px;
      }}

      .chart-svg {{
        display: block;
        width: 100%;
        height: auto;
      }}

      @media (max-width: 1120px) {{
        .hero,
        .sample-grid {{
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }}
      }}

      @media (max-width: 820px) {{
        .hero,
        .sample-grid,
        .price-grid {{
          grid-template-columns: 1fr;
        }}
      }}

      @media (max-width: 560px) {{
        .page {{
          padding: 14px;
        }}

        .topbar {{
          align-items: start;
          flex-direction: column;
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

        <section class="band hero">
          <div class="hero-copy">
            <h1>See what your purchase will be worth.</h1>
            <p>
              Pick an item, enter the retail price, and Spiffy estimates <strong>like new</strong>, <strong>good</strong>, and <strong>used</strong> resale prices.
            </p>
          </div>
        </section>

        <section class="band">
          <h2 class="section-title">What are you thinking of purchasing?</h2>
          <p class="section-subtitle">
            Pick an item to prefill the description, then revise the details you care about.
          </p>
          <div class="sample-grid" id="sample-grid"></div>
        </section>

        <section class="band composer">
          <div>
            <h2 class="section-title">Describe the clothing item</h2>
            <p class="section-subtitle">
              Include brand, category, size, fabric, color, era, and visible wear when you know it.
            </p>
          </div>
          <textarea id="description-input" placeholder="Example: Patagonia Synchilla fleece pullover in navy, men's medium, lightly worn with no stains or holes."></textarea>
          <div class="retail-row">
            <label class="retail-input" for="retail-price">
              <span>$</span>
              <input id="retail-price" type="number" min="1" step="0.01" placeholder="Retail price" />
            </label>
            <div class="composer-note">Retail price anchors the comparison chart.</div>
          </div>
          <div class="composer-row">
            <div class="composer-note" id="composer-note">Add details, then estimate the three resale prices.</div>
            <button class="action" id="estimate-button" type="button">Estimate prices</button>
          </div>
        </section>

        <section class="band">
          <div class="results-head">
            <div>
              <h2 class="section-title">Condition pricing</h2>
              <p class="results-summary" id="result-summary">No estimate yet.</p>
            </div>
            <div class="status-pill" id="result-provider">Awaiting input</div>
          </div>
          <div class="price-grid">
            <article class="price-card">
              <span class="label">Like New</span>
              <strong class="value" id="price-like-new">$0</strong>
              <p>Minimal visible wear. Best-case used-market presentation.</p>
            </article>
            <article class="price-card">
              <span class="label">Good</span>
              <strong class="value" id="price-good">$0</strong>
              <p>Normal pre-owned condition with light signs of use.</p>
            </article>
            <article class="price-card">
              <span class="label">Used</span>
              <strong class="value" id="price-used">$0</strong>
              <p>Clear wear, but still sellable and functional.</p>
            </article>
          </div>
          <div class="chart-wrap">
            <svg class="chart-svg" id="price-chart" viewBox="0 0 1100 440" role="img" aria-label="Retail-to-resale price chart"></svg>
          </div>
        </section>
      </div>
    </main>

    <script>
      const SAMPLES = {samples_json};
      const sampleGrid = document.getElementById("sample-grid");
      const descriptionInput = document.getElementById("description-input");
      const retailPriceInput = document.getElementById("retail-price");
      const estimateButton = document.getElementById("estimate-button");
      const composerNote = document.getElementById("composer-note");
      const resultSummary = document.getElementById("result-summary");
      const resultProvider = document.getElementById("result-provider");
      const priceLikeNew = document.getElementById("price-like-new");
      const priceGood = document.getElementById("price-good");
      const priceUsed = document.getElementById("price-used");
      const priceChart = document.getElementById("price-chart");

      function money(value) {{
        return new Intl.NumberFormat("en-US", {{
          style: "currency",
          currency: "USD",
          maximumFractionDigits: 0
        }}).format(value);
      }}

      function renderSamples() {{
        SAMPLES.forEach((item) => {{
          const button = document.createElement("button");
          button.type = "button";
          button.className = "sample-card";
          button.innerHTML = `
            <img class="sample-image" src="${{item.image}}" alt="${{item.name}}" />
            <span class="sample-tag">${{item.tag}}</span>
            <span class="sample-name">${{item.name}}</span>
            <span class="sample-description">${{item.description}}</span>
          `;
          button.addEventListener("click", () => {{
            descriptionInput.value = item.description;
            if (!retailPriceInput.value) {{
              retailPriceInput.value = "140";
            }}
            composerNote.textContent = `Prefilled from ${{item.name}}. Edit the prompt if you want more detail.`;
            descriptionInput.focus();
          }});
          sampleGrid.appendChild(button);
        }});
      }}

      function drawChart(retailPrice, prices) {{
        const width = 1100;
        const height = 440;
        const left = 320;
        const right = 400;
        const top = 76;
        const bottom = 76;
        const retailX = left + 24;
        const conditionX = width - right + 40;
        const conditionPoints = [
          {{ key: "like_new", label: "Like New", value: prices.like_new, color: "#4f8fb7" }},
          {{ key: "good", label: "Good", value: prices.good, color: "#77afd2" }},
          {{ key: "used", label: "Used", value: prices.used, color: "#9bc7e2" }},
        ];
        const laneGap = (height - top - bottom) / Math.max(conditionPoints.length - 1, 1);
        const yForIndex = (index) => top + index * laneGap;

        const guides = conditionPoints.map((item, index) => {{
          const y = yForIndex(index);
          return `
            <line x1="${{retailX}}" y1="${{y}}" x2="${{conditionX + 6}}" y2="${{y}}" stroke="rgba(24,33,41,0.08)" stroke-width="1" />
          `;
        }}).join("");

        const leftAnchor = `
          <text x="${{retailX - 130}}" y="${{yForIndex(1) - 34}}" font-size="24" fill="#5d6873">Retail</text>
          <text x="${{retailX - 130}}" y="${{yForIndex(1) + 2}}" font-size="40" font-weight="800" fill="#bb6946">${{money(retailPrice)}}</text>
        `;

        const segments = conditionPoints.map((item, index) => {{
          const y = yForIndex(index);
          return `
            <line x1="${{retailX}}" y1="${{yForIndex(1)}}" x2="${{conditionX}}" y2="${{y}}" stroke="${{item.color}}" stroke-width="3" stroke-linecap="round" />
            <circle cx="${{conditionX}}" cy="${{y}}" r="10" fill="${{item.color}}" />
            <text x="${{conditionX + 22}}" y="${{y - 12}}" font-size="22" fill="#5d6873">${{item.label}}</text>
            <text x="${{conditionX + 22}}" y="${{y + 30}}" font-size="38" font-weight="800" fill="${{item.color}}">${{money(item.value)}}</text>
          `;
        }}).join("");

        priceChart.innerHTML = `
          ${{guides}}
          ${{segments}}
          ${{leftAnchor}}
          <circle cx="${{retailX}}" cy="${{yForIndex(1)}}" r="11" fill="#bb6946" />
        `;
      }}

      function setBusy(isBusy) {{
        estimateButton.disabled = isBusy;
        estimateButton.textContent = isBusy ? "Estimating..." : "Estimate prices";
      }}

      async function estimatePrices() {{
        const description = descriptionInput.value.trim();
        const retailPrice = Number(retailPriceInput.value);
        if (description.length < 8) {{
          resultSummary.textContent = "Add a fuller clothing description before running inference.";
          resultProvider.textContent = "Need more detail";
          return;
        }}
        if (!Number.isFinite(retailPrice) || retailPrice <= 0) {{
          resultSummary.textContent = "Add a valid retail price before running inference.";
          resultProvider.textContent = "Need retail price";
          return;
        }}

        setBusy(true);
        resultSummary.textContent = "Generating the price estimate...";
        resultProvider.textContent = "Working";

        try {{
          const response = await fetch("/api/condition-prices", {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{ description, retail_price: retailPrice }})
          }});

          if (!response.ok) {{
            resultSummary.textContent = "The pricing request failed.";
            resultProvider.textContent = "Request failed";
            return;
          }}

          const payload = await response.json();
          priceLikeNew.textContent = money(payload.prices.like_new);
          priceGood.textContent = money(payload.prices.good);
          priceUsed.textContent = money(payload.prices.used);
          resultSummary.textContent = payload.item_summary;
          resultProvider.textContent = "Estimate ready";
          drawChart(payload.retail_price, payload.prices);
        }} catch (error) {{
          resultSummary.textContent = "The pricing request could not be completed.";
          resultProvider.textContent = "Unavailable";
        }} finally {{
          setBusy(false);
        }}
      }}

      estimateButton.addEventListener("click", estimatePrices);
      renderSamples();
    </script>
  </body>
</html>"""


@app.get("/api")
def api_root() -> dict[str, Any]:
    return {
        "message": "Spiffy condition pricing service",
        "example_request": pricing_request_example(),
        "sample_items": ITEM_OPTIONS,
        "provider": _pricing_backend.provider_name,
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", **_pricing_backend.health()}


@app.post("/predict")
@app.post("/api/condition-prices")
def condition_prices(payload: PricingRequest) -> dict[str, Any]:
    result = _pricing_backend.estimate(payload.description, payload.retail_price)
    return _result_payload(payload.description, payload.retail_price, result)
