"""Vertex AI condition-pricing adapter with deterministic fallback behavior."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass


RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "item_summary": {"type": "STRING"},
        "like_new": {"type": "NUMBER"},
        "good": {"type": "NUMBER"},
        "used": {"type": "NUMBER"},
        "confidence_notes": {"type": "STRING"},
    },
    "required": ["item_summary", "like_new", "good", "used", "confidence_notes"],
    "propertyOrdering": [
        "item_summary",
        "like_new",
        "good",
        "used",
        "confidence_notes",
    ],
}


@dataclass
class PricingResult:
    item_summary: str
    retail_price: float
    like_new: float
    good: float
    used: float
    provider: str
    model: str
    confidence_notes: str
    warning: str | None = None


@dataclass
class VertexAISettings:
    project: str | None
    location: str
    model: str
    temperature: float

    @classmethod
    def from_env(cls) -> "VertexAISettings":
        return cls(
            project=os.getenv("GOOGLE_CLOUD_PROJECT"),
            location=os.getenv("VERTEX_AI_LOCATION", os.getenv("GOOGLE_CLOUD_LOCATION", "global")),
            model=os.getenv("VERTEX_AI_MODEL", "gemini-2.5-flash"),
            temperature=float(os.getenv("VERTEX_AI_TEMPERATURE", "0.2")),
        )


def _normalize_price_ladder(
    *,
    retail_price: float,
    like_new: float,
    good: float,
    used: float,
) -> tuple[float, float, float]:
    retail_price = round(max(retail_price, 15.0), 2)
    like_new = round(max(like_new, 12.0), 2)
    good = round(max(good, 10.0), 2)
    used = round(max(used, 8.0), 2)

    like_new = min(like_new, retail_price)
    good = min(good, retail_price)
    used = min(used, retail_price)

    if good >= like_new:
        good = round(max(like_new * 0.84, 10.0), 2)
    if used >= good:
        used = round(max(good * 0.76, 8.0), 2)

    return like_new, good, used


def _normalize_description(description: str) -> str:
    return re.sub(r"\s+", " ", description.strip())


class HeuristicPricingBackend:
    provider_name = "heuristic_fallback"
    model_name = "rule-based-fallback"

    _brand_multipliers = {
        "supreme": 1.7,
        "balenciaga": 2.1,
        "patagonia": 1.3,
        "levi": 1.0,
        "fear of god": 1.6,
        "essentials": 1.35,
        "jordan": 1.5,
        "nike": 1.2,
        "adidas": 1.05,
        "stussy": 1.25,
        "carhartt": 1.15,
    }
    _category_bases = {
        "jacket": 110.0,
        "coat": 145.0,
        "hoodie": 80.0,
        "sweatshirt": 72.0,
        "fleece": 78.0,
        "tee": 46.0,
        "t-shirt": 46.0,
        "shirt": 52.0,
        "jeans": 60.0,
        "denim": 60.0,
        "bag": 190.0,
        "sneaker": 150.0,
        "shoe": 135.0,
        "boots": 120.0,
        "dress": 85.0,
        "skirt": 58.0,
    }
    _feature_adjustments = {
        "vintage": 24.0,
        "archive": 32.0,
        "limited": 28.0,
        "rare": 26.0,
        "wool": 18.0,
        "cashmere": 42.0,
        "leather": 38.0,
        "silk": 22.0,
        "made in usa": 14.0,
    }

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        normalized = _normalize_description(description).lower()
        base_price = 68.0

        for category, category_base in self._category_bases.items():
            if category in normalized:
                base_price = category_base
                break

        multiplier = 1.0
        for brand, brand_multiplier in self._brand_multipliers.items():
            if brand in normalized:
                multiplier = max(multiplier, brand_multiplier)

        premium = sum(adjustment for token, adjustment in self._feature_adjustments.items() if token in normalized)

        target_good = round(base_price * multiplier + premium, 2)
        retail_cap = round(max(retail_price, 15.0), 2)
        good = round(min(target_good, retail_cap * 0.72), 2)
        like_new = round(min(max(good * 1.18, retail_cap * 0.64), retail_cap * 0.9), 2)
        used = round(min(good * 0.74, retail_cap * 0.56), 2)
        like_new, good, used = _normalize_price_ladder(
            retail_price=retail_cap,
            like_new=like_new,
            good=good,
            used=used,
        )

        return PricingResult(
            item_summary=_normalize_description(description),
            retail_price=retail_cap,
            like_new=like_new,
            good=good,
            used=used,
            provider=self.provider_name,
            model=self.model_name,
            confidence_notes=(
                "Fallback estimate based on item type, brand cues, and material keywords. "
                "Enable Vertex AI for model-backed pricing."
            ),
        )


class VertexAIPricingBackend:
    provider_name = "vertex_ai"

    def __init__(self, settings: VertexAISettings) -> None:
        if not settings.project:
            raise ValueError("GOOGLE_CLOUD_PROJECT is required for Vertex AI pricing.")
        self._settings = settings

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        from google import genai
        from google.genai.types import HttpOptions

        client = genai.Client(
            vertexai=True,
            project=self._settings.project,
            location=self._settings.location,
            http_options=HttpOptions(api_version="v1"),
        )

        prompt = f"""
You estimate current resale prices in USD for used clothing and accessories.

Task:
- Read the item description.
- Use the retail price as the upper anchor for plausible used-market prices.
- Infer a likely market segment, brand strength, garment type, and resale positioning.
- Return three resale prices for these conditions only: like_new, good, used.
- Prices must be realistic, descending, and expressed as plain numbers without currency symbols.
- The three used prices should not exceed the retail price.
- If the description is incomplete, make conservative assumptions.

Item description:
{_normalize_description(description)}

Retail price (USD):
{round(max(retail_price, 15.0), 2)}
""".strip()

        response = client.models.generate_content(
            model=self._settings.model,
            contents=prompt,
            config={
                "temperature": self._settings.temperature,
                "response_mime_type": "application/json",
                "response_schema": RESPONSE_SCHEMA,
            },
        )
        payload = json.loads(response.text)

        normalized_retail_price = round(max(retail_price, 15.0), 2)
        like_new, good, used = _normalize_price_ladder(
            retail_price=normalized_retail_price,
            like_new=float(payload["like_new"]),
            good=float(payload["good"]),
            used=float(payload["used"]),
        )

        return PricingResult(
            item_summary=str(payload["item_summary"]).strip(),
            retail_price=normalized_retail_price,
            like_new=like_new,
            good=good,
            used=used,
            provider=self.provider_name,
            model=self._settings.model,
            confidence_notes=str(payload["confidence_notes"]).strip(),
        )


class PricingBackendRouter:
    def __init__(
        self,
        *,
        primary: VertexAIPricingBackend | None,
        fallback: HeuristicPricingBackend,
        setup_warning: str | None,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._setup_warning = setup_warning

    @property
    def provider_name(self) -> str:
        if self._primary is not None:
            return self._primary.provider_name
        return self._fallback.provider_name

    def health(self) -> dict[str, str | bool | None]:
        return {
            "provider": self.provider_name,
            "vertex_ai_configured": self._primary is not None,
            "setup_warning": self._setup_warning,
        }

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        cleaned = _normalize_description(description)
        normalized_retail_price = round(max(retail_price, 15.0), 2)
        if self._primary is None:
            fallback_result = self._fallback.estimate(cleaned, normalized_retail_price)
            fallback_result.warning = self._setup_warning or "Vertex AI is not configured."
            return fallback_result

        try:
            return self._primary.estimate(cleaned, normalized_retail_price)
        except Exception as exc:
            fallback_result = self._fallback.estimate(cleaned, normalized_retail_price)
            fallback_result.warning = (
                "Vertex AI request failed, so the response came from the local fallback. "
                f"{exc.__class__.__name__}: {exc}"
            )
            return fallback_result


def build_pricing_backend() -> PricingBackendRouter:
    fallback = HeuristicPricingBackend()
    settings = VertexAISettings.from_env()

    if not settings.project:
        return PricingBackendRouter(
            primary=None,
            fallback=fallback,
            setup_warning="Set GOOGLE_CLOUD_PROJECT to enable Vertex AI pricing.",
        )

    try:
        primary = VertexAIPricingBackend(settings)
    except ImportError:
        return PricingBackendRouter(
            primary=None,
            fallback=fallback,
            setup_warning="Install google-genai to enable Vertex AI pricing.",
        )

    return PricingBackendRouter(primary=primary, fallback=fallback, setup_warning=None)
