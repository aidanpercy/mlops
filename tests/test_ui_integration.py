from __future__ import annotations

import json
import re
import unittest

from fastapi.testclient import TestClient

import clothing_mlops.service as service
from clothing_mlops.vertex_pricing import PricingResult


class FakeBackend:
    provider_name = "fake_vertex"

    def health(self) -> dict[str, str | bool | None]:
        return {
            "provider": self.provider_name,
            "vertex_ai_configured": True,
            "setup_warning": None,
        }

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        like_new = round(retail_price * 0.88, 2)
        good = round(retail_price * 0.71, 2)
        used = round(retail_price * 0.49, 2)
        return PricingResult(
            item_summary="Fear of God Essentials hoodie, medium-weight neutral fleece.",
            retail_price=retail_price,
            like_new=like_new,
            good=good,
            used=used,
            provider=self.provider_name,
            model="gemini-test",
            confidence_notes=f"Inferred pricing from: {description}",
        )


class UiIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._original_backend = service._pricing_backend
        service._pricing_backend = FakeBackend()
        cls.client = TestClient(service.app)

    @classmethod
    def tearDownClass(cls) -> None:
        service._pricing_backend = cls._original_backend

    def test_homepage_catalog_matches_api_sample_items(self) -> None:
        home = self.client.get("/")
        self.assertEqual(home.status_code, 200)
        html = home.text

        match = re.search(r"const SAMPLES = (\[.*?\]);", html, flags=re.DOTALL)
        self.assertIsNotNone(match, "Could not find embedded SAMPLES catalog in homepage script.")
        embedded_items = json.loads(match.group(1))
        embedded_names = [item["name"] for item in embedded_items]

        api_response = self.client.get("/api")
        self.assertEqual(api_response.status_code, 200)
        api_items = api_response.json()["sample_items"]

        self.assertEqual(embedded_names, api_items)

    def test_predict_response_reflects_backend_behavior(self) -> None:
        payload = {
            "description": "Fear of God Essentials hoodie in oatmeal, lightly worn, men's medium.",
            "retail_price": 160.0,
        }
        response = self.client.post("/predict", json=payload)
        self.assertEqual(response.status_code, 200)

        body = response.json()
        self.assertEqual(body["provider"], "fake_vertex")
        self.assertEqual(body["retail_price"], 160.0)
        self.assertEqual(body["prices"]["like_new"], 140.8)
        self.assertEqual(body["prices"]["good"], 113.6)
        self.assertEqual(body["prices"]["used"], 78.4)

    def test_condition_price_payload_is_descending(self) -> None:
        payload = {
            "description": "Vintage Supreme box logo tee, size large, no stains, lightly worn.",
            "retail_price": 200.0,
        }
        response = self.client.post("/api/condition-prices", json=payload)
        self.assertEqual(response.status_code, 200)
        body = response.json()

        prices = body["prices"]
        self.assertGreater(prices["like_new"], prices["good"])
        self.assertGreater(prices["good"], prices["used"])
        self.assertIn("Inferred pricing from", body["confidence_notes"])


if __name__ == "__main__":
    unittest.main()
