from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

import clothing_mlops.service as service
from clothing_mlops.data_pipeline import pricing_request_example
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
        like_new = round(retail_price * 0.9, 2)
        good = round(retail_price * 0.74, 2)
        used = round(retail_price * 0.52, 2)
        return PricingResult(
            item_summary=f"Parsed item: {description}",
            retail_price=retail_price,
            like_new=like_new,
            good=good,
            used=used,
            provider=self.provider_name,
            model="gemini-test",
            confidence_notes="Structured test response.",
        )


class ServiceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._original_backend = service._pricing_backend
        service._pricing_backend = FakeBackend()
        cls.client = TestClient(service.app)

    @classmethod
    def tearDownClass(cls) -> None:
        service._pricing_backend = cls._original_backend

    def test_root_endpoint(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Describe the clothing item", response.text)
        self.assertIn("spiffy", response.text.lower())

    def test_ui_shell_contains_expected_hooks(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        html = response.text
        self.assertIn('id="sample-grid"', html)
        self.assertIn('id="description-input"', html)
        self.assertIn('id="retail-price"', html)
        self.assertIn('id="estimate-button"', html)
        self.assertIn('id="price-like-new"', html)
        self.assertIn('id="price-good"', html)
        self.assertIn('id="price-used"', html)
        self.assertIn('id="price-chart"', html)
        self.assertIn('fetch("/api/condition-prices"', html)
        self.assertIn("const SAMPLES =", html)
        for item in service.ITEM_CATALOG:
            self.assertIn(item["image"], html)

    def test_api_root_endpoint(self) -> None:
        response = self.client.get("/api")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["message"], "Spiffy condition pricing service")
        self.assertEqual(body["example_request"], pricing_request_example())
        self.assertEqual(body["sample_items"], service.ITEM_OPTIONS)

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ok",
                "provider": "fake_vertex",
                "vertex_ai_configured": True,
                "setup_warning": None,
            },
        )

    def test_predict_endpoint(self) -> None:
        response = self.client.post("/predict", json=pricing_request_example())
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["provider"], "fake_vertex")
        self.assertEqual(body["model"], "gemini-test")
        self.assertEqual(body["retail_price"], 139.0)
        self.assertEqual(body["prices"]["like_new"], 125.1)
        self.assertEqual(body["prices"]["good"], 102.86)
        self.assertEqual(body["prices"]["used"], 72.28)

    def test_condition_prices_endpoint(self) -> None:
        response = self.client.post("/api/condition-prices", json=pricing_request_example())
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["confidence_notes"], "Structured test response.")
        self.assertIn("Patagonia", body["item_summary"])

    def test_predict_validation(self) -> None:
        response = self.client.post("/predict", json={"description": "short", "retail_price": 139.0})
        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
