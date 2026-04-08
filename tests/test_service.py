from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from clothing_mlops.data_pipeline import prediction_example
from clothing_mlops.service import app, refresh_model


class ServiceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        refresh_model()
        cls.client = TestClient(app)

    def test_root_endpoint(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("What are you thinking of purchasing?", response.text)
        self.assertIn("spiffy", response.text.lower())

    def test_api_root_endpoint(self) -> None:
        response = self.client.get("/api")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["message"], "Spiffy value prediction service")
        self.assertEqual(body["example_request"], prediction_example())

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"status": "ok", "model_loaded": True},
        )

    def test_predict_endpoint(self) -> None:
        response = self.client.post("/predict", json=prediction_example())
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["model_status"], "ok")
        self.assertIsInstance(body["predicted_sale_price"], float)

    def test_predict_validation(self) -> None:
        response = self.client.post("/predict", json={"brand": "Patagonia"})
        self.assertEqual(response.status_code, 422)

    def test_lifetime_curve_endpoint(self) -> None:
        response = self.client.post(
            "/api/lifetime-curve",
            json={
                "item_name": "fear of god essentials hoodie",
                "purchase_price": 140.0,
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["curve_type"], "medium_depreciation")
        self.assertEqual(body["points"][0]["value"], 140.0)
        self.assertLess(body["points"][-1]["value"], body["points"][0]["value"])


if __name__ == "__main__":
    unittest.main()
