from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from clothing_mlops.vertex_pricing import (
    HeuristicPricingBackend,
    PricingBackendRouter,
    PricingResult,
    VertexAIPricingBackend,
    VertexAISettings,
    build_pricing_backend,
)


class StubPrimaryBackend:
    provider_name = "vertex_ai"

    def __init__(self, result: PricingResult) -> None:
        self._result = result

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        return self._result


class RaisingPrimaryBackend:
    provider_name = "vertex_ai"

    def estimate(self, description: str, retail_price: float) -> PricingResult:
        raise RuntimeError("simulated primary failure")


class VertexPricingRouterTestCase(unittest.TestCase):
    def test_router_uses_primary_when_available(self) -> None:
        primary_result = PricingResult(
            item_summary="primary response",
            retail_price=200.0,
            like_new=170.0,
            good=135.0,
            used=95.0,
            provider="vertex_ai",
            model="gemini-test",
            confidence_notes="primary",
        )
        router = PricingBackendRouter(
            primary=StubPrimaryBackend(primary_result),
            fallback=HeuristicPricingBackend(),
            setup_warning=None,
        )

        result = router.estimate("Supreme hoodie", 200.0)

        self.assertEqual(result.item_summary, "primary response")
        self.assertEqual(result.provider, "vertex_ai")
        self.assertIsNone(result.warning)

    def test_router_falls_back_on_primary_exception(self) -> None:
        router = PricingBackendRouter(
            primary=RaisingPrimaryBackend(),
            fallback=HeuristicPricingBackend(),
            setup_warning=None,
        )

        result = router.estimate("Basic cotton tee", 120.0)

        self.assertEqual(result.provider, "heuristic_fallback")
        self.assertEqual(result.model, "rule-based-fallback")
        self.assertIsNotNone(result.warning)
        self.assertIn("Vertex AI request failed", result.warning or "")
        self.assertIn("RuntimeError", result.warning or "")

    def test_router_health_reflects_configuration(self) -> None:
        configured_router = PricingBackendRouter(
            primary=StubPrimaryBackend(
                PricingResult(
                    item_summary="ok",
                    retail_price=100.0,
                    like_new=85.0,
                    good=70.0,
                    used=50.0,
                    provider="vertex_ai",
                    model="gemini-test",
                    confidence_notes="ok",
                )
            ),
            fallback=HeuristicPricingBackend(),
            setup_warning=None,
        )
        unconfigured_router = PricingBackendRouter(
            primary=None,
            fallback=HeuristicPricingBackend(),
            setup_warning="Set GOOGLE_CLOUD_PROJECT to enable Vertex AI pricing.",
        )

        self.assertEqual(
            configured_router.health(),
            {"provider": "vertex_ai", "vertex_ai_configured": True, "setup_warning": None},
        )
        self.assertEqual(
            unconfigured_router.health(),
            {
                "provider": "heuristic_fallback",
                "vertex_ai_configured": False,
                "setup_warning": "Set GOOGLE_CLOUD_PROJECT to enable Vertex AI pricing.",
            },
        )


class VertexPricingBuildBackendTestCase(unittest.TestCase):
    def test_build_pricing_backend_without_project_uses_fallback(self) -> None:
        with patch("clothing_mlops.vertex_pricing.VertexAISettings.from_env") as mock_from_env:
            mock_from_env.return_value = VertexAISettings(
                project=None,
                location="global",
                model="gemini-2.5-flash",
                temperature=0.2,
            )
            backend = build_pricing_backend()

        self.assertEqual(backend.provider_name, "heuristic_fallback")
        self.assertEqual(
            backend.health(),
            {
                "provider": "heuristic_fallback",
                "vertex_ai_configured": False,
                "setup_warning": "Set GOOGLE_CLOUD_PROJECT to enable Vertex AI pricing.",
            },
        )

    def test_build_pricing_backend_with_project_uses_primary(self) -> None:
        with patch("clothing_mlops.vertex_pricing.VertexAISettings.from_env") as mock_from_env:
            with patch("clothing_mlops.vertex_pricing.VertexAIPricingBackend") as mock_primary_cls:
                with patch.dict(os.environ, {"PRICING_MODEL_PATH": ""}, clear=False):
                    mock_from_env.return_value = VertexAISettings(
                        project="demo-project",
                        location="global",
                        model="gemini-2.5-flash",
                        temperature=0.2,
                    )
                    primary_instance = mock_primary_cls.return_value
                    primary_instance.provider_name = "vertex_ai"

                    backend = build_pricing_backend()

        self.assertEqual(backend.provider_name, "vertex_ai")
        self.assertEqual(
            backend.health(),
            {"provider": "vertex_ai", "vertex_ai_configured": True, "setup_warning": None},
        )
        mock_primary_cls.assert_called_once()

    def test_build_pricing_backend_with_model_path_uses_xgboost(self) -> None:
        with patch("clothing_mlops.vertex_pricing.VertexAISettings.from_env") as mock_from_env, patch(
            "clothing_mlops.vertex_pricing.XGBoostHybridPricingBackend"
        ) as mock_xgb_cls, patch(
            "clothing_mlops.vertex_pricing.VertexAIPricingBackend"
        ) as mock_gemini_cls, patch.dict(
            os.environ,
            {"PRICING_MODEL_PATH": "tests/test_vertex_pricing.py"},
            clear=False,
        ):
            mock_from_env.return_value = VertexAISettings(
                project="demo-project",
                location="global",
                model="gemini-2.5-flash",
                temperature=0.2,
            )
            xgb_instance = mock_xgb_cls.return_value
            xgb_instance.provider_name = "xgboost_via_vertex"

            backend = build_pricing_backend()

        self.assertEqual(backend.provider_name, "xgboost_via_vertex")
        mock_xgb_cls.assert_called_once()
        mock_gemini_cls.assert_not_called()

    def test_build_pricing_backend_warns_when_model_missing(self) -> None:
        with patch("clothing_mlops.vertex_pricing.VertexAISettings.from_env") as mock_from_env, patch.dict(
            os.environ,
            {"PRICING_MODEL_PATH": "definitely/not/a/real/model.joblib"},
            clear=False,
        ):
            mock_from_env.return_value = VertexAISettings(
                project="demo-project",
                location="global",
                model="gemini-2.5-flash",
                temperature=0.2,
            )
            backend = build_pricing_backend()

        self.assertEqual(backend.provider_name, "heuristic_fallback")
        warning = backend.health()["setup_warning"]
        self.assertIsInstance(warning, str)
        self.assertIn("PRICING_MODEL_PATH", warning or "")

    def test_fallback_warning_is_attached_on_primary_failure(self) -> None:
        router = PricingBackendRouter(
            primary=RaisingPrimaryBackend(),
            fallback=HeuristicPricingBackend(),
            setup_warning=None,
        )

        result = router.estimate("Levi's denim jacket, lightly worn.", 140.0)

        self.assertEqual(result.provider, "heuristic_fallback")
        self.assertIsNotNone(result.warning)
        self.assertIn("simulated primary failure", result.warning or "")


if __name__ == "__main__":
    unittest.main()
