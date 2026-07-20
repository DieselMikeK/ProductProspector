from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from product_prospector.core.stock_bridge import StockBridgeError, assess_price_review, preview_wd_prices


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class StockBridgeClientTests(unittest.TestCase):
    @patch("urllib.request.urlopen")
    def test_preview_sends_bearer_key_and_current_sku_payload(self, urlopen):
        urlopen.return_value = _Response({"sku": "ABC-1", "results": []})
        result = preview_wd_prices(
            "ABC-1",
            [{"distributorKey": "turn14", "newProductCost": 12.34, "sourceUrl": "https://example.com/p"}],
            "secret-key",
            base_url="https://stockbridge.example",
        )

        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer secret-key")
        self.assertEqual(json.loads(request.data)["sku"], "ABC-1")
        self.assertEqual(result["sku"], "ABC-1")

    def test_preview_rejects_missing_key_before_network(self):
        with self.assertRaises(StockBridgeError):
            preview_wd_prices("ABC-1", [], "")

    def test_review_flags_low_margin_and_large_cost_change(self):
        review = assess_price_review(100.0, 50.0, 85.0)
        self.assertAlmostEqual(review["marginPercent"], 15.0)
        self.assertAlmostEqual(review["costChangePercent"], 70.0)
        self.assertIn("Warning: margin below 20%", review["warnings"])
        self.assertIn("Warning: cost changed more than 20%", review["warnings"])

    def test_review_flags_zero_or_negative_margin(self):
        review = assess_price_review(100.0, 90.0, 100.0)
        self.assertIn("Warning: zero/negative margin", review["warnings"])


if __name__ == "__main__":
    unittest.main()
