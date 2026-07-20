from __future__ import annotations

import unittest

import pandas as pd

from desktop_app import (
    _audit_margin_text,
    _audit_wd_source_url,
    _filter_vendor_autocomplete_choices,
    _shopify_selling_price_index,
)


class VendorAutocompleteTests(unittest.TestCase):
    def test_audit_margin_uses_shopify_selling_price_and_scraped_cost(self) -> None:
        self.assertEqual(_audit_margin_text("99.00", "66.33"), "Margin: +33%")
        self.assertEqual(_audit_margin_text("97.00", "66.93"), "Margin: +31%")
        self.assertEqual(_audit_margin_text("99.00", "78.46"), "Margin: +20.7%")
        self.assertEqual(_audit_margin_text("99.00", "120.00"), "Margin: -21.2%")

    def test_audit_margin_is_zero_when_either_value_is_missing(self) -> None:
        self.assertEqual(_audit_margin_text("", "78.46"), "Margin: 0%")
        self.assertEqual(_audit_margin_text("99.00", ""), "Margin: 0%")

    def test_shopify_selling_price_index_matches_normalized_sku(self) -> None:
        catalog = pd.DataFrame(
            [
                {"sku": " afe-24-91092 ", "price": "99.00"},
                {"sku": "MBRP-M1004", "price": "432.34"},
            ]
        )
        self.assertEqual(
            _shopify_selling_price_index(catalog),
            {"AFE-24-91092": "99.00", "MBRP-M1004": "432.34"},
        )

    def test_wd_source_url_prefers_final_product_url(self) -> None:
        self.assertEqual(
            _audit_wd_source_url(
                {
                    "search_url": "https://example.com/search?q=M1004",
                    "source_url": "https://example.com/source/M1004",
                    "product_url": "https://example.com/product/M1004",
                }
            ),
            "https://example.com/product/M1004",
        )

    def test_wd_source_url_falls_back_to_search_url(self) -> None:
        self.assertEqual(
            _audit_wd_source_url({"search_url": "https://example.com/search?q=M1004"}),
            "https://example.com/search?q=M1004",
        )

    def test_prefix_matches_are_ranked_first(self) -> None:
        choices = ["Air Lift Company", "MBRP Exhaust", "MagnaFlow Exhaust", "Banks Power"]
        self.assertEqual(
            _filter_vendor_autocomplete_choices("mb", choices),
            ["MBRP Exhaust"],
        )

    def test_word_prefix_and_substring_search_the_full_vendor_list(self) -> None:
        choices = ["Diamond Eye Performance", "MBRP Exhaust", "MagnaFlow Exhaust"]
        self.assertEqual(
            _filter_vendor_autocomplete_choices("exh", choices),
            ["MBRP Exhaust", "MagnaFlow Exhaust"],
        )
        self.assertEqual(
            _filter_vendor_autocomplete_choices("eye", choices),
            ["Diamond Eye Performance"],
        )

    def test_search_is_case_insensitive_and_preserves_canonical_value(self) -> None:
        self.assertEqual(
            _filter_vendor_autocomplete_choices("mBrP", ["MBRP Exhaust"]),
            ["MBRP Exhaust"],
        )


if __name__ == "__main__":
    unittest.main()
