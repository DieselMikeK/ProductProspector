from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.scraper_engine import (
    _apg_payload_from_search_item,
    _extract_apg_search_card,
    _select_apg_search_item,
    scrape_vendor_records,
)


class ApgParserTests(unittest.TestCase):
    @staticmethod
    def _required_root(folder: str) -> Path:
        root = Path(folder) / "required"
        mappings = root / "mappings"
        mappings.mkdir(parents=True)
        (mappings / "VendorProfiles.csv").write_text(
            "canonical_vendor,aliases,sku_prefix\nMBRP Exhaust,MBRP,MBRP\n",
            encoding="utf-8",
        )
        return root

    @staticmethod
    def _search_payload() -> dict[str, object]:
        def item(code: str, handle: str, price: str) -> dict[str, object]:
            return {
                "title": f"MBRP Quiet Tone {code}",
                "description": "MBRP Exhaust Quiet Tone Muffler",
                "link": f"/products/{handle}",
                "price": price,
                "list_price": "349.9900",
                "product_code": code,
                "vendor": "MBRP",
                "shopify_images": [f"https://cdn.example/{handle}.jpg"],
                "shopify_variants": [
                    {
                        "sku": code,
                        "barcode": "882663113002",
                        "price": price,
                        "link": f"/products/{handle}?variant=1",
                    }
                ],
            }

        return {
            "items": [
                item("MBRM1004S", "m1004s", "165.7300"),
                item("MBRM1004", "m1004", "232.0200"),
                item("MBRM1004A", "m1004a", "162.4100"),
            ]
        }

    def test_json_result_picker_uses_exact_gray_sku_suffix(self) -> None:
        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            item = _select_apg_search_item(
                self._search_payload(),
                "M1004",
                "MBRP-M1004",
                "MBRP Exhaust",
                required_root,
            )

        self.assertEqual(item["product_code"], "MBRM1004")
        payload = _apg_payload_from_search_item(
            item,
            "https://apgwholesale.com/pages/search-results-page?q=M1004",
            "MBRP Exhaust",
        )
        self.assertEqual(payload["price"], "232.02")
        self.assertEqual(payload["barcode"], "882663113002")
        self.assertIn("/products/m1004?variant=1", payload["product_url"])

    def test_dom_fallback_rejects_suffix_variants(self) -> None:
        html = """
        <ul>
          <li class="snize-product"><a href="/products/m1004s"><span class="snize-title">M1004S</span><span class="snize-sku">MBRM1004S</span></a></li>
          <li class="snize-product"><a href="/products/m1004"><span class="snize-title">M1004</span><span class="snize-description">Exact</span><span class="snize-sku">MBRM1004</span></a></li>
          <li class="snize-product"><a href="/products/m1004a"><span class="snize-title">M1004A</span><span class="snize-sku">MBRM1004A</span></a></li>
        </ul>
        """
        item = _extract_apg_search_card(html, "M1004")
        self.assertEqual(item["product_code"], "MBRM1004")
        self.assertEqual(item["link"], "/products/m1004")

    def test_scrape_flow_uses_searchanise_json_without_browser(self) -> None:
        api_body = json.dumps(self._search_payload())

        def fake_fetch(url: str, timeout: int = 30):
            self.assertIn("searchserverapi1.com/getresults", url)
            self.assertIn("q=M1004", url)
            return api_body, None

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch("core.scraper_engine._fetch_html", side_effect=fake_fetch),
                patch("core.scraper_engine._fetch_html_with_real_chrome") as browser_fetch,
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://apgwholesale.com/pages/search-results-page?q={sku}",
                    skus=["MBRP-M1004"],
                    workers=3,
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-M1004": "M1004"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[],
                    vendor_name="MBRP Exhaust",
                )

        browser_fetch.assert_not_called()
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "232.02")
        self.assertEqual(records["MBRP-M1004"]["search_term"], "M1004")
        self.assertEqual(records["MBRP-M1004"]["search_provider"], "apg_searchanise_json")


if __name__ == "__main__":
    unittest.main()
