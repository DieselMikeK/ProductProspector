from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.scraper_engine import (
    _extract_turn14_search_card,
    _turn14_search_part,
    scrape_vendor_records,
)


def _result_card(part: str, orange_price: str, suffix: str = "") -> str:
    return f"""
    <article class="search-result product-card">
      <h2 class="part-number">Part #: <span>{part}</span></h2>
      <div><b>Manufacturer:</b> MBRP <b>Pricing Group:</b> MBRP</div>
      <div><b>Description:</b> Universal Quiet Tone Muffler {suffix}</div>
      <div><b>Product Name:</b> MBRP Muffler {suffix}</div>
      <div class="reference-prices">
        <span>MAP</span><span>$349.99</span>
        <span>Jobber</span><span>$411.75</span>
        <span>Retail</span><span>$432.34</span>
      </div>
      <strong class="text-t14-orange customer-price">${orange_price}</strong>
      <span>Jobber - 42%</span>
    </article>
    """


class Turn14ParserTests(unittest.TestCase):
    def test_exact_compact_part_wins_over_suffix_variants(self) -> None:
        html = (
            _result_card("mbrpM1004S", "170.58", "409")
            + _result_card("mbrpM1004", "238.82", "304")
            + _result_card("mbrpM1004A", "167.17", "AL")
        )

        payload = _extract_turn14_search_card(
            html,
            "MBRP-M1004",
            "https://turn14.com/search/index.php?vmmPart=MBRP-M1004",
            "MBRP Exhaust",
        )

        self.assertEqual(payload["price"], "238.82")
        self.assertEqual(payload["vendor"], "MBRP Exhaust")
        self.assertEqual(payload["title"], "MBRP Muffler 304")
        self.assertEqual(payload["search_provider"], "turn14_search_card_dom")

    def test_matching_is_generic_for_case_and_punctuation(self) -> None:
        html = (
            _result_card("xyzABC123T", "21.00")
            + _result_card("xyzABC123", "19.95")
        )

        payload = _extract_turn14_search_card(
            html,
            "XYZ-ABC-123",
            "https://turn14.com/search/index.php?vmmPart=XYZ-ABC-123",
            "Example Brand",
        )

        self.assertEqual(payload["price"], "19.95")

    def test_search_uses_existing_full_sku_or_rebuilds_prefix_from_vendor_profile(self) -> None:
        with TemporaryDirectory() as folder:
            required_root = Path(folder) / "required"
            mappings = required_root / "mappings"
            mappings.mkdir(parents=True)
            (mappings / "VendorProfiles.csv").write_text(
                "canonical_vendor,aliases,sku_prefix\nMBRP Exhaust,MBRP,MBRP\n",
                encoding="utf-8",
            )

            self.assertEqual(
                _turn14_search_part("MBRP-M1004", "M1004", "MBRP Exhaust", required_root),
                "MBRP-M1004",
            )
            self.assertEqual(
                _turn14_search_part("M1004", "M1004", "MBRP Exhaust", required_root),
                "MBRP-M1004",
            )

    def test_scrape_flow_uses_turn14_endpoint_and_card_price(self) -> None:
        html = _result_card("mbrpM1004S", "170.58") + _result_card("mbrpM1004", "238.82")

        def fake_fetch(url: str, timeout: int = 30):
            self.assertEqual(
                url,
                "https://turn14.com/search/index.php?vmmPart=MBRP-M1004",
            )
            return html, None

        with TemporaryDirectory() as folder:
            required_root = Path(folder) / "required"
            mappings = required_root / "mappings"
            mappings.mkdir(parents=True)
            (mappings / "VendorProfiles.csv").write_text(
                "canonical_vendor,aliases,sku_prefix\nMBRP Exhaust,MBRP,MBRP\n",
                encoding="utf-8",
            )
            with patch("core.scraper_engine._fetch_html", side_effect=fake_fetch):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://turn14.com/search/index.php?vmmPart={sku}",
                    skus=["MBRP-M1004"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-M1004": "M1004"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "PHPSESSID", "value": "private", "domain": ".turn14.com"}],
                    vendor_name="MBRP Exhaust",
                )

        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "238.82")
        self.assertEqual(records["MBRP-M1004"]["search_provider"], "turn14_search_card_dom")
        self.assertEqual(records["MBRP-M1004"]["detail_fetch_provider"], "turn14_authenticated_http_dom")


if __name__ == "__main__":
    unittest.main()
