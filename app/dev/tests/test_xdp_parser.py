from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.scraper_engine import (
    _extract_xtreme_customer_price,
    _resolve_xdp_variant_payload,
    _score_searchspring_item,
    _search_term_fallbacks,
    scrape_vendor_records,
)


class XdpParserTests(unittest.TestCase):
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
    def _searchspring_results() -> dict[str, object]:
        return {
            "results": [
                {
                    "name": 'MBRP Universal 4&quot; Quiet Tone Muffler',
                    "mpn": ["M1004", "M1004S", "M1004A"],
                    "price": "244.99",
                    "manufacturer_name": ["MBRP Inc."],
                    "product_url": "https://www.xtremediesel.com/mbrp-universal-4-quiet-tone-muffler",
                },
                {
                    "name": 'MBRP Armor Pro 4&quot; Quiet Tone Muffler M1004',
                    "sku": "MBM1004",
                    "mpn": ["M1004"],
                    "price": "349.99",
                    "manufacturer_name": ["MBRP Inc."],
                    "product_url": "https://www.xtremediesel.com/mbrp-universal-4-quiet-tone-muffler-mbm1004",
                },
                {
                    "name": 'MBRP Armor Plus 4&quot; Quiet Tone Muffler M1004S',
                    "sku": "MBM1004S",
                    "mpn": ["M1004S"],
                    "price": "249.99",
                    "manufacturer_name": ["MBRP Inc."],
                    "product_url": "https://www.xtremediesel.com/mbrp-universal-4-quiet-tone-muffler-mbm1004s",
                },
            ]
        }

    def test_exact_mpn_outranks_suffix_variant(self) -> None:
        items = self._searchspring_results()["results"]
        exact = items[1]
        suffix = items[2]

        self.assertGreater(
            _score_searchspring_item(exact, "M1004"),
            _score_searchspring_item(suffix, "M1004"),
        )

    def test_xdp_customer_price_outranks_map_street_price(self) -> None:
        html = """
        <div class="cmp-dealer-price__column">
          <span class="cmp-dealer-price__title">XDP Price</span>
          <span id="js-price-value" data-hook="product-details__price" content="66.93">$66.93</span>
        </div>
        <div class="cmp-dealer-price__column">
          <span class="cmp-dealer-price__title">Map/Street Price</span>
          <s id="js-price-value-additional">$97.00</s>
        </div>
        """

        self.assertEqual(_extract_xtreme_customer_price(html), "66.93")

    def test_search_fallbacks_remove_vendor_prefix_and_hyphens(self) -> None:
        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            self.assertEqual(
                _search_term_fallbacks(
                    "MBRP-12-34",
                    "MBRP-12-34",
                    vendor_name="MBRP Exhaust",
                    required_root=required_root,
                ),
                ["MBRP-12-34", "12-34", "MBRP1234", "1234"],
            )

    def test_xdp_variant_is_resolved_through_miva_without_browser_clicks(self) -> None:
        html = """
        <script>
        window.am189164 = new AttributeMachine({"product_code":"81284"});
        window.amAttributes189164 = {"success":1,"data":[
          {"id":46700,"type":"radio","options":[
            {"id":142821,"code":"exhaust_tip_finish_black_exhaust_tips","prompt":"Black Exhaust Tips"},
            {"id":142822,"code":"exhaust_tip_finish_polished_exhaust_tips","prompt":"Polished Exhaust Tips"}
          ]}
        ]}; window.amPossible189164 = {"success":1};
        </script>
        <table>
          <tr><th>Part Number</th><th>Exhaust Tip Finish</th></tr>
          <tr><td>49-34130-B</td><td>Black Exhaust Tips</td></tr>
          <tr><td>49-34130-P</td><td>Polished Exhaust Tips</td></tr>
        </table>
        """
        variant_response = json.dumps(
            {
                "success": 1,
                "data": {
                    "variant": {"variant_id": 144650},
                    "have_price": True,
                    "price": 1057.08,
                },
            }
        )
        part_response = json.dumps(
            {
                "success": 1,
                "data": [
                    {
                        "sku": "AFE49-34130-B",
                        "customfield_values": {"customfields": {"mpn": "49-34130-B"}},
                    }
                ],
            }
        )

        with (
            patch(
                "core.scraper_engine._post_form_html",
                return_value=("https://www.xtremediesel.com/mm5/json.mvc", variant_response, None),
            ) as post_form,
            patch("core.scraper_engine._fetch_html", return_value=(part_response, None)),
            patch("core.scraper_engine._fetch_html_with_real_chrome") as browser_fetch,
        ):
            payload, error = _resolve_xdp_variant_payload(
                html,
                "https://www.xtremediesel.com/afe-49-34130-vulcan-series-3-dpf-back-exhaust-system",
                "AFE-49-34130-B",
            )

        self.assertIsNone(error)
        self.assertEqual(payload["price"], "1057.08")
        self.assertTrue(payload["product_url"].endswith("?variant_id=144650"))
        self.assertEqual(payload["detail_fetch_provider"], "xdp_miva_variant_api")
        self.assertEqual(post_form.call_args.args[1]["Selected_Option_IDs"], "142821")
        browser_fetch.assert_not_called()

    def test_distributor_retry_uses_compact_search_only_after_exact_miss(self) -> None:
        attempted_terms: list[str] = []

        def fake_keystone(**kwargs):
            sku = kwargs["sku_values"][0]
            term = kwargs["search_terms_by_sku"][sku]
            attempted_terms.append(term)
            if term == "1234":
                return {sku: {"price": "10.00", "search_term": term}}, {}, []
            return {}, {sku: f"No exact Keystone product match found for {term}."}, []

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with patch("core.scraper_engine._scrape_keystone_records", side_effect=fake_keystone):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["MBRP-12-34"],
                    workers=1,
                    delay_seconds=0,
                    retry_count=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-12-34": "12-34"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "test"}],
                    vendor_name="MBRP Exhaust",
                )

        self.assertEqual(attempted_terms, ["12-34", "1234"])
        self.assertEqual(records["MBRP-12-34"]["price"], "10.00")
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])

    def test_scrape_flow_uses_searchspring_without_loading_bot_protected_search_page(self) -> None:
        api_body = json.dumps(self._searchspring_results())
        requested_urls: list[str] = []
        product_html = """
        <html><body>
          <h1>MBRP Armor Pro 4&quot; Quiet Tone Muffler M1004</h1>
          <span class="cmp-dealer-price__title">XDP Price</span>
          <span id="js-price-value" data-hook="product-details__price" content="238.82">$238.82</span>
          <span class="cmp-dealer-price__title">Map/Street Price</span>
          <s id="js-price-value-additional">$349.99</s>
        </body></html>
        """

        def fake_fetch(url: str, timeout: int = 30):
            requested_urls.append(url)
            if "k72wrs.a.searchspring.io/api/search/search.json" in url:
                if "bgfilter.mpn=M1004" in url:
                    return api_body, None
                return json.dumps({"results": []}), None
            if url.endswith("-mbm1004"):
                return product_html, None
            return "", "HTTP 403: bot challenge"

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch("core.scraper_engine._fetch_html", side_effect=fake_fetch),
                patch("core.scraper_engine._fetch_html_with_real_chrome") as browser_fetch,
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://www.xtremediesel.com/xtreme-diesel-performance-xdp-search?q={sku}",
                    skus=["MBRP-M1004"],
                    workers=1,
                    delay_seconds=0,
                    retry_count=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-M1004": "M1004"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[],
                    vendor_name="MBRP Exhaust",
                )

        browser_fetch.assert_not_called()
        self.assertFalse(any("xtreme-diesel-performance-xdp-search" in url for url in requested_urls))
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "238.82")
        self.assertEqual(records["MBRP-M1004"]["search_term"], "M1004")
        self.assertEqual(records["MBRP-M1004"]["search_provider"], "searchspring_mpn")
        self.assertTrue(records["MBRP-M1004"]["product_url"].endswith("-mbm1004"))


if __name__ == "__main__":
    unittest.main()
