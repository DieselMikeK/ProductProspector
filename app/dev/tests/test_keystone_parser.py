from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.scraper_engine import (
    _extract_keystone_search_result_url,
    _extract_page_payload,
    _looks_like_bot_challenge,
    scrape_vendor_records,
)


class KeystoneParserTests(unittest.TestCase):
    @staticmethod
    def _required_root(folder: str) -> Path:
        root = Path(folder) / "required"
        mappings = root / "mappings"
        mappings.mkdir(parents=True)
        (mappings / "KeystoneVendorLineCodes.csv").write_text(
            "vendor,line_code,aliases\n"
            "Dorman,DOR,Dorman (OE Solutions)\n"
            "MBRP Exhaust,MBR,MBRP\n",
            encoding="utf-8",
        )
        return root

    def test_extracts_authenticated_server_rendered_detail_fields(self) -> None:
        html = """
        <html>
          <head><title>eKeystone - Detail</title></head>
          <body>
            <div class="product-detail-header-title">Dorman (OE Solutions) 521-922</div>
            <div class="product-detail-header-description">
              Control Arm; OE Replacement; Non-Adjustable; Black; Iron
            </div>
            <div><strong>Supplier:</strong> <a>Dorman (OE Solutions) (D18)</a></div>
            <span class="product-detail-header-pricing-amount">$226.02 USD</span>
          </body>
        </html>
        """

        payload = _extract_page_payload(
            html,
            "https://wwwsc.ekeystone.com/Search/Detail?pid=DOR521-922&sid=test",
            "521-922",
            scrape_images=False,
        )

        self.assertEqual(payload["title"], "Dorman (OE Solutions) 521-922")
        self.assertEqual(payload["vendor"], "Dorman (OE Solutions)")
        self.assertEqual(payload["price"], "226.02")
        self.assertEqual(payload["type"], "Control Arm")
        self.assertEqual(payload["detail_fetch_provider"], "keystone_authenticated_http_dom")

    def test_does_not_treat_description_tab_label_as_product_type(self) -> None:
        html = """
        <div class="product-detail-header-title">Dorman (OE Solutions) 521-922</div>
        <div class="product-detail-description">Product Description</div>
        <span class="product-detail-header-pricing-amount">$226.02 USD</span>
        """

        payload = _extract_page_payload(
            html,
            "https://wwwsc.ekeystone.com/Search/Detail?pid=DOR521-922&sid=test",
            "521-922",
            scrape_images=False,
        )

        self.assertNotIn("type", payload)

    def test_dorman_uses_validated_direct_url_before_universal_search_fallback(self) -> None:
        detail_html = """
        <div class="product-detail-header-title">Dorman (OE Solutions) 521-922</div>
        <span class="product-detail-header-pricing-amount">$226.02 USD</span>
        """

        def fake_fetch(url: str, **_kwargs):
            if "/Search/Detail?" in url:
                self.assertIn("pid=DOR521-922&", url)
                return url, detail_html, [], None
            return url, "", [], "unexpected URL"

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with patch("core.scraper_engine._fetch_html_with_real_firefox", side_effect=fake_fetch):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/",
                    skus=["521-922"],
                    workers=5,
                    retry_count=2,
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"521-922": "521-922"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="Dorman",
                )

        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["521-922"]["price"], "226.02")
        self.assertEqual(records["521-922"]["search_provider"], "keystone_direct_pid")

    def test_vendor_profile_prefix_builds_direct_keystone_url_for_unseeded_brand(self) -> None:
        detail_html = """
        <div class="product-detail-header-title">Advanced FLOW Engineering 24-91092</div>
        <span class="product-detail-header-pricing-amount">$126.10 USD</span>
        """

        def fake_fetch(url: str, **_kwargs):
            self.assertIn("pid=AFE24-91092&", url)
            return url, detail_html, [], None

        with TemporaryDirectory() as folder:
            required_root = Path(folder) / "required"
            mappings = required_root / "mappings"
            mappings.mkdir(parents=True)
            (mappings / "KeystoneVendorLineCodes.csv").write_text(
                "vendor,line_code,aliases\n",
                encoding="utf-8",
            )
            (mappings / "VendorProfiles.csv").write_text(
                "canonical_vendor,aliases,sku_prefix\nAFE,,AFE\n",
                encoding="utf-8",
            )
            with patch("core.scraper_engine._fetch_html_with_real_firefox", side_effect=fake_fetch):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["AFE-24-91092"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"AFE-24-91092": "24-91092"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="AFE",
                )

        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["AFE-24-91092"]["price"], "126.10")
        self.assertEqual(records["AFE-24-91092"]["search_provider"], "keystone_direct_pid")

    def test_keystone_result_picker_rejects_suffix_variants_and_selects_exact_mpn(self) -> None:
        markup = """
        <a href="/Search/Detail?pid=MBRM1004S&amp;sid=test">MBRP M1004S Exhaust Tip</a>
        <a href="/Search/Detail?pid=MBRM1004T&amp;sid=test">MBRP M1004T Exhaust Tip</a>
        <a href="/Search/Detail?pid=MBRM1004&amp;sid=test">MBRP Exhaust Tip M1004</a>
        """
        result = _extract_keystone_search_result_url(
            markup,
            "https://wwwsc.ekeystone.com/search?issl=1&SearchTerm=M1004",
            "M1004",
        )
        self.assertIn("pid=MBRM1004&", result)
        self.assertNotIn("M1004S", result)
        self.assertNotIn("M1004T", result)

    def test_arbitrary_vendor_uses_prefix_adjusted_search_and_exact_result(self) -> None:
        result_markup = """
        <a href="/Search/Detail?pid=MBRM1004S&amp;sid=test">MBRP M1004S Exhaust Tip</a>
        <a href="/Search/Detail?pid=MBRM1004T&amp;sid=test">MBRP M1004T Exhaust Tip</a>
        <a href="/Search/Detail?pid=MBRM1004&amp;sid=test">MBRP Exhaust Tip M1004</a>
        """
        detail_html = """
        <div class="product-detail-header-title">MBRP Exhaust Tip M1004</div>
        <span class="product-detail-header-pricing-amount">$42.50 USD</span>
        """

        def fake_fetch(url: str, **_kwargs):
            if "/search?" in url.lower():
                return url, result_markup, [], None
            if "/Search/Detail?" in url:
                self.assertIn("pid=MBRM1004&", url)
                if "sid=00000000-0000-0000-0000-000000000000" in url:
                    return url, """
                    <div class="product-detail-header-title">MBRP M1004S Exhaust Tip</div>
                    <span class="product-detail-header-pricing-amount">$41.00 USD</span>
                    """, [], None
                return url, detail_html, [], None
            return url, "", [], "unexpected URL"

        with TemporaryDirectory() as folder:
            required_root = Path(folder) / "required"
            mappings = required_root / "mappings"
            mappings.mkdir(parents=True)
            (mappings / "KeystoneVendorLineCodes.csv").write_text(
                "vendor,line_code,aliases\n",
                encoding="utf-8",
            )
            with patch("core.scraper_engine._fetch_html_with_real_firefox", side_effect=fake_fetch):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["MBRP-M1004"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-M1004": "M1004"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="MBRP Exhaust",
                )

        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "42.50")
        self.assertEqual(records["MBRP-M1004"]["search_term"], "M1004")
        self.assertEqual(records["MBRP-M1004"]["search_provider"], "keystone_search_browser_dom")

    def test_search_learns_line_code_for_unconfigured_vendor_and_reuses_direct_urls(self) -> None:
        first_url = "https://wwwsc.ekeystone.com/Search/Detail?pid=XYZABC123&sid=test"
        first_html = """
        <div class="product-detail-header-title">Unconfigured Brand ABC123</div>
        <span class="product-detail-header-pricing-amount">$19.95 USD</span>
        """
        second_html = """
        <div class="product-detail-header-title">Unconfigured Brand ABC124</div>
        <span class="product-detail-header-pricing-amount">$20.95 USD</span>
        """

        def fake_fetch(url: str, **_kwargs):
            if "pid=XYZABC123" in url:
                return url, first_html, [], None
            if "pid=XYZABC124" in url:
                return url, second_html, [], None
            return url, "", [], "unexpected URL"

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch(
                    "core.scraper_engine._resolve_keystone_search_result",
                    return_value=(first_url, "keystone_catalog_search_json", None),
                ) as resolve,
                patch("core.scraper_engine._fetch_html_with_real_firefox", side_effect=fake_fetch),
                patch("core.scraper_engine.time.sleep"),
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["PREFIX-ABC123", "PREFIX-ABC124"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"PREFIX-ABC123": "ABC123", "PREFIX-ABC124": "ABC124"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="Brand That Has Never Been Configured",
                )

        self.assertEqual(resolve.call_count, 1)
        self.assertEqual(resolve.call_args.args[:2], (
            "ABC123",
            [{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
        ))
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["PREFIX-ABC123"]["price"], "19.95")
        self.assertEqual(records["PREFIX-ABC124"]["price"], "20.95")
        self.assertEqual(records["PREFIX-ABC124"]["search_provider"], "keystone_direct_pid")

    def test_imperva_access_page_is_treated_as_a_bot_challenge(self) -> None:
        html = "<title>Access denied</title><div>Powered by Imperva</div><p>Incapsula Incident ID: 123</p>"
        self.assertTrue(_looks_like_bot_challenge(html))

    def test_normal_keystone_incapsula_resource_script_is_not_a_challenge(self) -> None:
        html = """
        <html><head><title>eKeystone - Detail</title></head>
        <body><script src="/_Incapsula_Resource?SWJIYLWA=1"></script>
        <div class="product-detail-header-title">MBRP M1004</div></body></html>
        """
        self.assertFalse(_looks_like_bot_challenge(html))

    def test_keystone_uses_firefox_when_http_session_lands_on_login(self) -> None:
        login_html = "<html><title>Login</title><body>Welcome Back <input type='password'></body></html>"
        detail_html = """
        <div class="product-detail-header-title">MBRP M1004</div>
        <span class="product-detail-header-pricing-amount">$411.75 USD</span>
        """

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch(
                    "core.scraper_engine._fetch_html_with_real_firefox",
                    return_value=(
                        "https://wwwsc.ekeystone.com/Search/Detail?pid=MBRM1004&sid=test",
                        detail_html,
                        [],
                        None,
                    ),
                ) as firefox_fetch,
                patch("core.scraper_engine._fetch_html_with_real_chrome") as chrome_fetch,
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["MBRP-M1004"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-M1004": "M1004"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="MBRP Exhaust",
                )

        firefox_fetch.assert_called_once()
        chrome_fetch.assert_not_called()
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "411.75")
        self.assertEqual(
            records["MBRP-M1004"]["detail_fetch_provider"],
            "keystone_authenticated_firefox_dom",
        )

    def test_keystone_stops_batch_after_first_firefox_challenge(self) -> None:
        login_html = "<html><title>Login</title><body>Welcome Back <input type='password'></body></html>"
        skus = ["MBRP-M1004", "MBRP-S6052PLM", "MBRP-S6007P"]

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch(
                    "core.scraper_engine._fetch_html_with_real_firefox",
                    return_value=("https://wwwsc.ekeystone.com/", "", [], "Bot challenge page detected"),
                ) as firefox_fetch,
                patch("core.scraper_engine.time.sleep"),
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=skus,
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={sku: sku.split("-", 1)[-1] for sku in skus},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="MBRP Exhaust",
                )

        self.assertEqual(records, {})
        self.assertEqual(firefox_fetch.call_count, 1)
        self.assertEqual(set(errors), set(skus))
        self.assertEqual(len(warnings), 1)

    def test_keystone_restarts_firefox_after_direct_pid_no_match(self) -> None:
        found_html = """
        <div class="product-detail-header-title">MBRP S6242P</div>
        <span class="product-detail-header-pricing-amount">$409.67 USD</span>
        """

        def fake_fetch(url: str, **_kwargs):
            if "pid=MBRMISSING" in url:
                return url, "<html><title>eKeystone - Detail</title></html>", [], None
            if "pid=MBRS6242P" in url:
                return url, found_html, [], None
            return url, "", [], "unexpected URL"

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch("core.scraper_engine._fetch_html_with_real_firefox", side_effect=fake_fetch),
                patch("core.scraper_engine._KeystoneFirefoxSession") as firefox_session_class,
                patch("core.scraper_engine.time.sleep"),
            ):
                firefox_session_class.return_value.reset_context.return_value = None
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
                    skus=["MBRP-MISSING", "MBRP-S6242P"],
                    delay_seconds=0,
                    scrape_images=False,
                    search_terms_by_sku={"MBRP-MISSING": "MISSING", "MBRP-S6242P": "S6242P"},
                    requested_fields={"price"},
                    required_root=required_root,
                    cookies=[{"name": "session", "value": "private", "domain": ".ekeystone.com"}],
                    vendor_name="MBRP Exhaust",
                )

        # The first exact spelling miss resets its Firefox context; the shared
        # distributor fallback may start a second bounded attempt using the
        # full/punctuation-adjusted SKU before reporting the final miss.
        self.assertGreaterEqual(firefox_session_class.call_count, 1)
        firefox_session_class.return_value.reset_context.assert_called()
        self.assertEqual(set(errors), {"MBRP-MISSING"})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-S6242P"]["price"], "409.67")


if __name__ == "__main__":
    unittest.main()
