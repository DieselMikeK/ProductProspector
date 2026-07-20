from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from product_prospector.core.product_model import Product
from product_prospector.core.wholesale_distributors import (
    WHOLESALE_DISTRIBUTORS,
    append_distributor_error_report,
    compact_distributor_error,
    distributor_supports_vendor,
    flatten_distributor_results,
    load_distributor_cookies,
    processing_distributors,
    save_distributor_cookies,
    selected_distributors,
)


class WholesaleDistributorTests(unittest.TestCase):
    @property
    def required_root(self) -> Path:
        return Path(__file__).resolve().parents[2] / "required"

    def test_compacts_playwright_firefox_launch_error_for_ui(self) -> None:
        error = (
            "Playwright Firefox unavailable: BrowserType.launch: Executable doesn't exist at "
            r"C:\Temp\_MEI123\playwright\firefox.exe\nPlease run playwright install"
        )
        self.assertEqual(compact_distributor_error(error), "Playwright Firefox unavailable")

    def test_compacts_expired_turn14_session_for_ui(self) -> None:
        error = "Turn 14 saved Chrome session is no longer valid. Its PHP session can expire independently."
        self.assertEqual(compact_distributor_error(error), "Authentication required")

    def test_full_distributor_error_is_written_to_private_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "wd_scrape_errors.log"
            result = append_distributor_error_report(
                "keystone",
                "MBRP-M1004",
                "Playwright Firefox unavailable: complete technical details",
                report_path=report_path,
            )
            self.assertEqual(result, report_path)
            record = json.loads(report_path.read_text(encoding="utf-8").strip())
            self.assertEqual(record["distributor"], "keystone")
            self.assertEqual(record["sku"], "MBRP-M1004")
            self.assertEqual(record["summary"], "Playwright Firefox unavailable")
            self.assertIn("complete technical details", record["details"])

    def test_selection_uses_display_order(self) -> None:
        selected = selected_distributors(["xdp", "keystone", "missing"])
        self.assertEqual([item.key for item in selected], ["keystone", "xdp"])
        self.assertEqual(len(WHOLESALE_DISTRIBUTORS), 5)

    def test_processing_places_turn14_search_url_last(self) -> None:
        selected = processing_distributors(["turn14", "xdp", "keystone", "meyer"])
        self.assertEqual([item.key for item in selected], ["keystone", "meyer", "xdp", "turn14"])

    def test_processing_search_urls_show_the_effective_endpoint(self) -> None:
        by_key = {item.key: item for item in WHOLESALE_DISTRIBUTORS}
        self.assertIn("/search?", by_key["keystone"].processing_search_url)
        self.assertIn("SearchTerm={sku}", by_key["keystone"].processing_search_url)
        self.assertEqual(
            by_key["turn14"].processing_search_url,
            "https://turn14.com/search/index.php?vmmPart={sku}",
        )
        self.assertEqual(
            by_key["premier_apg"].processing_search_url,
            "https://apgwholesale.com/pages/search-results-page?q={sku}",
        )
        self.assertEqual(
            by_key["meyer"].processing_search_url,
            "https://online.meyerdistributing.com/parts/search;search={sku};search_within={sku}",
        )

    def test_flattened_results_use_distributor_labels(self) -> None:
        result = flatten_distributor_results(
            {
                "keystone": {"price": "$339.15", "title": "Part"},
                "xdp": {"price": "$349.00"},
            },
            ["price"],
        )
        self.assertEqual(result["Keystone Automotive - price"], "$339.15")
        self.assertEqual(result["Xtreme Diesel Power - price"], "$349.00")
        self.assertNotIn("Keystone Automotive - title", result)

    def test_product_row_includes_results_and_failure_status(self) -> None:
        product = Product(
            sku="AFE-46-20819-B",
            audit_distributor_results={"turn14": {"price": "310.00"}},
            audit_distributor_errors={"meyer": "No matching product found."},
            audit_requested_fields=["price"],
        )
        row = product.to_row()
        self.assertEqual(row["Turn 14 Distribution - price"], "310.00")
        self.assertEqual(row["Meyer Distributing - status"], "No matching product found.")

    def test_bundled_vendor_availability_matches_aliases(self) -> None:
        self.assertTrue(distributor_supports_vendor("keystone", "AFE", self.required_root))
        self.assertTrue(distributor_supports_vendor("premier_apg", "ADS Shocks", self.required_root))
        self.assertFalse(distributor_supports_vendor("keystone", "ADS Shocks", self.required_root))
        self.assertFalse(distributor_supports_vendor("xdp", "4x4 Posi-Lok", self.required_root))

    def test_wd_brand_descriptors_do_not_create_false_vendor_skips(self) -> None:
        expected_matches = [
            ("keystone", "MBRP Exhaust"),
            ("keystone", "MagnaFlow Exhaust"),
            ("keystone", "K&N Filters"),
            ("keystone", "RECON Lighting"),
            ("keystone", "South Bend Clutch"),
            ("keystone", "BD-Power"),
            ("keystone", "Bilstein Shock Absorbers"),
            ("keystone", "Borgeson"),
            ("keystone", "Dana / Spicer"),
            ("keystone", "Injen Technology"),
            ("keystone", "Kleinn Air Horns"),
            ("keystone", "Powermaster Performance Starters and Alternators"),
            ("keystone", "Realtruck Superlift Suspension"),
            ("keystone", "Trigger Wireless Accessory Controller"),
            ("turn14", "Diamond Eye Performance"),
            ("turn14", "Rock Slide Engineering"),
            ("xdp", "AirDog | PureFlow Technologies"),
            ("xdp", "Beans Diesel | Bean Machine"),
            ("xdp", "Mopar | Factory OEM Dodge Ram"),
            ("xdp", "PSC"),
            ("xdp", "Rare Parts Steering and Suspension"),
            ("xdp", "RevMax Converters"),
            ("premier_apg", "Wehrli Custom Fabrication"),
        ]
        for distributor_key, vendor_name in expected_matches:
            with self.subTest(distributor=distributor_key, vendor=vendor_name):
                self.assertTrue(distributor_supports_vendor(distributor_key, vendor_name, self.required_root))

    def test_every_wd_matrix_entry_matches_its_own_catalog_identity(self) -> None:
        # Guards normalization changes against every vendor/site cell supplied
        # in WDVendorAvailability.csv, rather than one hand-picked example.
        import csv

        matrix_path = self.required_root / "mappings" / "WDVendorAvailability.csv"
        labels = {item.label: item.key for item in WHOLESALE_DISTRIBUTORS}
        with matrix_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        for row in rows:
            for label, distributor_key in labels.items():
                vendor_name = str(row.get(label, "") or "").strip()
                if not vendor_name:
                    continue
                with self.subTest(distributor=distributor_key, vendor=vendor_name):
                    self.assertTrue(distributor_supports_vendor(distributor_key, vendor_name, self.required_root))

    def test_product_row_includes_vendor_unavailable_status(self) -> None:
        product = Product(
            sku="SKU-1",
            audit_distributor_skips={"turn14": "Vendor Doesn't Exist"},
            audit_requested_fields=["price"],
        )
        self.assertEqual(product.to_row()["Turn 14 Distribution - status"], "Vendor Doesn't Exist")

    def test_cookie_sessions_load_only_from_private_local_app_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session_dir = Path(temp_dir) / "ProductProspector" / "wd_sessions"
            session_dir.mkdir(parents=True)
            (session_dir / "keystone.json").write_text(
                json.dumps({"cookies": [{"name": "session", "value": "private", "domain": ".example.com"}]}),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"LOCALAPPDATA": temp_dir}):
                cookies, error = load_distributor_cookies("keystone")
            self.assertIsNone(error)
            self.assertEqual(cookies[0]["name"], "session")

    def test_keystone_merges_rotating_live_firefox_cookies_with_saved_auth(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_app_data = Path(temp_dir) / "local"
            app_data = Path(temp_dir) / "roaming"
            session_dir = local_app_data / "ProductProspector" / "wd_sessions"
            session_dir.mkdir(parents=True)
            (session_dir / "keystone.json").write_text(
                json.dumps(
                    [
                        {"name": "ASP.NET_SessionId", "value": "auth", "domain": ".ekeystone.com", "path": "/"},
                        {"name": "visid_incap", "value": "old", "domain": ".ekeystone.com", "path": "/"},
                    ]
                ),
                encoding="utf-8",
            )

            profile = app_data / "Mozilla" / "Firefox" / "Profiles" / "test.default-release"
            profile.mkdir(parents=True)
            database = sqlite3.connect(profile / "cookies.sqlite")
            database.execute(
                "CREATE TABLE moz_cookies (name TEXT, value TEXT, host TEXT, path TEXT, expiry INTEGER, "
                "isSecure INTEGER, isHttpOnly INTEGER, sameSite INTEGER)"
            )
            database.execute(
                "INSERT INTO moz_cookies VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("visid_incap", "current", ".ekeystone.com", "/", 1999999999, 1, 1, 1),
            )
            database.commit()
            database.close()

            with patch.dict(
                os.environ,
                {"LOCALAPPDATA": str(local_app_data), "APPDATA": str(app_data)},
            ):
                cookies, error = load_distributor_cookies("keystone")

        self.assertIsNone(error)
        values = {str(item["name"]): str(item["value"]) for item in cookies}
        self.assertEqual(values["ASP.NET_SessionId"], "auth")
        self.assertEqual(values["visid_incap"], "current")

    def test_turn14_chrome_export_adds_apex_domain_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session_dir = Path(temp_dir) / "ProductProspector" / "wd_sessions"
            session_dir.mkdir(parents=True)
            (session_dir / "turn14.json").write_text(
                json.dumps(
                    [
                        {
                            "name": "PHPSESSID",
                            "value": "private",
                            "domain": "www.turn14.com",
                            "path": "/",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"LOCALAPPDATA": temp_dir}):
                cookies, error = load_distributor_cookies("turn14")

        self.assertIsNone(error)
        session_domains = {
            str(item.get("domain", ""))
            for item in cookies
            if item.get("name") == "PHPSESSID"
        }
        self.assertEqual(session_domains, {"www.turn14.com", ".turn14.com"})

    def test_fresh_turn14_export_is_saved_privately_and_previous_session_is_retained(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session_dir = Path(temp_dir) / "ProductProspector" / "wd_sessions"
            session_dir.mkdir(parents=True)
            session_path = session_dir / "turn14.json"
            session_path.write_text(
                json.dumps([{"name": "PHPSESSID", "value": "old", "domain": "www.turn14.com"}]),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"LOCALAPPDATA": temp_dir}):
                error = save_distributor_cookies(
                    "turn14",
                    [{"name": "PHPSESSID", "value": "fresh", "domain": "www.turn14.com", "path": "/"}],
                )
                cookies, load_error = load_distributor_cookies("turn14")

            previous = json.loads((session_dir / "turn14.previous.json").read_text(encoding="utf-8"))

        self.assertIsNone(error)
        self.assertIsNone(load_error)
        self.assertEqual(previous[0]["value"], "old")
        fresh_values = {
            str(item.get("value", ""))
            for item in cookies
            if item.get("name") == "PHPSESSID"
        }
        self.assertEqual(fresh_values, {"fresh"})


if __name__ == "__main__":
    unittest.main()
