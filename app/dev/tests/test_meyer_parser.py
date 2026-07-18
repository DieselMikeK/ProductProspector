from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.scraper_engine import (
    _meyer_payload_from_part,
    _select_meyer_part,
    scrape_vendor_records,
)


class MeyerParserTests(unittest.TestCase):
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
        def part(mpn: str, price: float) -> dict[str, object]:
            return {
                "meyerPart": f"MBR{mpn}",
                "mfgID": "MBR",
                "mfgName": "MBRP, Inc.",
                "mfgPart": mpn,
                "itemDesc": f"Quiet Tone Muffler {mpn}",
                "category": "Exhaust Mufflers",
                "imageUrl": f"/productimages/MBR/MBR{mpn}.jpg",
                "jobberPrice": 411.75,
                "customerPrice": price,
                "customerPriceWithoutExtraFees": price,
                "retailPrice": 349.99,
            }

        return {
            "searchResults": {
                "parts": [
                    part("M1004S", 167.43427),
                    part("M1004", 234.40076),
                    part("M1004A", 164.09016),
                ]
            }
        }

    def test_exact_mfg_part_wins_when_result_is_in_the_middle(self) -> None:
        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            part = _select_meyer_part(
                self._search_payload(),
                "M1004",
                "MBRP Exhaust",
                required_root,
            )

        self.assertEqual(part["mfgPart"], "M1004")
        payload = _meyer_payload_from_part(
            part,
            "https://online.meyerdistributing.com/parts/search;search=M1004;search_within=M1004",
            "MBRP Exhaust",
        )
        self.assertEqual(payload["price"], "234.40")
        self.assertIn("MBR/MBRM1004.jpg", payload["media_urls"])

    def test_scrape_flow_logs_in_once_and_uses_search_within_json(self) -> None:
        observed: list[tuple[str, dict[str, object], str]] = []

        def fake_post(url: str, payload: dict[str, object], access_token: str = "", timeout: int = 30):
            observed.append((url, payload, access_token))
            return self._search_payload(), None

        with TemporaryDirectory() as folder:
            required_root = self._required_root(folder)
            with (
                patch("core.scraper_engine._meyer_login_access_token", return_value=("private-token", None)) as login,
                patch("core.scraper_engine._meyer_post_json", side_effect=fake_post),
            ):
                records, errors, warnings = scrape_vendor_records(
                    vendor_search_url=(
                        "https://online.meyerdistributing.com/parts/search;"
                        "search={sku};search_within={sku}"
                    ),
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

        login.assert_called_once_with()
        self.assertEqual(errors, {})
        self.assertEqual(warnings, [])
        self.assertEqual(records["MBRP-M1004"]["price"], "234.40")
        self.assertEqual(records["MBRP-M1004"]["search_term"], "M1004")
        self.assertEqual(records["MBRP-M1004"]["search_provider"], "meyer_authenticated_search_json")
        self.assertEqual(observed[0][1]["searchWithin"], "M1004")
        self.assertEqual(observed[0][1]["numberPerPage"], 12)
        self.assertEqual(observed[0][2], "private-token")


if __name__ == "__main__":
    unittest.main()
