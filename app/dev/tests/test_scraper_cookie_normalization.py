from __future__ import annotations

import unittest

from core.scraper_engine import _clean_playwright_cookies


class ScraperCookieNormalizationTests(unittest.TestCase):
    def test_normalizes_firefox_cookie_editor_fields_for_playwright(self) -> None:
        cookies = _clean_playwright_cookies(
            [
                {
                    "name": "session",
                    "value": "private",
                    "domain": ".example.com",
                    "path": "/",
                    "expirationDate": 1_900_000_000.25,
                    "sameSite": "no_restriction",
                    "hostOnly": False,
                    "storeId": "firefox-default",
                },
                {
                    "name": "preference",
                    "value": "",
                    "domain": "www.example.com",
                    "path": "/",
                    "sameSite": "lax",
                },
                {
                    "name": "firefox-milliseconds",
                    "value": "private",
                    "domain": ".example.com",
                    "path": "/",
                    "expirationDate": 1_900_000_000_000,
                },
                {
                    "name": "session-cookie",
                    "value": "private",
                    "domain": ".example.com",
                    "path": "/",
                    "expires": 0,
                },
            ]
        )

        self.assertEqual(len(cookies), 4)
        self.assertEqual(cookies[0]["expires"], 1_900_000_000.25)
        self.assertEqual(cookies[0]["sameSite"], "None")
        self.assertNotIn("expirationDate", cookies[0])
        self.assertNotIn("hostOnly", cookies[0])
        self.assertNotIn("storeId", cookies[0])
        self.assertEqual(cookies[1]["sameSite"], "Lax")
        self.assertEqual(cookies[2]["expires"], 1_900_000_000.0)
        self.assertNotIn("expires", cookies[3])


if __name__ == "__main__":
    unittest.main()
