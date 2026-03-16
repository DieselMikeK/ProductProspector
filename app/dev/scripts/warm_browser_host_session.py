from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
from pathlib import Path

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from core.scraper_engine import (
    _browser_session_profile_path,
    _browser_session_state_path,
    _clean_text,
    _looks_like_bot_challenge,
    _real_chrome_executable_path,
)


def _default_output_path(url: str) -> Path:
    host = _clean_text(urllib.parse.urlparse(url).netloc).lower()
    return _browser_session_state_path(host)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Open a real browser session for a blocked host and save the solved cookies for later scraper reuse."
    )
    parser.add_argument("--url", required=True, help="Blocked URL to open in the browser, e.g. a Holley search or product page.")
    parser.add_argument("--output", default="", help="Optional storage_state JSON output path.")
    parser.add_argument("--profile-dir", default="", help="Optional persistent Chrome profile dir for this host.")
    parser.add_argument("--click-selector", default="", help="Optional selector to click after the initial page load, e.g. first product link on a search results page.")
    parser.add_argument("--timeout-seconds", type=int, default=180, help="How long to wait for the challenge to clear.")
    parser.add_argument("--headless", action="store_true", help="Use headless mode. Default is headed because this flow is meant for manual challenge solve.")
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"Playwright unavailable: {exc}")
        return 1

    target_url = _clean_text(args.url)
    if not target_url:
        print("Missing --url")
        return 1
    host = _clean_text(urllib.parse.urlparse(target_url).netloc).lower()
    output_path = Path(args.output).expanduser().resolve() if _clean_text(args.output) else _default_output_path(target_url)
    profile_dir = (
        Path(args.profile_dir).expanduser().resolve()
        if _clean_text(args.profile_dir)
        else _browser_session_profile_path(host)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    chrome_path = _real_chrome_executable_path()
    if not chrome_path:
        print("Google Chrome executable not found")
        return 1

    deadline = time.time() + max(15, int(args.timeout_seconds))
    print(f"Opening browser for: {target_url}")
    print(f"Saving session to: {output_path}")
    print(f"Using profile dir: {profile_dir}")
    print("If a bot/interstitial page appears, solve it in the browser window and wait.")

    with sync_playwright() as play:
        context = play.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            executable_path=chrome_path,
            headless=bool(args.headless),
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        click_selector = _clean_text(args.click_selector)
        click_attempted = False

        solved = False
        last_title = ""
        last_url = ""
        while time.time() < deadline:
            page.wait_for_timeout(2000)
            try:
                html = page.content()
                last_title = _clean_text(page.title())
                last_url = _clean_text(page.url)
            except Exception:
                html = ""
            if click_selector and not click_attempted and html and not _looks_like_bot_challenge(html):
                try:
                    locator = page.locator(click_selector).first
                    if locator.count() > 0:
                        locator.click(timeout=5000)
                        click_attempted = True
                        page.wait_for_timeout(2500)
                        continue
                except Exception:
                    pass
            if html and not _looks_like_bot_challenge(html) and last_title.lower() not in {"just a moment...", "verifying your connection..."}:
                solved = True
                break

        state = context.storage_state()
        output_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        context.close()

    print(f"Final URL: {last_url or target_url}")
    print(f"Final title: {last_title}")
    print(f"Saved cookies: {len(state.get('cookies', [])) if isinstance(state, dict) else 0}")
    if solved:
        print("Session saved after challenge cleared.")
        return 0
    print("Session saved, but the page still looked challenged when the timeout expired.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
