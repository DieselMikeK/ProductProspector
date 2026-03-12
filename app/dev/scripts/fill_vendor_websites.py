from __future__ import annotations

import argparse
import csv
from pathlib import Path

import discover_vendor_search_urls as dv


def _load_master_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0].keys()) if rows else []
    return rows, fieldnames


def _write_master_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Incrementally fill VendorDiscoveryMaster official_website_url values.")
    parser.add_argument(
        "--vendor-profiles",
        default="app/required/mappings/VendorProfiles.csv",
        help="Path to VendorProfiles.csv",
    )
    parser.add_argument(
        "--master",
        default="app/required/mappings/discovery/VendorDiscoveryMaster.csv",
        help="Path to VendorDiscoveryMaster.csv",
    )
    parser.add_argument(
        "--search-provider",
        choices=["ddg", "bing", "google", "auto"],
        default="ddg",
        help="Search provider",
    )
    parser.add_argument("--vendor-filter", default="", help="Only process vendors matching this text.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of vendors to process (0 = all missing).")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay seconds between external requests.")
    parser.add_argument("--max-results-per-query", type=int, default=8, help="Search results to score.")
    args = parser.parse_args()

    vendor_profiles_path = Path(args.vendor_profiles).expanduser().resolve()
    master_path = Path(args.master).expanduser().resolve()
    profiles = dv._load_vendor_profiles(vendor_profiles_path)
    profile_by_vendor = {profile.canonical_vendor: profile for profile in profiles}

    rows, fieldnames = _load_master_rows(master_path)
    if not rows or not fieldnames:
        print(f"ERROR: no rows in master file: {master_path}")
        return 1

    vendors_to_process: list[dict[str, str]] = []
    needle = dv._clean_text(args.vendor_filter).lower()
    for row in rows:
        vendor = dv._clean_text(row.get("vendor", ""))
        if not vendor:
            continue
        if needle and needle not in vendor.lower() and needle not in dv._clean_text(row.get("display_name", "")).lower():
            continue
        if dv._clean_text(row.get("official_website_url", "")):
            continue
        vendors_to_process.append(row)
    if args.limit > 0:
        vendors_to_process = vendors_to_process[: args.limit]

    print(f"Missing vendors queued: {len(vendors_to_process)}")
    updated = 0
    for index, row in enumerate(vendors_to_process, start=1):
        vendor = dv._clean_text(row.get("vendor", ""))
        profile = profile_by_vendor.get(vendor)
        if profile is None:
            print(f"[{index}/{len(vendors_to_process)}] {vendor}: no profile")
            continue
        print(f"[{index}/{len(vendors_to_process)}] {vendor}")
        candidates, errors = dv._discover_vendor_website_candidates(
            vendor=profile,
            max_results_per_query=max(1, args.max_results_per_query),
            delay_seconds=max(0.0, args.delay),
            search_provider=args.search_provider,
            google_api_key="",
            google_cx="",
        )
        chosen = candidates[0] if candidates else None
        if chosen is None:
            row["official_website_status"] = "unresolved"
            row["official_website_source"] = f"search_discovery:{args.search_provider}"
            row["official_website_confidence"] = "low"
            row["review_notes"] = " | ".join(errors[:3])
            print(f"  unresolved: {' | '.join(errors[:2]) if errors else 'no candidates'}")
        else:
            confidence = "low"
            if chosen.score >= 120:
                confidence = "high"
            elif chosen.score >= 55:
                confidence = "medium"
            row["official_website_url"] = chosen.candidate_url
            row["official_website_status"] = "probable"
            row["official_website_source"] = f"search_discovery:{args.search_provider}"
            row["official_website_confidence"] = confidence
            row["review_notes"] = chosen.reasons
            updated += 1
            print(f"  -> {chosen.candidate_url} ({confidence}, score={chosen.score})")
        _write_master_rows(master_path, fieldnames, rows)

    print(f"Updated website rows: {updated}")
    print(f"Master file: {master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
