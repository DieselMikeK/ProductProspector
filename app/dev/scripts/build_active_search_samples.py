from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


DISCOVERY_ROOT = Path(__file__).resolve().parents[2] / "required" / "mappings" / "discovery"
MASTER_PATH = DISCOVERY_ROOT / "VendorDiscoveryMaster.csv"
PROFILES_PATH = DISCOVERY_ROOT / "VendorResolverProfiles.csv"
DETAILS_PATH = DISCOVERY_ROOT / "VendorEndToEndValidationDetails.csv"
OUTPUT_PATH = DISCOVERY_ROOT / "VendorActiveSearchSamples.csv"
HINTS_PATH = Path(__file__).resolve().parents[2] / "required" / "mappings" / "VendorSkuPrefixHints.csv"

FIELDNAMES = [
    "vendor",
    "display_name",
    "official_website_url",
    "search_url_template",
    "active_search_sku_1",
    "active_search_sku_2",
    "active_sample_status",
    "active_sample_source",
    "candidate_search_sku_1",
    "candidate_search_sku_2",
    "last_checked_utc",
    "notes",
]


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [{str(key): _clean(value) for key, value in row.items()} for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    profiles = {row["vendor"]: row for row in _load_csv(PROFILES_PATH)}
    master = {row["vendor"]: row for row in _load_csv(MASTER_PATH)}
    hint_rows = _load_csv(HINTS_PATH) if HINTS_PATH.exists() else []
    hints_by_vendor = {_clean(row.get("shopify_vendor")): row for row in hint_rows}
    detail_rows = _load_csv(DETAILS_PATH) if DETAILS_PATH.exists() else []

    verified_by_vendor: dict[str, list[str]] = defaultdict(list)
    seen_by_vendor: dict[str, set[str]] = defaultdict(set)
    for row in detail_rows:
        vendor = _clean(row.get("vendor"))
        sku = _clean(row.get("sku"))
        if not vendor or not sku or row.get("core_complete") != "yes":
            continue
        if sku in seen_by_vendor[vendor]:
            continue
        seen_by_vendor[vendor].add(sku)
        verified_by_vendor[vendor].append(sku)

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_rows: list[dict[str, str]] = []
    vendor_names = sorted(set(profiles) | set(master))
    for vendor in vendor_names:
        profile = profiles.get(vendor, {})
        master_row = master.get(vendor, {})
        hint_row = hints_by_vendor.get(vendor, {})
        verified = verified_by_vendor.get(vendor, [])
        hint_candidates = [
            _clean(hint_row.get("sample_sku_1")),
            _clean(hint_row.get("sample_sku_2")),
            _clean(hint_row.get("sample_sku_3")),
        ]
        candidate_values: list[str] = []
        for value in [
            _clean(master_row.get("sample_sku")),
            _clean(profile.get("sample_sku")),
            *hint_candidates,
        ]:
            if value and value not in candidate_values:
                candidate_values.append(value)
        candidate_1 = candidate_values[0] if len(candidate_values) >= 1 else ""
        candidate_2 = candidate_values[1] if len(candidate_values) >= 2 else ""

        status = ""
        source = ""
        notes = ""
        if verified:
            status = "verified_runtime"
            source = "end_to_end_validation"
            notes = "Prefer these SKUs for future validation runs."
        else:
            status = "pending_verification"
            source = "discovery_seed"
            notes = "Fallback candidate only until runtime validation confirms live SKUs."

        output_rows.append(
            {
                "vendor": vendor,
                "display_name": _clean(master_row.get("display_name")) or _clean(profile.get("display_name")) or vendor,
                "official_website_url": _clean(master_row.get("official_website_url")) or _clean(profile.get("official_website_url")),
                "search_url_template": _clean(master_row.get("search_url_template")) or _clean(profile.get("search_url_template")),
                "active_search_sku_1": verified[0] if len(verified) >= 1 else "",
                "active_search_sku_2": verified[1] if len(verified) >= 2 else "",
                "active_sample_status": status,
                "active_sample_source": source,
                "candidate_search_sku_1": candidate_1,
                "candidate_search_sku_2": candidate_2,
                "last_checked_utc": now_utc if verified else "",
                "notes": notes,
            }
        )

    _write_csv(OUTPUT_PATH, output_rows)
    verified_count = sum(1 for row in output_rows if _clean(row.get("active_search_sku_1")))
    print(f"Wrote {len(output_rows)} rows to {OUTPUT_PATH}")
    print(f"Vendors with verified active SKUs: {verified_count}")


if __name__ == "__main__":
    main()
