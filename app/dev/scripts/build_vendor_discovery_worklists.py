from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _profile_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        vendor = _clean(row.get("vendor"))
        if vendor:
            out[vendor] = row
    return out


def _unresolved_priority(website_status: str, review_notes: str) -> tuple[int, int]:
    status_rank = 0 if website_status == "confirmed" else 1
    note = review_notes.lower()
    note_rank = 4
    if "bot challenge" in note or "429" in note or "403" in note:
        note_rank = 0
    elif "405" in note:
        note_rank = 1
    elif "unknown page type" in note or "search returned no results" in note:
        note_rank = 2
    elif "no visible search input found" in note:
        note_rank = 3
    return status_rank, note_rank


def _resolved_priority(website_status: str, template_status: str, confidence: str) -> tuple[int, int, int]:
    website_rank = 0 if website_status == "confirmed" else 1
    status_order = {
        "confirmed": 0,
        "probed": 1,
        "detected": 2,
        "probable": 3,
        "custom": 4,
    }
    confidence_order = {
        "high": 0,
        "medium": 1,
        "review": 2,
        "low": 3,
        "blocked": 4,
    }
    return (
        website_rank,
        status_order.get(template_status, 9),
        confidence_order.get(confidence, 9),
    )


def build_worklists(master_path: Path, profiles_path: Path, unresolved_path: Path, resolved_path: Path) -> tuple[int, int]:
    master_rows = _load_csv(master_path)
    profile_rows = _load_csv(profiles_path)
    profile_by_vendor = _profile_map(profile_rows)

    unresolved_fields = [
        "manual_priority",
        "vendor",
        "display_name",
        "official_website_status",
        "official_website_url",
        "sample_sku",
        "review_notes",
        "platform_family",
        "search_family",
        "transport",
        "blocking_hint",
        "browser_required_hint",
        "resolver_hint",
    ]
    resolved_fields = [
        "success_priority",
        "vendor",
        "display_name",
        "official_website_status",
        "official_website_url",
        "sample_sku",
        "search_url_template",
        "search_template_status",
        "search_result_mode",
        "platform_family",
        "search_family",
        "transport",
        "parameter_name",
        "confidence_bucket",
        "browser_required_hint",
        "blocking_hint",
        "resolver_hint",
        "review_notes",
    ]

    unresolved_rows: list[dict[str, str]] = []
    resolved_rows: list[dict[str, str]] = []

    for row in master_rows:
        vendor = _clean(row.get("vendor"))
        website_status = _clean(row.get("official_website_status")).lower()
        if website_status not in {"confirmed", "probable"}:
            continue
        profile = profile_by_vendor.get(vendor, {})
        template = _clean(row.get("search_url_template"))
        notes = _clean(row.get("review_notes"))

        if not template:
            priority_key = _unresolved_priority(website_status, notes)
            unresolved_rows.append(
                {
                    "manual_priority": f"{priority_key[0]}-{priority_key[1]}",
                    "vendor": vendor,
                    "display_name": _clean(row.get("display_name")),
                    "official_website_status": website_status,
                    "official_website_url": _clean(row.get("official_website_url")),
                    "sample_sku": _clean(row.get("sample_sku")),
                    "review_notes": notes,
                    "platform_family": _clean(profile.get("platform_family")),
                    "search_family": _clean(profile.get("search_family")),
                    "transport": _clean(profile.get("transport")),
                    "blocking_hint": _clean(profile.get("blocking_hint")),
                    "browser_required_hint": _clean(profile.get("browser_required_hint")),
                    "resolver_hint": _clean(row.get("resolver_hint")),
                }
            )
            continue

        template_status = _clean(row.get("search_template_status")).lower()
        confidence = _clean(profile.get("confidence_bucket")).lower()
        priority_key = _resolved_priority(website_status, template_status, confidence)
        resolved_rows.append(
            {
                "success_priority": f"{priority_key[0]}-{priority_key[1]}-{priority_key[2]}",
                "vendor": vendor,
                "display_name": _clean(row.get("display_name")),
                "official_website_status": website_status,
                "official_website_url": _clean(row.get("official_website_url")),
                "sample_sku": _clean(row.get("sample_sku")),
                "search_url_template": template,
                "search_template_status": _clean(row.get("search_template_status")),
                "search_result_mode": _clean(row.get("search_result_mode")),
                "platform_family": _clean(profile.get("platform_family")),
                "search_family": _clean(profile.get("search_family")),
                "transport": _clean(profile.get("transport")),
                "parameter_name": _clean(profile.get("parameter_name")),
                "confidence_bucket": _clean(profile.get("confidence_bucket")),
                "browser_required_hint": _clean(profile.get("browser_required_hint")),
                "blocking_hint": _clean(profile.get("blocking_hint")),
                "resolver_hint": _clean(row.get("resolver_hint")),
                "review_notes": notes,
            }
        )

    unresolved_rows.sort(
        key=lambda item: (
            item["manual_priority"],
            item["vendor"].lower(),
        )
    )
    resolved_rows.sort(
        key=lambda item: (
            item["success_priority"],
            item["vendor"].lower(),
        )
    )

    with unresolved_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=unresolved_fields)
        writer.writeheader()
        writer.writerows(unresolved_rows)
    with resolved_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fields)
        writer.writeheader()
        writer.writerows(resolved_rows)

    return len(unresolved_rows), len(resolved_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build unresolved and resolved worklists from vendor discovery files.")
    parser.add_argument("--master", default="app/required/mappings/discovery/VendorDiscoveryMaster.csv")
    parser.add_argument("--profiles", default="app/required/mappings/discovery/VendorSearchStructureProfiles.csv")
    parser.add_argument("--unresolved-output", default="app/required/mappings/discovery/VendorDiscoveryUnresolvedWorklist.csv")
    parser.add_argument("--resolved-output", default="app/required/mappings/discovery/VendorDiscoveryResolvedWorklist.csv")
    args = parser.parse_args()

    unresolved_count, resolved_count = build_worklists(
        master_path=Path(args.master).expanduser().resolve(),
        profiles_path=Path(args.profiles).expanduser().resolve(),
        unresolved_path=Path(args.unresolved_output).expanduser().resolve(),
        resolved_path=Path(args.resolved_output).expanduser().resolve(),
    )
    print(f"Wrote {unresolved_count} unresolved rows and {resolved_count} resolved rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
