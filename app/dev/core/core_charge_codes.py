from __future__ import annotations

import csv
import re
from pathlib import Path


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def find_core_charge_codes_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "CoreChargeProductCodes.csv",
        required_root / "mappings" / "core_charge_product_codes.csv",
        required_root / "mappings" / "CoreCharges.csv",
        required_root / "mappings" / "core_charges.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def load_core_charge_codes(required_root: Path | None) -> set[str]:
    path = find_core_charge_codes_file(required_root)
    if path is None:
        return set()

    values: set[str] = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                for cell in row:
                    text = _clean_text(cell).upper()
                    if re.fullmatch(r"CORECHARGE-\d+", text):
                        values.add(text)
    except Exception:
        return set()
    return values


def normalize_core_charge_product_code(value: object, required_root: Path | None = None) -> str:
    text = _clean_text(value)
    if not text:
        return ""

    allowed_codes = load_core_charge_codes(required_root)
    upper = text.upper().replace(" ", "")

    direct_match = re.search(r"CORECHARGE-?(\d{1,5})", upper)
    if direct_match:
        candidate = f"CORECHARGE-{int(direct_match.group(1))}"
        if not allowed_codes or candidate in allowed_codes:
            return candidate

    number_matches = re.findall(r"\$?\s*([0-9]{1,5}(?:\.[0-9]{1,2})?)", text.replace(",", ""))
    for raw_number in number_matches:
        try:
            numeric = int(round(float(raw_number)))
        except Exception:
            continue
        if numeric <= 0:
            continue
        candidate = f"CORECHARGE-{numeric}"
        if not allowed_codes or candidate in allowed_codes:
            return candidate

    if re.fullmatch(r"CORECHARGE-\d+", upper):
        return upper
    return ""
