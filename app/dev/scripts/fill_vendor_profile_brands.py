from __future__ import annotations

from pathlib import Path
import re
import sys

import pandas as pd


DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from product_prospector.core.shopify_brand_metaobjects import find_brand_metaobject_file, resolve_brand_metaobject_gid


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_aliases(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    return [_clean_text(item) for item in parts if _clean_text(item)]


def _resolve_required_root() -> Path:
    here = Path(__file__).resolve()
    dev_root = here.parents[1]
    runtime_app = dev_root.parent
    required_root = runtime_app / "required"
    required_root.mkdir(parents=True, exist_ok=True)
    return required_root


def _load_brand_name_by_gid(required_root: Path) -> dict[str, str]:
    path = find_brand_metaobject_file(required_root)
    if path is None:
        return {}
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str, keep_default_na=False)
    else:
        return {}
    if df.empty:
        return {}
    for col in ("brand_gid", "gid", "metaobject_gid"):
        if col in df.columns:
            gid_col = col
            break
    else:
        return {}
    for col in ("brand_name", "name", "display_name"):
        if col in df.columns:
            name_col = col
            break
    else:
        name_col = ""
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        gid = _clean_text(row.get(gid_col, ""))
        if not gid:
            continue
        name = _clean_text(row.get(name_col, "")) if name_col else ""
        if gid not in out:
            out[gid] = name
    return out


def main() -> int:
    required_root = _resolve_required_root()
    profiles_path = required_root / "mappings" / "VendorProfiles.csv"
    if not profiles_path.exists():
        print("ERROR: VendorProfiles.csv not found.")
        return 1

    df = pd.read_csv(profiles_path, dtype=str, keep_default_na=False)
    if df.empty:
        print("No rows found in VendorProfiles.csv")
        return 0

    for col in ("canonical_vendor", "aliases", "shopify_vendor_value", "brand_name", "brand_gid"):
        if col not in df.columns:
            df[col] = ""

    brand_name_by_gid = _load_brand_name_by_gid(required_root=required_root)

    updated_gid = 0
    updated_name = 0
    for idx, row in df.iterrows():
        brand_gid = _clean_text(row.get("brand_gid", ""))
        brand_name = _clean_text(row.get("brand_name", ""))
        if brand_gid and brand_name:
            continue

        candidates: list[str] = []
        if brand_name:
            candidates.append(brand_name)
        candidates.append(_clean_text(row.get("canonical_vendor", "")))
        candidates.append(_clean_text(row.get("shopify_vendor_value", "")))
        candidates.extend(_split_aliases(row.get("aliases", "")))
        candidates = [item for item in candidates if item]

        resolved_gid = brand_gid
        resolved_name = brand_name
        if not resolved_gid:
            for candidate in candidates:
                gid = resolve_brand_metaobject_gid(candidate, required_root=required_root)
                if gid:
                    resolved_gid = gid
                    if not resolved_name:
                        resolved_name = brand_name_by_gid.get(gid, "") or candidate
                    break
        elif not resolved_name:
            resolved_name = brand_name_by_gid.get(resolved_gid, "")

        if resolved_gid and resolved_gid != brand_gid:
            df.at[idx, "brand_gid"] = resolved_gid
            updated_gid += 1
        if resolved_name and resolved_name != brand_name:
            df.at[idx, "brand_name"] = resolved_name
            updated_name += 1

    df.to_csv(profiles_path, index=False, encoding="utf-8-sig")
    print(f"Updated VendorProfiles.csv: brand_gid +{updated_gid}, brand_name +{updated_name}")
    print(str(profiles_path.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
