from __future__ import annotations

import csv
import json
import re
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

import pandas as pd

from product_prospector.core.config_store import ShopifyConfig


DEFAULT_FITMENT_VEHICLE_METAOBJECT_FILE = "ShopifyFitmentVehicleMetaobjects.csv"

_KNOWN_ENGINE_CODES = {
    "LB7",
    "LLY",
    "LBZ",
    "LMM",
    "LML",
    "L5P",
    "LM2",
    "LZO",
    "LWN",
    "IDI",
}
_ENGINE_CODE_STOPWORDS = {
    "CHEVY",
    "CHEVROLET",
    "GMC",
    "GM",
    "FORD",
    "RAM",
    "DODGE",
    "NISSAN",
    "JEEP",
    "CUMMINS",
    "DURAMAX",
    "POWERSTROKE",
    "ECODIESEL",
    "DETROIT",
}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_make_key(value: str) -> str:
    text = _normalize_key(value)
    if not text:
        return ""
    if re.search(r"\b(chevy|chevrolet)\b", text):
        return "chevrolet"
    if re.search(r"\bgmc\b", text):
        return "gmc"
    if re.search(r"\b(gm|general motors)\b", text):
        return "gm"
    if re.search(r"\b(ram|dodge)\b", text):
        return "ram"
    if re.search(r"\bford\b", text):
        return "ford"
    if re.search(r"\bnissan\b", text):
        return "nissan"
    if re.search(r"\bjeep\b", text):
        return "jeep"
    return text.split(" ")[0]


def _normalize_family_key(value: str) -> str:
    text = _normalize_key(value)
    if not text:
        return ""
    if "duramax" in text:
        return "duramax"
    if "powerstroke" in text:
        return "powerstroke"
    if "cummins" in text:
        return "cummins"
    if "ecodiesel" in text:
        return "ecodiesel"
    if re.search(r"\bidi\b", text):
        return "idi"
    return ""


def _normalize_engine_liter(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    match = re.search(r"(\d\.\d)", text)
    if not match:
        return ""
    return f"{match.group(1)}L"


def _extract_engine_code(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    upper = text.upper()
    for code in _KNOWN_ENGINE_CODES:
        if re.search(rf"\b{re.escape(code)}\b", upper):
            return code
    tokens = re.findall(r"\b[A-Z0-9]{2,5}\b", upper)
    for token in tokens:
        if token in _ENGINE_CODE_STOPWORDS:
            continue
        if re.fullmatch(r"\dL", token):
            continue
        if re.search(r"[A-Z]", token):
            return token
    return ""


def _coerce_gid(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if re.match(r"^gid://shopify/Metaobject/\d+$", text):
        return text
    digits = re.sub(r"\D+", "", text)
    if digits:
        return f"gid://shopify/Metaobject/{digits}"
    return ""


def _parse_year_float(value: str) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    has_half = ".5" in text
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if len(digits) >= 4:
        base = int(digits[:4])
    elif len(digits) == 2:
        yy = int(digits)
        if yy >= 80:
            base = 1900 + yy
        elif yy <= 30:
            base = 2000 + yy
        else:
            return None
    else:
        return None
    return float(base) + (0.5 if has_half else 0.0)


def _normalize_year_value(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) * 2.0) / 2.0


def _is_half_year(value: float | None) -> bool:
    normalized = _normalize_year_value(value)
    if normalized is None:
        return False
    return abs(normalized - int(normalized)) > 0.001


def _format_year_value(value: float | None) -> str:
    normalized = _normalize_year_value(value)
    if normalized is None:
        return ""
    if not _is_half_year(normalized):
        return str(int(normalized))
    return f"{normalized:.1f}"


def _request_graphql(config: ShopifyConfig, access_token: str, query: str, variables: dict) -> tuple[dict | None, str | None]:
    query_text = _clean_text(query).lower()
    if re.search(r"\bmutation\b", query_text):
        return None, "Read-only guard blocked non-query GraphQL operation."
    if not re.match(r"^\s*query\b", query_text):
        return None, "Read-only guard requires GraphQL query operations only."

    url = f"https://{config.shop_domain}/admin/api/{config.api_version}/graphql.json"
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Shopify-Access-Token": access_token,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return None, f"Shopify HTTP {exc.code}: {detail}"
    except Exception as exc:
        return None, str(exc)

    try:
        parsed = json.loads(body)
    except Exception:
        return None, "Invalid JSON response from Shopify GraphQL."

    errors = parsed.get("errors") or []
    if errors:
        messages: list[str] = []
        for error in errors:
            if isinstance(error, dict):
                message = _clean_text(error.get("message", ""))
                if message:
                    messages.append(message)
        return None, "; ".join(messages) or "Shopify GraphQL returned errors."

    return parsed.get("data") or {}, None


_PRODUCT_FITMENT_TYPE_QUERY = """
query ProductFitmentVehicleType($cursor: String) {
  products(first: 75, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        metafield(namespace: "fitment", key: "vehicles") {
          references(first: 1) {
            nodes {
              __typename
              ... on Metaobject {
                type
              }
            }
          }
        }
      }
    }
  }
}
"""

_METAOBJECTS_QUERY = """
query FitmentVehicleMetaobjects($type: String!, $cursor: String) {
  metaobjects(type: $type, first: 250, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      type
      handle
      displayName
      updatedAt
      fields {
        key
        value
      }
    }
  }
}
"""

_METAOBJECT_PING_QUERY = """
query MetaobjectPing($type: String!) {
  metaobjects(type: $type, first: 1) {
    nodes {
      id
    }
  }
}
"""


def detect_fitment_vehicle_metaobject_type(
    config: ShopifyConfig,
    access_token: str,
    max_product_pages: int = 12,
) -> tuple[str, str | None]:
    cursor: str | None = None
    page = 0
    while page < max_product_pages:
        page += 1
        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_PRODUCT_FITMENT_TYPE_QUERY,
            variables={"cursor": cursor},
        )
        if error:
            break
        products = (data or {}).get("products") or {}
        edges = products.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            metafield = node.get("metafield") or {}
            references = (metafield.get("references") or {}).get("nodes") or []
            for reference in references:
                if _clean_text((reference or {}).get("__typename")) != "Metaobject":
                    continue
                reference_type = _clean_text((reference or {}).get("type"))
                if reference_type:
                    return reference_type, None
        page_info = products.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

    for candidate in [
        "fitment_vehicle",
        "fitment-vehicle",
        "fitmentvehicle",
        "vehicle_fitment",
        "vehicles",
    ]:
        _, ping_error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_METAOBJECT_PING_QUERY,
            variables={"type": candidate},
        )
        if ping_error is None:
            return candidate, "Fitment vehicle type inferred from fallback candidate."

    return "fitment_vehicle", "Could not auto-detect fitment vehicle type; using default 'fitment_vehicle'."


def _extract_fitment_fields(display_name: str, field_map: dict[str, str]) -> tuple[str, str, str, str, str]:
    normalized_map = {_normalize_key(key).replace(" ", "_"): _clean_text(value) for key, value in field_map.items()}

    year_text = (
        normalized_map.get("year")
        or normalized_map.get("model_year")
        or normalized_map.get("fitment_year")
        or display_name
    )
    make_text = (
        normalized_map.get("make")
        or normalized_map.get("vehicle_make")
        or normalized_map.get("brand")
        or display_name
    )
    liter_text = (
        normalized_map.get("engine_liter")
        or normalized_map.get("liter")
        or normalized_map.get("engine_size")
        or normalized_map.get("engine")
        or display_name
    )
    family_text = (
        normalized_map.get("engine_family")
        or normalized_map.get("family")
        or normalized_map.get("engine")
        or display_name
    )
    code_text = (
        normalized_map.get("engine_code")
        or normalized_map.get("code")
        or normalized_map.get("generation")
        or display_name
    )

    year_value = _parse_year_float(year_text) or _parse_year_float(display_name)
    year = _format_year_value(year_value)
    if not year:
        year_match = re.search(r"\b(19|20)\d{2}\b", year_text)
        year = year_match.group(0) if year_match else ""
    make = _normalize_make_key(make_text)
    liter = _normalize_engine_liter(liter_text)
    family = _normalize_family_key(family_text)
    code = _extract_engine_code(code_text)
    return year, make, liter, family, code


def fetch_fitment_vehicle_metaobjects(
    config: ShopifyConfig,
    access_token: str,
    metaobject_type: str | None = None,
    max_pages: int = 200,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None, str]:
    resolved_type = _clean_text(metaobject_type)
    detect_note: str | None = None
    if not resolved_type:
        resolved_type, detect_note = detect_fitment_vehicle_metaobject_type(config=config, access_token=access_token)

    rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    cursor: str | None = None
    page = 0
    while page < max_pages:
        page += 1
        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_METAOBJECTS_QUERY,
            variables={"type": resolved_type, "cursor": cursor},
        )
        if error:
            return pd.DataFrame(), error, resolved_type

        metaobjects = (data or {}).get("metaobjects") or {}
        nodes = metaobjects.get("nodes") or []
        for node in nodes:
            gid = _coerce_gid(_clean_text((node or {}).get("id")))
            if not gid or gid in seen_ids:
                continue
            seen_ids.add(gid)
            fields = (node or {}).get("fields") or []
            field_map: dict[str, str] = {}
            for item in fields:
                key = _clean_text((item or {}).get("key"))
                if not key or key in field_map:
                    continue
                field_map[key] = _clean_text((item or {}).get("value"))
            display_name = _clean_text((node or {}).get("displayName"))
            year, make, liter, family, code = _extract_fitment_fields(display_name=display_name, field_map=field_map)
            rows.append(
                {
                    "fitment_vehicle_name": display_name,
                    "fitment_vehicle_handle": _clean_text((node or {}).get("handle")),
                    "fitment_vehicle_gid": gid,
                    "metaobject_type": _clean_text((node or {}).get("type")) or resolved_type,
                    "aliases": "",
                    "updated_at": _clean_text((node or {}).get("updatedAt")),
                    "year": year,
                    "make": make,
                    "engine_liter": liter,
                    "engine_family": family,
                    "engine_code": code,
                    "fields_json": json.dumps(field_map, ensure_ascii=False),
                }
            )

        if progress_callback is not None:
            try:
                progress_callback(page, len(rows))
            except Exception:
                pass

        page_info = metaobjects.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

    columns = [
        "fitment_vehicle_name",
        "fitment_vehicle_handle",
        "fitment_vehicle_gid",
        "metaobject_type",
        "aliases",
        "updated_at",
        "year",
        "make",
        "engine_liter",
        "engine_family",
        "engine_code",
        "fields_json",
    ]
    output = pd.DataFrame(rows, columns=columns)
    if output.empty:
        output = pd.DataFrame(columns=columns)
    else:
        output = output.fillna("")
        output["fitment_vehicle_name"] = output["fitment_vehicle_name"].astype(str).str.strip()
        output["fitment_vehicle_gid"] = output["fitment_vehicle_gid"].astype(str).str.strip()
        output["make"] = output["make"].astype(str).str.strip().str.lower()
        output["engine_liter"] = output["engine_liter"].astype(str).str.strip()
        output["engine_family"] = output["engine_family"].astype(str).str.strip().str.lower()
        output["engine_code"] = output["engine_code"].astype(str).str.strip().str.upper()
        output = output.drop_duplicates(subset=["fitment_vehicle_gid"], keep="first")
        output = output.sort_values(by=["year", "make", "fitment_vehicle_name"], kind="stable").reset_index(drop=True)
    if detect_note:
        return output, detect_note, resolved_type
    return output, None, resolved_type


def default_fitment_vehicle_metaobject_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_FITMENT_VEHICLE_METAOBJECT_FILE


def save_fitment_vehicle_metaobjects_table(df: pd.DataFrame, output_path: Path) -> tuple[int, str | None]:
    out = df.copy() if df is not None else pd.DataFrame()
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "fitment_vehicle_name",
                "fitment_vehicle_handle",
                "fitment_vehicle_gid",
                "metaobject_type",
                "aliases",
                "updated_at",
                "year",
                "make",
                "engine_liter",
                "engine_family",
                "engine_code",
                "fields_json",
            ]
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if output_path.suffix.lower() == ".xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                out.to_excel(writer, index=False, sheet_name="fitment_vehicles")
        else:
            out.to_csv(output_path, index=False, encoding="utf-8-sig")
    except Exception as exc:
        return 0, str(exc)
    return int(len(out)), None


def find_fitment_vehicle_metaobject_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "ShopifyFitmentVehicleMetaobjects.csv",
        required_root / "mappings" / "shopify_fitment_vehicle_metaobjects.csv",
        required_root / "mappings" / "FitmentVehicleMetaobjects.csv",
        required_root / "mappings" / "fitment_vehicle_metaobjects.csv",
        required_root / "mappings" / "ShopifyFitmentVehicleMetaobjects.xlsx",
        required_root / "mappings" / "fitment_vehicle_metaobjects.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    return pd.DataFrame()


@lru_cache(maxsize=16)
def _load_fitment_vehicle_records_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict, ...]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    table = _read_table(path)
    if table.empty:
        return tuple()

    columns = list(table.columns)
    normalized_columns = [_normalize_key(column).replace(" ", "_") for column in columns]

    gid_col = ""
    name_col = ""
    year_col = ""
    make_col = ""
    liter_col = ""
    family_col = ""
    code_col = ""
    fields_json_col = ""

    for index, normalized in enumerate(normalized_columns):
        col = columns[index]
        if not gid_col and normalized in {"fitment_vehicle_gid", "gid", "metaobject_gid", "id"}:
            gid_col = col
        if not name_col and normalized in {"fitment_vehicle_name", "display_name", "name", "title"}:
            name_col = col
        if not year_col and normalized in {"year", "model_year", "fitment_year"}:
            year_col = col
        if not make_col and normalized in {"make", "vehicle_make", "brand"}:
            make_col = col
        if not liter_col and normalized in {"engine_liter", "liter", "engine_size"}:
            liter_col = col
        if not family_col and normalized in {"engine_family", "family"}:
            family_col = col
        if not code_col and normalized in {"engine_code", "code", "generation"}:
            code_col = col
        if not fields_json_col and normalized in {"fields_json", "fields", "metaobject_fields", "field_json"}:
            fields_json_col = col

    if not gid_col:
        return tuple()
    if not name_col and columns:
        name_col = columns[0]

    records: list[dict] = []
    for _, row in table.iterrows():
        gid = _coerce_gid(_clean_text(row.get(gid_col, "")))
        if not gid:
            continue
        display_name = _clean_text(row.get(name_col, "")) if name_col else ""
        year_text = _clean_text(row.get(year_col, "")) if year_col else display_name
        make_text = _clean_text(row.get(make_col, "")) if make_col else display_name
        liter_text = _clean_text(row.get(liter_col, "")) if liter_col else display_name
        family_text = _clean_text(row.get(family_col, "")) if family_col else display_name
        code_text = _clean_text(row.get(code_col, "")) if code_col else display_name

        field_year_text = ""
        if fields_json_col:
            raw_fields = _clean_text(row.get(fields_json_col, ""))
            if raw_fields:
                try:
                    parsed_fields = json.loads(raw_fields)
                    if isinstance(parsed_fields, dict):
                        field_year_text = _clean_text(parsed_fields.get("year", ""))
                except Exception:
                    field_year_text = ""

        year_value = (
            _parse_year_float(field_year_text)
            or _parse_year_float(year_text)
            or _parse_year_float(display_name)
        )
        year_value = _normalize_year_value(year_value)
        year = int(year_value) if year_value is not None else None
        record = {
            "gid": gid,
            "display_name": display_name,
            "year": year,
            "year_value": year_value,
            "make_key": _normalize_make_key(make_text),
            "family_key": _normalize_family_key(family_text),
            "liter": _normalize_engine_liter(liter_text),
            "engine_code": _extract_engine_code(code_text).upper(),
        }
        records.append(record)
    return tuple(records)


def load_fitment_vehicle_records(required_root: Path | None) -> list[dict]:
    path = find_fitment_vehicle_metaobject_file(required_root=required_root)
    if path is None:
        return []
    try:
        stat = path.stat()
    except Exception:
        return []
    try:
        records = _load_fitment_vehicle_records_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return []
    return [dict(item) for item in records]


def _fitment_engine_map_path(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "FitmentEngineRanges.csv",
        required_root / "mappings" / "fitment_engine_ranges.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16)
def _load_fitment_engine_rules_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict, ...]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    rows: list[dict] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                start_value = _parse_year_float(_clean_text(row.get("year_start", "")))
                end_value = _parse_year_float(_clean_text(row.get("year_end", "")))
                if start_value is None or end_value is None:
                    continue
                rows.append(
                    {
                        "make_key": _normalize_make_key(_clean_text(row.get("make", ""))),
                        "family_key": _normalize_family_key(_clean_text(row.get("family", ""))),
                        "engine_liter": _normalize_engine_liter(_clean_text(row.get("engine_liter", ""))),
                        "engine_code": _extract_engine_code(_clean_text(row.get("source_label", ""))).upper(),
                        "start": min(start_value, end_value),
                        "end": max(start_value, end_value),
                    }
                )
    except Exception:
        return tuple()
    return tuple(rows)


def _load_fitment_engine_rules(required_root: Path | None) -> list[dict]:
    path = _fitment_engine_map_path(required_root)
    if path is None:
        return []
    try:
        stat = path.stat()
    except Exception:
        return []
    try:
        rows = _load_fitment_engine_rules_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return []
    return [dict(item) for item in rows]


def _extract_year_spans(text: str) -> list[tuple[float, float]]:
    source = _clean_text(text)
    if not source:
        return []
    source = source.replace("\u2013", "-").replace("\u2014", "-")
    spans: list[tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for match in re.finditer(r"\b(\d{2,4}(?:\.5)?)\s*[-/]\s*(\d{2,4}(?:\.5)?)\b", source, flags=re.IGNORECASE):
        start = _parse_year_float(match.group(1))
        end = _parse_year_float(match.group(2))
        if start is None or end is None:
            continue
        pair = (min(start, end), max(start, end))
        if pair in seen:
            continue
        seen.add(pair)
        spans.append(pair)
    if spans:
        return spans

    for match in re.finditer(r"\b(19|20)\d{2}\b", source):
        year = float(int(match.group(0)))
        pair = (year, year)
        if pair in seen:
            continue
        seen.add(pair)
        spans.append(pair)
    return spans


def _expand_year_points(spans: list[tuple[float, float]], half_year_candidates: set[float] | None = None) -> list[float]:
    years: list[float] = []
    seen: set[float] = set()
    normalized_half_candidates = {_normalize_year_value(value) for value in (half_year_candidates or set())}
    normalized_half_candidates.discard(None)
    for start, end in spans:
        first = int(start)
        last = int(end)
        if first > last:
            first, last = last, first
        for year in range(first, last + 1):
            full_year = float(year)
            if start <= full_year <= end and full_year not in seen:
                seen.add(full_year)
                years.append(full_year)

            half_year = float(year) + 0.5
            if (
                half_year in normalized_half_candidates
                and start <= half_year <= end
                and half_year not in seen
            ):
                seen.add(half_year)
                years.append(half_year)
    return years


def _detect_primary_make_key(text: str) -> str:
    source = _clean_text(text).lower()
    if not source:
        return ""
    matches: list[tuple[int, str]] = []
    gm_match = re.search(r"\b(gm|gmc|chevy|chevrolet)\b", source)
    if gm_match:
        matches.append((gm_match.start(), "gm"))
    ram_match = re.search(r"\b(ram|dodge)\b", source)
    if ram_match:
        matches.append((ram_match.start(), "ram"))
    ford_match = re.search(r"\bford\b", source)
    if ford_match:
        matches.append((ford_match.start(), "ford"))
    if not matches:
        return ""
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def _target_make_keys(make_key: str) -> list[str]:
    if make_key == "gm":
        return ["gmc", "chevrolet"]
    if make_key == "ram":
        return ["ram"]
    if make_key == "ford":
        return ["ford"]
    return [make_key] if make_key else []


def _select_fitment_engine_rule_for_year(
    year_value: float,
    make_key: str,
    family_hint: str,
    liter_hint: str,
    rules: list[dict],
) -> dict | None:
    best_rule: dict | None = None
    best_rank: tuple[int, int, int, float] | None = None
    for rule in rules:
        start = float(rule.get("start", 0))
        end = float(rule.get("end", 0))
        if float(year_value) < start or float(year_value) > end:
            continue

        rule_make = _clean_text(rule.get("make_key", ""))
        if not rule_make:
            make_rank = 1
        elif rule_make == make_key or (make_key == "gm" and rule_make in {"gm", "gmc", "chevrolet"}):
            make_rank = 2
        else:
            continue

        rule_family = _clean_text(rule.get("family_key", ""))
        family_rank = 1 if family_hint and rule_family == family_hint else 0
        if family_hint and rule_family and rule_family != family_hint:
            family_rank = -1

        rule_liter = _clean_text(rule.get("engine_liter", ""))
        liter_rank = 1 if liter_hint and rule_liter == liter_hint else 0
        if liter_hint and rule_liter and rule_liter != liter_hint:
            liter_rank = -1

        span = max(0.0, float(end) - float(start))
        rank = (make_rank, family_rank, liter_rank, -span)
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_rule = rule
    return dict(best_rule) if best_rule is not None else None


def _record_candidates_for_year_make(records: list[dict], year_value: float, make_key: str) -> list[dict]:
    out: list[dict] = []
    for record in records:
        record_year = record.get("year_value")
        if record_year is None:
            fallback_year = record.get("year")
            if fallback_year is not None:
                record_year = float(fallback_year)
        record_make = _clean_text(record.get("make_key", ""))
        if record_year is None:
            continue
        if abs(float(record_year) - float(year_value)) > 0.001:
            continue
        if record_make != make_key:
            continue
        out.append(record)
    return out


def _pick_fitment_vehicle_gid(
    records: list[dict],
    year_value: float,
    make_key: str,
    family_key: str,
    liter: str,
    engine_code: str,
) -> str:
    candidates = _record_candidates_for_year_make(records=records, year_value=year_value, make_key=make_key)
    if not candidates:
        return ""

    def filter_stage(items: list[dict], require_family: bool, require_liter: bool, require_code: bool) -> list[dict]:
        out: list[dict] = []
        for item in items:
            item_family = _clean_text(item.get("family_key", ""))
            item_liter = _clean_text(item.get("liter", ""))
            item_code = _clean_text(item.get("engine_code", ""))
            if require_family and family_key and item_family != family_key:
                continue
            if require_liter and liter and item_liter != liter:
                continue
            if require_code and engine_code and item_code != engine_code:
                continue
            out.append(item)
        return out

    stages = [
        (True, True, True),
        (True, True, False),
        (True, False, False),
        (False, True, False),
        (False, False, False),
    ]
    for require_family, require_liter, require_code in stages:
        matched = filter_stage(candidates, require_family=require_family, require_liter=require_liter, require_code=require_code)
        if not matched:
            continue
        matched.sort(key=lambda item: _clean_text(item.get("display_name", "")).lower())
        return _clean_text(matched[0].get("gid", ""))
    return ""


def resolve_fitment_vehicle_metaobject_gids(
    application_text: str,
    required_root: Path | None,
    title_text: str = "",
    description_text: str = "",
) -> tuple[list[str], list[str]]:
    records = load_fitment_vehicle_records(required_root=required_root)
    if not records:
        return [], ["Fitment vehicle metaobject mapping file not found or empty."]

    context = " ".join([_clean_text(application_text), _clean_text(title_text), _clean_text(description_text)]).strip()
    spans = _extract_year_spans(_clean_text(application_text) or context)
    if not spans:
        return [], ["Could not detect fitment year range from application text."]

    make_key = _detect_primary_make_key(_clean_text(application_text) or context)
    family_hint = _normalize_family_key(context)
    liter_hint = _normalize_engine_liter(context)

    if not make_key and family_hint == "duramax":
        make_key = "gm"
    elif not make_key and family_hint == "powerstroke":
        make_key = "ford"
    elif not make_key and family_hint == "cummins":
        make_key = "ram"

    if not make_key:
        return [], ["Could not detect vehicle make from application text (expected GM/Ram/Ford)."]

    target_makes = _target_make_keys(make_key)
    if not target_makes:
        return [], [f"Unsupported fitment make '{make_key}' for fitment vehicle resolution."]

    half_year_candidates: set[float] = set()
    if make_key in {"gm", "ram"}:
        for record in records:
            record_make = _clean_text(record.get("make_key", ""))
            if record_make not in target_makes:
                continue
            record_year_value = _normalize_year_value(record.get("year_value"))
            if record_year_value is None:
                fallback_year = record.get("year")
                if fallback_year is not None:
                    record_year_value = _normalize_year_value(float(fallback_year))
            if _is_half_year(record_year_value):
                half_year_candidates.add(float(record_year_value))

    years = _expand_year_points(spans=spans, half_year_candidates=half_year_candidates)
    if not years:
        return [], ["Could not detect fitment year range from application text."]

    engine_rules = _load_fitment_engine_rules(required_root=required_root)
    warnings: list[str] = []
    gids: list[str] = []
    seen_gids: set[str] = set()

    for year_value in years:
        selected_rule = _select_fitment_engine_rule_for_year(
            year_value=year_value,
            make_key=make_key,
            family_hint=family_hint,
            liter_hint=liter_hint,
            rules=engine_rules,
        )
        family = _clean_text((selected_rule or {}).get("family_key", "")) or family_hint
        liter = _clean_text((selected_rule or {}).get("engine_liter", "")) or liter_hint
        engine_code = _clean_text((selected_rule or {}).get("engine_code", ""))
        for target_make in target_makes:
            gid = _pick_fitment_vehicle_gid(
                records=records,
                year_value=year_value,
                make_key=target_make,
                family_key=family,
                liter=liter,
                engine_code=engine_code,
            )
            if not gid:
                detail = f"{_format_year_value(year_value) or str(year_value)} {target_make}"
                if liter:
                    detail += f" {liter}"
                if family:
                    detail += f" {family}"
                warnings.append(f"No fitment vehicle metaobject match for {detail}.")
                continue
            if gid in seen_gids:
                continue
            seen_gids.add(gid)
            gids.append(gid)

    if not gids and not warnings:
        warnings.append("No fitment vehicle metaobjects resolved.")
    return gids, warnings
