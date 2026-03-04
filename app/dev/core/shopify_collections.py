from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

import pandas as pd

from product_prospector.core.config_store import ShopifyConfig
from core.shopify_ymm_tags import (
    _build_fitment_segments as _build_fitment_segments_from_ymm,
    _derive_rule_hints as _derive_rule_hints_from_ymm,
    _extract_literal_liters as _extract_literal_liters_from_ymm,
    _extract_valve_tokens as _extract_valve_tokens_from_ymm,
    _extract_year_spans as _extract_year_spans_from_ymm,
    _load_fitment_engine_rules as _load_fitment_engine_rules_from_ymm,
    _normalize_family_key as _normalize_family_key_from_ymm,
    _normalize_make_key as _normalize_make_key_from_ymm,
    _parse_year_float as _parse_year_float_from_ymm,
)


DEFAULT_COLLECTIONS_FILE = "ShopifyCollections.csv"


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_type_key(value: object) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = re.sub(r"\s*[/|]+\s*", " - ", text)
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_token_list(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    output: list[str] = []
    seen: set[str] = set()
    for part in parts:
        item = _clean_text(part)
        key = item.lower()
        if not item or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _to_year_text(value: float) -> str:
    if abs(value - round(value)) < 0.0001:
        return str(int(round(value)))
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _extract_collection_id(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    direct = re.sub(r"[^0-9]", "", text)
    if direct.isdigit():
        return direct
    match = re.search(r"/Collection/(\d+)", text)
    if match:
        return match.group(1)
    return ""


def _parse_collection_rows(collection_gid: str, title: str, handle: str) -> list[dict]:
    label = _clean_text(title)
    if label.count("|") != 1:
        return []
    type_label, fitment_label = [item.strip() for item in label.split("|", 1)]
    if not type_label or not fitment_label:
        return []
    if not re.match(r"^\d{2,4}(?:\.5)?\s*[-/]\s*\d{2,4}(?:\.5)?\b", fitment_label):
        return []
    type_key = _normalize_type_key(type_label)
    if not type_key:
        return []
    spans = _extract_year_spans_from_ymm(fitment_label)
    if not spans:
        return []

    make_key = _normalize_make_key_from_ymm(fitment_label)
    family_key = _normalize_family_key_from_ymm(fitment_label)
    liters = sorted(_extract_literal_liters_from_ymm(fitment_label))
    valves = sorted(_extract_valve_tokens_from_ymm(fitment_label))
    collection_id = _extract_collection_id(collection_gid)
    liter_text = " | ".join(liters)
    valve_text = " | ".join(valves)

    rows: list[dict] = []
    for start, end in spans:
        rows.append(
            {
                "collection_id": collection_id,
                "collection_gid": _clean_text(collection_gid),
                "collection_title": label,
                "collection_handle": _clean_text(handle),
                "type_label": type_label,
                "type_key": type_key,
                "fitment_label": fitment_label,
                "year_start": _to_year_text(min(float(start), float(end))),
                "year_end": _to_year_text(max(float(start), float(end))),
                "make_key": make_key,
                "family_key": family_key,
                "liter": liter_text,
                "valve": valve_text,
            }
        )
    return rows


def _request_graphql_query(
    config: ShopifyConfig,
    access_token: str,
    query: str,
    variables: dict,
) -> tuple[dict | None, str | None]:
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


_COLLECTIONS_QUERY = """
query CollectionTitles($cursor: String) {
  collections(first: 250, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        title
        handle
      }
    }
  }
}
"""


def fetch_shopify_collection_mapping_table(
    config: ShopifyConfig,
    access_token: str,
    max_pages: int = 120,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None]:
    rows: list[dict] = []
    cursor: str | None = None
    page_count = 0
    total_collections = 0

    while page_count < max(1, int(max_pages)):
        page_count += 1
        data, error = _request_graphql_query(
            config=config,
            access_token=access_token,
            query=_COLLECTIONS_QUERY,
            variables={"cursor": cursor},
        )
        if error:
            return pd.DataFrame(), error

        collections_block = (data or {}).get("collections") or {}
        edges = collections_block.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            title = _clean_text(node.get("title", ""))
            if not title:
                continue
            total_collections += 1
            parsed_rows = _parse_collection_rows(
                collection_gid=_clean_text(node.get("id", "")),
                title=title,
                handle=_clean_text(node.get("handle", "")),
            )
            if parsed_rows:
                rows.extend(parsed_rows)

        if progress_callback is not None:
            try:
                progress_callback(page_count, total_collections, len(rows))
            except Exception:
                pass

        page_info = collections_block.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _clean_text(page_info.get("endCursor", ""))
        if not cursor:
            break

    columns = [
        "collection_id",
        "collection_gid",
        "collection_title",
        "collection_handle",
        "type_label",
        "type_key",
        "fitment_label",
        "year_start",
        "year_end",
        "make_key",
        "family_key",
        "liter",
        "valve",
    ]
    if not rows:
        return pd.DataFrame(columns=columns), None

    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    df = df[columns].copy()
    df["collection_title"] = df["collection_title"].astype(str).str.strip()
    df = df[df["collection_title"] != ""].copy()
    df = df.drop_duplicates(
        subset=[
            "collection_id",
            "collection_title",
            "type_key",
            "year_start",
            "year_end",
            "make_key",
            "family_key",
            "liter",
            "valve",
        ],
        keep="first",
    )
    df = df.sort_values(
        by=["type_key", "year_start", "make_key", "family_key", "liter", "collection_title"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)
    return df, None


def default_collection_mapping_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_COLLECTIONS_FILE


def save_collection_mapping_table(df: pd.DataFrame, output_path: Path) -> tuple[int, str | None]:
    columns = [
        "collection_id",
        "collection_gid",
        "collection_title",
        "collection_handle",
        "type_label",
        "type_key",
        "fitment_label",
        "year_start",
        "year_end",
        "make_key",
        "family_key",
        "liter",
        "valve",
    ]
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out = df.copy() if df is not None else pd.DataFrame(columns=columns)
        for column in columns:
            if column not in out.columns:
                out[column] = ""
        out = out[columns]
        if output_path.suffix.lower() == ".xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                out.to_excel(writer, index=False, sheet_name="collections")
        else:
            out.to_csv(output_path, index=False, encoding="utf-8")
        return int(len(out.index)), None
    except Exception as exc:
        return 0, str(exc)


def find_collection_mapping_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "ShopifyCollections.csv",
        required_root / "mappings" / "shopify_collections.csv",
        required_root / "mappings" / "Collections.csv",
        required_root / "mappings" / "collections.csv",
        required_root / "mappings" / "ShopifyCollections.xlsx",
        required_root / "mappings" / "shopify_collections.xlsx",
        required_root / "mappings" / "Collections.xlsx",
        required_root / "mappings" / "collections.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16)
def _load_collection_records_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict, ...]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    try:
        if path.suffix.lower() == ".csv":
            table = pd.read_csv(path, dtype=str, keep_default_na=False)
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            table = pd.read_excel(path, dtype=str, keep_default_na=False)
        else:
            return tuple()
    except Exception:
        return tuple()
    if table.empty:
        return tuple()

    normalized_columns = {str(col).strip().lower().replace(" ", "_"): str(col) for col in table.columns}

    def pick(*candidates: str) -> str:
        for candidate in candidates:
            if candidate in normalized_columns:
                return normalized_columns[candidate]
        return ""

    collection_id_col = pick("collection_id", "id")
    collection_gid_col = pick("collection_gid", "gid")
    collection_title_col = pick("collection_title", "title", "name")
    collection_handle_col = pick("collection_handle", "handle")
    type_label_col = pick("type_label", "type")
    type_key_col = pick("type_key")
    fitment_label_col = pick("fitment_label", "fitment")
    year_start_col = pick("year_start")
    year_end_col = pick("year_end")
    make_key_col = pick("make_key")
    family_key_col = pick("family_key")
    liter_col = pick("liter", "liters")
    valve_col = pick("valve", "valves")

    if not collection_title_col or not year_start_col or not year_end_col:
        return tuple()

    records: list[dict] = []
    for _, row in table.iterrows():
        collection_gid = _clean_text(row.get(collection_gid_col, "")) if collection_gid_col else ""
        collection_id = _extract_collection_id(_clean_text(row.get(collection_id_col, ""))) if collection_id_col else ""
        if not collection_id and collection_gid:
            collection_id = _extract_collection_id(collection_gid)
        title = _clean_text(row.get(collection_title_col, ""))
        if not title:
            continue
        year_start = _parse_year_float_from_ymm(_clean_text(row.get(year_start_col, "")))
        year_end = _parse_year_float_from_ymm(_clean_text(row.get(year_end_col, "")))
        if year_start is None or year_end is None:
            continue
        start = min(float(year_start), float(year_end))
        end = max(float(year_start), float(year_end))
        type_label = _clean_text(row.get(type_label_col, "")) if type_label_col else ""
        type_key_value = _clean_text(row.get(type_key_col, "")) if type_key_col else ""
        fitment_label = _clean_text(row.get(fitment_label_col, "")) if fitment_label_col else ""
        make_key = _normalize_make_key_from_ymm(_clean_text(row.get(make_key_col, ""))) if make_key_col else ""
        family_key = _normalize_family_key_from_ymm(_clean_text(row.get(family_key_col, ""))) if family_key_col else ""
        liters = _normalize_token_list(row.get(liter_col, "")) if liter_col else []
        valves = _normalize_token_list(row.get(valve_col, "")) if valve_col else []
        records.append(
            {
                "collection_id": collection_id,
                "collection_gid": collection_gid,
                "collection_title": title,
                "collection_handle": _clean_text(row.get(collection_handle_col, "")) if collection_handle_col else "",
                "type_label": type_label,
                "type_key": _normalize_type_key(type_key_value or type_label),
                "fitment_label": fitment_label,
                "year_start": start,
                "year_end": end,
                "make_key": make_key,
                "family_key": family_key,
                "liter_set": {item for item in liters if _clean_text(item)},
                "valve_set": {item.lower() for item in valves if _clean_text(item)},
            }
        )

    deduped: list[dict] = []
    seen: set[tuple[str, str, float, float, str, str, str]] = set()
    for record in records:
        key = (
            _clean_text(record.get("collection_id", "")),
            _clean_text(record.get("collection_title", "")),
            float(record.get("year_start", 0.0)),
            float(record.get("year_end", 0.0)),
            _clean_text(record.get("type_key", "")),
            _clean_text(record.get("make_key", "")),
            _clean_text(record.get("family_key", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)

    deduped.sort(
        key=lambda item: (
            _clean_text(item.get("type_key", "")),
            float(item.get("year_start", 9999.0)),
            _clean_text(item.get("make_key", "")),
            _clean_text(item.get("collection_title", "")),
        )
    )
    return tuple(deduped)


def load_collection_records(required_root: Path | None) -> list[dict]:
    path = find_collection_mapping_file(required_root=required_root)
    if path is None:
        return []
    try:
        stat = path.stat()
    except Exception:
        return []
    try:
        records = _load_collection_records_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return []
    return [dict(item) for item in records]


def _year_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return not (a_end < b_start or b_end < a_start)


def _record_matches_segment(
    record: dict,
    segment: dict,
    explicit_liters: set[str],
    explicit_valves: set[str],
) -> bool:
    start = float(record.get("year_start", 0.0))
    end = float(record.get("year_end", 0.0))
    spans = segment.get("spans") or []
    if not spans:
        return False
    if not any(_year_overlap(start, end, float(seg_start), float(seg_end)) for seg_start, seg_end in spans):
        return False

    target_makes = segment.get("target_makes") or []
    record_make = _clean_text(record.get("make_key", ""))
    if target_makes and record_make and record_make not in target_makes:
        return False

    record_family = _clean_text(record.get("family_key", ""))
    segment_family = _clean_text(segment.get("family_key", ""))
    derived_families: set[str] = segment.get("derived_families", set())
    if segment_family and record_family and record_family != segment_family:
        return False
    if not segment_family and derived_families and record_family and record_family not in derived_families:
        return False

    record_liters: set[str] = {str(item) for item in (record.get("liter_set") or set()) if _clean_text(item)}
    if "3.0L" in record_liters and "3.0L" not in explicit_liters:
        return False
    segment_liters: set[str] = {str(item) for item in (segment.get("liters") or set()) if _clean_text(item)}
    derived_liters: set[str] = {str(item) for item in (segment.get("derived_liters") or set()) if _clean_text(item)}
    allowed_liters = segment_liters or derived_liters
    if allowed_liters and record_liters and not (record_liters & allowed_liters):
        return False

    record_valves: set[str] = {str(item).lower() for item in (record.get("valve_set") or set()) if _clean_text(item)}
    if record_valves and not (record_valves & explicit_valves):
        return False

    return True


def resolve_collection_assignments(
    product_type: str,
    application_text: str,
    required_root: Path | None,
    title_text: str = "",
    description_text: str = "",
) -> tuple[list[dict], list[str]]:
    records = load_collection_records(required_root=required_root)
    if not records:
        return [], ["Collection mapping file not found or empty."]

    type_key = _normalize_type_key(product_type)
    if not type_key:
        return [], ["Product type is blank; could not resolve collections."]
    type_records = [record for record in records if _clean_text(record.get("type_key", "")) == type_key]
    if not type_records:
        return [], [f"No collection mappings found for type '{_clean_text(product_type)}'."]

    segments = _build_fitment_segments_from_ymm(
        application_text=application_text,
        title_text=title_text,
        description_text=description_text,
    )
    if not segments:
        return [], ["Could not detect fitment year range from application text."]

    context = " ".join([_clean_text(application_text), _clean_text(title_text), _clean_text(description_text)]).strip()
    explicit_liters = _extract_literal_liters_from_ymm(context)
    explicit_valves = {item.lower() for item in _extract_valve_tokens_from_ymm(context)}
    engine_rules = _load_fitment_engine_rules_from_ymm(required_root=required_root)

    for segment in segments:
        rule_families, rule_liters = _derive_rule_hints_from_ymm(
            spans=segment.get("spans", []),
            make_key=_clean_text(segment.get("make_key", "")),
            rules=engine_rules,
        )
        if "3.0L" not in explicit_liters and "3.0L" in rule_liters:
            rule_liters.discard("3.0L")
        segment["derived_families"] = rule_families
        segment["derived_liters"] = rule_liters

    targets: list[dict] = []
    seen_ids: set[str] = set()
    seen_titles: set[str] = set()
    for record in type_records:
        matched = any(
            _record_matches_segment(
                record=record,
                segment=segment,
                explicit_liters=explicit_liters,
                explicit_valves=explicit_valves,
            )
            for segment in segments
        )
        if not matched:
            continue
        collection_id = _clean_text(record.get("collection_id", ""))
        title = _clean_text(record.get("collection_title", ""))
        if not collection_id or not title:
            continue
        title_key = re.sub(r"\s+", " ", title).strip().lower()
        if title_key in seen_titles:
            continue
        if collection_id in seen_ids:
            continue
        seen_titles.add(title_key)
        seen_ids.add(collection_id)
        targets.append(
            {
                "collection_id": collection_id,
                "collection_title": title,
                "collection_gid": _clean_text(record.get("collection_gid", "")),
                "collection_handle": _clean_text(record.get("collection_handle", "")),
            }
        )

    targets.sort(key=lambda item: _clean_text(item.get("collection_title", "")))
    if targets:
        return targets, []
    return [], [f"No existing collections matched type '{_clean_text(product_type)}' and fitment years."]


def resolve_collection_assignments_from_titles(
    collections_text: str,
    required_root: Path | None,
) -> tuple[list[dict], list[str]]:
    records = load_collection_records(required_root=required_root)
    if not records:
        return [], ["Collection mapping file not found or empty."]

    requested_titles: list[str] = []
    seen_requested: set[str] = set()
    for raw in re.split(r"[,\n]+", _clean_text(collections_text)):
        title = _clean_text(raw)
        if not title:
            continue
        key = re.sub(r"\s+", " ", title).strip().lower()
        if key in seen_requested:
            continue
        seen_requested.add(key)
        requested_titles.append(title)

    if not requested_titles:
        return [], ["Collections input is blank."]

    by_title_key: dict[str, list[dict]] = {}
    for record in records:
        title = _clean_text(record.get("collection_title", ""))
        if not title:
            continue
        key = re.sub(r"\s+", " ", title).strip().lower()
        by_title_key.setdefault(key, []).append(record)

    output: list[dict] = []
    warnings: list[str] = []
    seen_ids: set[str] = set()
    seen_titles: set[str] = set()
    for requested in requested_titles:
        requested_key = re.sub(r"\s+", " ", requested).strip().lower()
        matches = by_title_key.get(requested_key, [])
        if not matches:
            warnings.append(f"Collection not found in local mapping: '{requested}'.")
            continue
        for match in matches:
            collection_id = _clean_text(match.get("collection_id", ""))
            collection_title = _clean_text(match.get("collection_title", ""))
            if not collection_id or not collection_title:
                continue
            title_key = re.sub(r"\s+", " ", collection_title).strip().lower()
            if title_key in seen_titles:
                continue
            if collection_id in seen_ids:
                continue
            seen_titles.add(title_key)
            seen_ids.add(collection_id)
            output.append(
                {
                    "collection_id": collection_id,
                    "collection_title": collection_title,
                    "collection_gid": _clean_text(match.get("collection_gid", "")),
                    "collection_handle": _clean_text(match.get("collection_handle", "")),
                }
            )

    output.sort(key=lambda item: _clean_text(item.get("collection_title", "")))
    if output:
        return output, warnings
    if warnings:
        return [], warnings
    return [], ["No valid collections resolved from input."]
