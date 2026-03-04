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


DEFAULT_YMM_TAG_FILE = "ShopifyYMMTags.csv"


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9.\s-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_ymm_tag(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if not re.match(r"^\s*ymm\s*:", text, flags=re.IGNORECASE):
        return ""
    suffix = re.sub(r"^\s*ymm\s*:\s*", "", text, flags=re.IGNORECASE).strip()
    if not suffix:
        return ""
    return f"YMM: {suffix}"


def _canonical_tag_key(value: str) -> str:
    tag = _normalize_ymm_tag(value)
    if not tag:
        return ""
    key = tag.lower()
    key = re.sub(r"\s*-\s*", "-", key)
    key = re.sub(r"\s+", " ", key).strip()
    return key


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
    if re.search(r"\bjeep\b", text):
        return "jeep"
    if re.search(r"\bnissan\b", text):
        return "nissan"
    return ""


def _normalize_family_key(value: str) -> str:
    text = _normalize_key(value)
    if not text:
        return ""
    if re.search(r"\bdura\s*max\b|\bduramax\b", text):
        return "duramax"
    if re.search(r"\bpower\s*stroke\b|\bpowerstroke\b", text):
        return "powerstroke"
    if re.search(r"\bcummins\b", text):
        return "cummins"
    if re.search(r"\beco\s*diesel\b|\becodiesel\b", text):
        return "ecodiesel"
    if re.search(r"\bidi\b", text):
        return "idi"
    return ""


def _normalize_engine_liter(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    match = re.search(r"\b(\d\.\d)\s*l\b", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}L"


def _extract_literal_liters(value: str) -> set[str]:
    text = _clean_text(value)
    if not text:
        return set()
    found: set[str] = set()
    for match in re.finditer(r"\b(\d\.\d)\s*l\b", text, flags=re.IGNORECASE):
        found.add(f"{match.group(1)}L")
    return found


def _extract_valve_tokens(value: str) -> set[str]:
    text = _clean_text(value)
    if not text:
        return set()
    found: set[str] = set()
    for match in re.finditer(r"\b(\d{1,2})\s*[- ]?\s*valve\b", text, flags=re.IGNORECASE):
        try:
            number = int(match.group(1))
        except Exception:
            continue
        found.add(f"{number}-valve")
    return found


def _detect_valve_token(value: str) -> str:
    tokens = sorted(_extract_valve_tokens(value))
    return tokens[0] if tokens else ""


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


def _extract_year_spans(value: str) -> list[tuple[float, float]]:
    text = _clean_text(value)
    if not text:
        return []
    source = text.replace("\u2013", "-").replace("\u2014", "-")
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


def _year_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return not (a_end < b_start or b_end < a_start)


def _detect_primary_make_key(value: str) -> str:
    source = _clean_text(value).lower()
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
    jeep_match = re.search(r"\bjeep\b", source)
    if jeep_match:
        matches.append((jeep_match.start(), "jeep"))
    nissan_match = re.search(r"\bnissan\b", source)
    if nissan_match:
        matches.append((nissan_match.start(), "nissan"))
    if not matches:
        return ""
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def _target_make_keys(make_key: str) -> list[str]:
    if make_key == "gm":
        return ["gmc", "chevrolet", "gm"]
    if make_key == "ram":
        return ["ram"]
    if make_key == "ford":
        return ["ford"]
    if make_key == "jeep":
        return ["jeep"]
    if make_key == "nissan":
        return ["nissan"]
    return [make_key] if make_key else []


def _infer_make_from_family(family_key: str, context_text: str) -> str:
    if family_key == "duramax":
        return "gm"
    if family_key == "powerstroke":
        return "ford"
    if family_key == "cummins":
        return "ram"
    if family_key == "ecodiesel":
        context_make = _detect_primary_make_key(context_text)
        return context_make or "ram"
    return ""


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


def _derive_rule_hints(
    spans: list[tuple[float, float]],
    make_key: str,
    rules: list[dict],
) -> tuple[set[str], set[str]]:
    if not spans or not make_key or not rules:
        return set(), set()
    years: set[int] = set()
    for start, end in spans:
        first = int(start)
        last = int(end)
        if first > last:
            first, last = last, first
        for year in range(first, last + 1):
            years.add(year)
    families: set[str] = set()
    liters: set[str] = set()
    for year in sorted(years):
        for rule in rules:
            start = float(rule.get("start", 0.0))
            end = float(rule.get("end", 0.0))
            if float(year) < start or float(year) > end:
                continue
            rule_make = _clean_text(rule.get("make_key", ""))
            if rule_make:
                if make_key == "gm":
                    if rule_make not in {"gm", "gmc", "chevrolet"}:
                        continue
                elif rule_make != make_key:
                    continue
            family = _clean_text(rule.get("family_key", ""))
            liter = _clean_text(rule.get("engine_liter", ""))
            if family:
                families.add(family)
            if liter:
                liters.add(liter)
    return families, liters


def _parse_ymm_tag_row(tag_value: str) -> dict:
    tag_text = _normalize_ymm_tag(tag_value)
    if not tag_text:
        return {}
    source = re.sub(r"^\s*YMM\s*:\s*", "", tag_text, flags=re.IGNORECASE).strip()
    year_spans = _extract_year_spans(source)
    year_start = year_spans[0][0] if year_spans else None
    year_end = year_spans[0][1] if year_spans else None
    return {
        "tag": tag_text,
        "year_start": year_start,
        "year_end": year_end,
        "make_key": _normalize_make_key(source),
        "family_key": _normalize_family_key(source),
        "liter": _normalize_engine_liter(source),
        "valve": _detect_valve_token(source),
    }


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


_PRODUCT_TAGS_QUERY = """
query ProductTags($cursor: String) {
  products(first: 100, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        tags
      }
    }
  }
}
"""


def fetch_ymm_tags_table(
    config: ShopifyConfig,
    access_token: str,
    max_pages: int = 250,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None]:
    tags: list[str] = []
    seen: set[str] = set()
    cursor: str | None = None
    page_count = 0

    while page_count < max_pages:
        page_count += 1
        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_PRODUCT_TAGS_QUERY,
            variables={"cursor": cursor},
        )
        if error:
            return pd.DataFrame(), error

        products = (data or {}).get("products") or {}
        edges = products.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            for raw_tag in (node.get("tags") or []):
                tag = _normalize_ymm_tag(raw_tag)
                if not tag:
                    continue
                key = _canonical_tag_key(tag)
                if key in seen:
                    continue
                seen.add(key)
                tags.append(tag)

        if progress_callback is not None:
            try:
                progress_callback(page_count, len(tags))
            except Exception:
                pass

        page_info = products.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _clean_text(page_info.get("endCursor", ""))
        if not cursor:
            break

    columns = ["tag", "year_start", "year_end", "make_key", "family_key", "liter", "valve"]
    if not tags:
        return pd.DataFrame(columns=columns), None

    rows: list[dict] = []
    for tag in tags:
        parsed = _parse_ymm_tag_row(tag)
        if not parsed:
            continue
        rows.append(parsed)
    if not rows:
        return pd.DataFrame(columns=columns), None

    output = pd.DataFrame(rows)
    for col in columns:
        if col not in output.columns:
            output[col] = ""
    output = output[columns].copy()
    output["tag"] = output["tag"].astype(str).str.strip()
    output = output[output["tag"] != ""].copy()
    output = output.drop_duplicates(subset=["tag"], keep="first")
    output = output.sort_values(by=["year_start", "make_key", "liter", "tag"], kind="stable", na_position="last").reset_index(
        drop=True
    )
    return output, None


def default_ymm_tags_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_YMM_TAG_FILE


def save_ymm_tags_table(df: pd.DataFrame, output_path: Path) -> tuple[int, str | None]:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        columns = ["tag", "year_start", "year_end", "make_key", "family_key", "liter", "valve"]
        out = df.copy() if df is not None else pd.DataFrame(columns=columns)
        for col in columns:
            if col not in out.columns:
                out[col] = ""
        out = out[columns]
        suffix = output_path.suffix.lower()
        if suffix == ".xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                out.to_excel(writer, index=False, sheet_name="ymm_tags")
        else:
            out.to_csv(output_path, index=False, encoding="utf-8")
        return len(out.index), None
    except Exception as exc:
        return 0, str(exc)


def find_ymm_tag_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "ShopifyYMMTags.csv",
        required_root / "mappings" / "shopify_ymm_tags.csv",
        required_root / "mappings" / "YMMTags.csv",
        required_root / "mappings" / "ymm_tags.csv",
        required_root / "mappings" / "ShopifyYMMTags.xlsx",
        required_root / "mappings" / "shopify_ymm_tags.xlsx",
        required_root / "mappings" / "YMMTags.xlsx",
        required_root / "mappings" / "ymm_tags.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16)
def _load_ymm_records_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict, ...]:
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
    tag_col = ""
    for candidate in ("tag", "ymm_tag", "name", "title"):
        if candidate in normalized_columns:
            tag_col = normalized_columns[candidate]
            break
    if not tag_col:
        return tuple()

    year_start_col = normalized_columns.get("year_start", "")
    year_end_col = normalized_columns.get("year_end", "")
    make_col = normalized_columns.get("make_key", "")
    family_col = normalized_columns.get("family_key", "")
    liter_col = normalized_columns.get("liter", "")
    valve_col = normalized_columns.get("valve", "")

    records: list[dict] = []
    for _, row in table.iterrows():
        tag_value = _normalize_ymm_tag(_clean_text(row.get(tag_col, "")))
        if not tag_value:
            continue
        parsed = _parse_ymm_tag_row(tag_value)
        if not parsed:
            continue
        if year_start_col:
            parsed_start = _parse_year_float(_clean_text(row.get(year_start_col, "")))
            if parsed_start is not None:
                parsed["year_start"] = parsed_start
        if year_end_col:
            parsed_end = _parse_year_float(_clean_text(row.get(year_end_col, "")))
            if parsed_end is not None:
                parsed["year_end"] = parsed_end
        if make_col:
            make_key = _normalize_make_key(_clean_text(row.get(make_col, "")))
            if make_key:
                parsed["make_key"] = make_key
        if family_col:
            family_key = _normalize_family_key(_clean_text(row.get(family_col, "")))
            if family_key:
                parsed["family_key"] = family_key
        if liter_col:
            liter = _normalize_engine_liter(_clean_text(row.get(liter_col, "")))
            if liter:
                parsed["liter"] = liter
        if valve_col:
            valve = _detect_valve_token(_clean_text(row.get(valve_col, "")))
            if valve:
                parsed["valve"] = valve
        records.append(parsed)

    deduped: list[dict] = []
    seen: set[str] = set()
    for row in records:
        key = _canonical_tag_key(_clean_text(row.get("tag", "")))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(
        key=lambda item: (
            float(item.get("year_start", 9999.0) or 9999.0),
            _clean_text(item.get("make_key", "")),
            _clean_text(item.get("liter", "")),
            _clean_text(item.get("tag", "")),
        )
    )
    return tuple(deduped)


def load_ymm_tag_records(required_root: Path | None) -> list[dict]:
    path = find_ymm_tag_file(required_root=required_root)
    if path is None:
        return []
    try:
        stat = path.stat()
    except Exception:
        return []
    try:
        records = _load_ymm_records_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return []
    return [dict(item) for item in records]


def _build_fitment_segments(application_text: str, title_text: str, description_text: str) -> list[dict]:
    context = " ".join([_clean_text(application_text), _clean_text(title_text), _clean_text(description_text)]).strip()
    source = _clean_text(application_text) or context
    if not source:
        return []
    chunks = [item.strip() for item in re.split(r"\s*\|\s*", source) if _clean_text(item)]
    if not chunks:
        chunks = [source]

    segments: list[dict] = []
    for chunk in chunks:
        spans = _extract_year_spans(chunk)
        if not spans:
            continue
        make_key = _detect_primary_make_key(chunk) or _detect_primary_make_key(context)
        family_key = _normalize_family_key(chunk) or _normalize_family_key(context)
        if not make_key and family_key:
            make_key = _infer_make_from_family(family_key, context)
        segments.append(
            {
                "spans": spans,
                "make_key": make_key,
                "target_makes": _target_make_keys(make_key),
                "family_key": family_key,
                "liters": _extract_literal_liters(chunk),
                "chunk_text": chunk,
            }
        )
    return segments


def _record_matches_segment(
    record: dict,
    segment: dict,
    explicit_liters: set[str],
    explicit_valves: set[str],
) -> bool:
    tag_text = _clean_text(record.get("tag", ""))
    if not tag_text:
        return False

    record_start = record.get("year_start")
    record_end = record.get("year_end")
    if record_start is None or record_end is None:
        return False
    try:
        start = float(record_start)
        end = float(record_end)
    except Exception:
        return False

    spans = segment.get("spans") or []
    if not spans:
        return False
    overlap = False
    for seg_start, seg_end in spans:
        if _year_overlap(start, end, seg_start, seg_end):
            overlap = True
            break
    if not overlap:
        return False

    target_makes = segment.get("target_makes") or []
    record_make = _clean_text(record.get("make_key", ""))
    if target_makes and record_make:
        if record_make not in target_makes:
            return False

    segment_family = _clean_text(segment.get("family_key", ""))
    record_family = _clean_text(record.get("family_key", ""))
    derived_families: set[str] = segment.get("derived_families", set())
    if segment_family and record_family and record_family != segment_family:
        return False
    if not segment_family and derived_families and record_family and record_family not in derived_families:
        return False

    record_liter = _clean_text(record.get("liter", ""))
    if record_liter == "3.0L" and "3.0L" not in explicit_liters:
        return False
    segment_liters: set[str] = segment.get("liters", set())
    derived_liters: set[str] = segment.get("derived_liters", set())
    allowed_liters = segment_liters or derived_liters
    if allowed_liters and record_liter and record_liter not in allowed_liters:
        return False

    record_valve = _clean_text(record.get("valve", ""))
    if record_valve and record_valve not in explicit_valves:
        return False

    return True


def resolve_ymm_tags(
    application_text: str,
    required_root: Path | None,
    title_text: str = "",
    description_text: str = "",
) -> tuple[list[str], list[str]]:
    records = load_ymm_tag_records(required_root=required_root)
    if not records:
        return [], ["YMM tag mapping file not found or empty."]

    context = " ".join([_clean_text(application_text), _clean_text(title_text), _clean_text(description_text)]).strip()
    segments = _build_fitment_segments(application_text=application_text, title_text=title_text, description_text=description_text)
    if not segments:
        return [], ["Could not detect fitment year range from application text."]

    explicit_liters = _extract_literal_liters(context)
    explicit_valves = _extract_valve_tokens(context)
    engine_rules = _load_fitment_engine_rules(required_root=required_root)

    for segment in segments:
        rule_families, rule_liters = _derive_rule_hints(
            spans=segment.get("spans", []),
            make_key=_clean_text(segment.get("make_key", "")),
            rules=engine_rules,
        )
        if "3.0L" not in explicit_liters and "3.0L" in rule_liters:
            rule_liters.discard("3.0L")
        segment["derived_families"] = rule_families
        segment["derived_liters"] = rule_liters

    tags: list[str] = []
    seen: set[str] = set()
    for record in records:
        matched = False
        for segment in segments:
            if _record_matches_segment(
                record=record,
                segment=segment,
                explicit_liters=explicit_liters,
                explicit_valves=explicit_valves,
            ):
                matched = True
                break
        if not matched:
            continue
        tag_text = _clean_text(record.get("tag", ""))
        if not tag_text:
            continue
        key = _canonical_tag_key(tag_text)
        if key in seen:
            continue
        seen.add(key)
        tags.append(tag_text)
    return tags, []
