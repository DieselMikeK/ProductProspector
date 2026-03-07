from __future__ import annotations

import csv
import re
from functools import lru_cache
from pathlib import Path

from product_prospector.core.pricing_rules import (
    calculate_cost_from_price,
    choose_best_discount,
    find_vendor_discount_file,
    load_vendor_discounts,
)
from product_prospector.core.core_charge_codes import normalize_core_charge_product_code
from product_prospector.core.processing import normalize_sku
from product_prospector.core.product_model import Product
from product_prospector.core.shopify_collections import resolve_collection_assignments
from product_prospector.core.vendor_profiles import resolve_vendor_profile
from product_prospector.core.vendor_normalization import (
    normalize_vendor_name as normalize_vendor_from_rules,
    resolve_vendor_title_name,
)


_HTML_STRIP_RE = re.compile(r"<[^>]+>")


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _normalize_description_html(text: str) -> str:
    value = _clean_text(text)
    if not value:
        return ""
    # Remove potentially unsafe script/style blocks before preserving simple HTML text.
    value = re.sub(r"<script[\s\S]*?</script>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"<style[\s\S]*?</style>", "", value, flags=re.IGNORECASE)
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    return value.strip()


def _normalize_vendor_name(vendor: str, required_root: Path | None = None) -> str:
    value = _clean_text(vendor)
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    normalized = normalize_vendor_from_rules(value.strip(), required_root=required_root)
    return _clean_text(normalized) or value.strip()


def _normalize_prefix_token(value: str) -> str:
    token = _clean_text(value).upper()
    token = re.sub(r"[^A-Z0-9]+", "", token)
    return token


def _apply_vendor_sku_prefix(raw_sku: str, sku_prefix: str) -> str:
    sku = normalize_sku(raw_sku)
    prefix = _normalize_prefix_token(sku_prefix)
    if not sku or not prefix:
        return sku
    if sku.startswith(f"{prefix}-") or sku == prefix or sku.startswith(prefix):
        return sku
    if "-" in sku:
        # Allow prefixing numeric-first SKUs like "57-923545" (Westin pattern),
        # but avoid overriding already-prefixed alpha-leading SKUs (e.g. PPE-..., AFE-...).
        first_token = sku.split("-", 1)[0]
        if re.search(r"[A-Z]", first_token):
            return sku
    return f"{prefix}-{sku}"


def _sanitize_text_for_parse(value: str) -> str:
    text = _clean_text(_HTML_STRIP_RE.sub(" ", value))
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _short_year_token(year_text: str) -> str:
    text = _clean_text(year_text)
    if not text:
        return ""
    half = ".5" if ".5" in text else ""
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if len(digits) >= 4:
        value = digits[-2:]
    elif len(digits) == 2:
        yy = int(digits)
        if yy >= 80 or yy <= 30:
            value = digits
        else:
            return ""
    else:
        return ""
    return f"{value}{half}"


def _expand_year_token_to_full(year_text: str) -> str:
    text = _clean_text(year_text)
    if not text:
        return ""
    half = ".5" if ".5" in text else ""
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if not digits:
        return ""
    if len(digits) >= 4:
        return f"{digits[:4]}{half}"
    if len(digits) == 2:
        yy = int(digits)
        if not (yy >= 80 or yy <= 30):
            return ""
        prefix = "19" if yy >= 80 else "20"
        return f"{prefix}{digits}{half}"
    return ""


def _extract_year_ranges(text: str) -> list[str]:
    source = _sanitize_text_for_parse(text)
    if not source:
        return []
    pattern = re.compile(
        r"\b(\d{2,4}(?:\.5)?)\s*[-/]\s*(\d{2,4}(?:\.5)?)\b",
        flags=re.IGNORECASE,
    )
    output: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(source):
        start = _short_year_token(match.group(1))
        end = _short_year_token(match.group(2))
        if not start or not end:
            continue
        token = f"{start}-{end}"
        if token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def _expand_short_year_ranges(text: str) -> str:
    source = _sanitize_text_for_parse(text)
    if not source:
        return ""
    pattern = re.compile(r"\b(\d{2,4}(?:\.5)?)\s*-\s*(\d{2,4}(?:\.5)?)\b", flags=re.IGNORECASE)

    def repl(match: re.Match) -> str:
        start = _expand_year_token_to_full(match.group(1))
        end = _expand_year_token_to_full(match.group(2))
        if not start or not end:
            return match.group(0)
        return f"{start}-{end}"

    return pattern.sub(repl, source)


def _derive_application_from_title(title_text: str) -> str:
    source = _sanitize_text_for_parse(title_text)
    if not source:
        return ""
    pattern = re.compile(r"\b\d{2,4}(?:\.5)?\s*-\s*\d{2,4}(?:\.5)?\b", flags=re.IGNORECASE)
    match = pattern.search(source)
    if not match:
        return ""
    fitment = source[match.start() :].strip(" -")
    fitment = _expand_short_year_ranges(fitment)
    fitment = re.sub(r"\s+", " ", fitment).strip()
    return fitment


def _looks_like_fitment_text(value: str) -> bool:
    source = _sanitize_text_for_parse(value)
    if not source:
        return False
    has_year = bool(re.search(r"\b\d{2,4}(?:\.5)?\s*[-/]\s*\d{2,4}(?:\.5)?\b|\b(19|20)\d{2}\b", source))
    has_vehicle = bool(
        re.search(
            r"(?i)\b(ford|ram|dodge|gmc|chevy|chevrolet|gm|jeep|nissan|toyota|cummins|duramax|powerstroke)\b",
            source,
        )
    )
    return has_year and has_vehicle


def _detect_make_label(text: str) -> str:
    low = _sanitize_text_for_parse(text).lower()
    if re.search(r"\b(chevy|chevrolet|gmc|gm)\b", low):
        return "GM"
    if re.search(r"\b(ram|dodge)\b", low):
        return "Ram"
    if re.search(r"\b(ford)\b", low):
        return "Ford"
    if re.search(r"\b(jeep)\b", low):
        return "Jeep"
    if re.search(r"\b(nissan)\b", low):
        return "Nissan"
    return ""


def _detect_engine_liter(text: str) -> str:
    source = _sanitize_text_for_parse(text)
    match = re.search(r"\b(\d\.\d)\s*l\b", source, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\b(\d\.\d)\s*(?=(?:power\s*stroke|powerstroke|duramax|cummins|ecodiesel)\b)", source, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\b(?:power\s*stroke|powerstroke|duramax|cummins|ecodiesel)\s*(\d\.\d)\b", source, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}L"


def _year_token_to_value(token: str) -> float | None:
    text = _clean_text(token)
    if not text:
        return None
    has_half = ".5" in text
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if len(digits) >= 4:
        base_year = int(digits[:4])
    elif len(digits) == 2:
        yy = int(digits)
        if yy >= 80:
            base_year = 1900 + yy
        elif yy <= 30:
            base_year = 2000 + yy
        else:
            return None
    else:
        return None
    return float(base_year) + (0.5 if has_half else 0.0)


def _first_year_window(years: list[str]) -> tuple[float | None, float | None]:
    if not years:
        return None, None
    first = _clean_text(years[0])
    if not first:
        return None, None
    if "-" in first:
        start_token, end_token = first.split("-", 1)
    else:
        start_token = first
        end_token = first
    return _year_token_to_value(start_token), _year_token_to_value(end_token)


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


def _normalize_make_key(value: str) -> str:
    text = _sanitize_text_for_parse(value).lower()
    if not text:
        return ""
    if re.search(r"\b(ram|dodge)\b", text):
        return "ram"
    if re.search(r"\b(ford)\b", text):
        return "ford"
    if re.search(r"\b(chevy|chevrolet|gmc|gm)\b", text):
        return "gm"
    if re.search(r"\b(nissan)\b", text):
        return "nissan"
    if re.search(r"\b(jeep)\b", text):
        return "jeep"
    return text


def _normalize_engine_family_key(value: str) -> str:
    text = _sanitize_text_for_parse(value).lower()
    if re.search(r"\bpower\s*stroke\b", text):
        return "powerstroke"
    if "duramax" in text:
        return "duramax"
    if "cummins" in text:
        return "cummins"
    if re.search(r"\beco\s*diesel\b", text):
        return "ecodiesel"
    return ""


def _normalize_engine_liter_text(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    match = re.search(r"(\d\.\d)", text)
    if not match:
        return ""
    return f"{match.group(1)}L"


def _parse_year_value(value: str) -> float | None:
    return _year_token_to_value(value)


@lru_cache(maxsize=16)
def _load_fitment_engine_rules_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict, ...]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    rules: list[dict] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family_key = _normalize_engine_family_key(_clean_text(row.get("family", "")))
                if not family_key:
                    continue
                liter = _normalize_engine_liter_text(
                    _clean_text(row.get("engine_liter", "")) or _clean_text(row.get("liter", ""))
                )
                if not liter:
                    continue
                start_value = _parse_year_value(_clean_text(row.get("year_start", "")))
                end_value = _parse_year_value(_clean_text(row.get("year_end", "")))
                if start_value is None or end_value is None:
                    continue
                make_key = _normalize_make_key(_clean_text(row.get("make", "")))
                rules.append(
                    {
                        "family": family_key,
                        "make": make_key,
                        "start": min(start_value, end_value),
                        "end": max(start_value, end_value),
                        "engine_liter": liter,
                    }
                )
    except Exception:
        return tuple()
    return tuple(rules)


def _load_fitment_engine_rules(required_root: Path | None) -> list[dict]:
    path = _fitment_engine_map_path(required_root)
    if path is None:
        return []
    try:
        stat = path.stat()
    except Exception:
        return []
    try:
        rules = _load_fitment_engine_rules_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return []
    return [dict(item) for item in rules]


def _detect_engine_family_key(context_text: str, drive: str) -> str:
    combined = " ".join([_clean_text(drive), _clean_text(context_text)]).strip()
    return _normalize_engine_family_key(combined)


def _infer_engine_liter_from_fitment_map(
    years: list[str],
    make_label: str,
    drive: str,
    context_text: str,
    required_root: Path | None,
) -> str:
    start_year, end_year = _first_year_window(years)
    if start_year is None and end_year is None:
        return ""
    if start_year is None:
        start_year = end_year
    if end_year is None:
        end_year = start_year

    family_key = _detect_engine_family_key(context_text=context_text, drive=drive)
    if not family_key:
        return ""

    rules = _load_fitment_engine_rules(required_root=required_root)
    if not rules:
        return ""

    make_key = _normalize_make_key(make_label)
    context_make_key = _normalize_make_key(context_text)
    best_rank: tuple[int, float, float] | None = None
    best_liter = ""

    for rule in rules:
        if _clean_text(rule.get("family", "")) != family_key:
            continue
        rule_start = float(rule.get("start", 0.0))
        rule_end = float(rule.get("end", 0.0))
        if end_year < rule_start or start_year > rule_end:
            continue

        rule_make = _clean_text(rule.get("make", ""))
        if rule_make:
            if make_key and rule_make == make_key:
                make_score = 2
            elif context_make_key and rule_make == context_make_key:
                make_score = 1
            else:
                continue
        else:
            make_score = 0

        range_span = max(0.0, rule_end - rule_start)
        overlap_span = max(0.0, min(end_year, rule_end) - max(start_year, rule_start))
        rank = (make_score, -range_span, overlap_span)
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_liter = _clean_text(rule.get("engine_liter", ""))

    return best_liter


def _infer_engine_liter_from_fitment(
    years: list[str],
    make_label: str,
    drive: str,
    context_text: str,
    required_root: Path | None = None,
) -> str:
    mapped = _infer_engine_liter_from_fitment_map(
        years=years,
        make_label=make_label,
        drive=drive,
        context_text=context_text,
        required_root=required_root,
    )
    if mapped:
        return mapped

    start_year, end_year = _first_year_window(years)
    drive_low = _clean_text(drive).lower()
    context_low = _sanitize_text_for_parse(context_text).lower()

    if make_label == "Ram" and ("cummins" in drive_low or "cummins" in context_low):
        if start_year is not None and start_year >= 2007.5:
            return "6.7L"
        if end_year is not None and end_year >= 2008.0:
            return "6.7L"
        if start_year is not None:
            return "5.9L"
        return ""

    if make_label == "Ford" and ("powerstroke" in drive_low or "powerstroke" in context_low):
        if start_year is None:
            return ""
        if start_year >= 2011:
            return "6.7L"
        if start_year >= 2008:
            return "6.4L"
        if start_year >= 2003:
            return "6.0L"
        if start_year >= 1994:
            return "7.3L"
        return ""

    if make_label == "GM" and ("duramax" in drive_low or "duramax" in context_low):
        return "6.6L"

    return ""


def _detect_drive_family(text: str, make_label: str) -> str:
    low = _sanitize_text_for_parse(text).lower()
    if make_label == "Ford":
        if "super duty" in low or "superduty" in low:
            return "Super Duty"
        return "Powerstroke"
    if make_label == "GM":
        if re.search(r"\b(2500hd\s*/\s*3500|2500hd|3500)\b", low) and "duramax" not in low:
            return "2500HD / 3500"
        return "Duramax"
    if make_label == "Ram":
        if re.search(r"\b(2500\s*/\s*3500|2500|3500)\b", low) and "cummins" not in low:
            return "2500 / 3500"
        return "Cummins"
    if re.search(r"\bpower\s*stroke\b", low):
        return "Powerstroke"
    if "duramax" in low:
        return "Duramax"
    if "cummins" in low:
        return "Cummins"
    if "ecodiesel" in low:
        return "EcoDiesel"
    if make_label == "Jeep" and "gladiator" in low:
        return "Gladiator"
    return ""


def _is_universal_fitment(text: str) -> bool:
    low = _sanitize_text_for_parse(text).lower()
    return any(
        token in low
        for token in [
            "universal",
            "all makes",
            "all models",
            "fits all",
            "multi fit",
            "multi-fit",
        ]
    )


def _detect_engine_family_token(text: str) -> str:
    low = _sanitize_text_for_parse(text).lower()
    if re.search(r"\bpower\s*stroke\b", low):
        return "Powerstroke"
    if "duramax" in low:
        return "Duramax"
    if "cummins" in low:
        return "Cummins"
    if "ecodiesel" in low:
        return "EcoDiesel"
    return ""


def _extract_liter_tokens(text: str) -> list[str]:
    source = _sanitize_text_for_parse(text)
    if not source:
        return []
    values: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"\b(\d\.\d)\s*l\b", source, flags=re.IGNORECASE):
        token = f"{match.group(1)}L"
        if token in seen:
            continue
        seen.add(token)
        values.append(token)
    if values:
        return values
    inferred = _detect_engine_liter(source)
    return [inferred] if inferred else []


def _extract_model_tokens(text: str, make_label: str) -> list[str]:
    source = _sanitize_text_for_parse(text)
    low = source.lower()
    found: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        token = _clean_text(value)
        key = token.lower()
        if not token or key in seen:
            return
        seen.add(key)
        found.append(token)

    if make_label == "Ram":
        for token in ["1500", "2500", "3500", "4500", "5500"]:
            if re.search(rf"\b{re.escape(token)}\b", low):
                add(token)
    elif make_label == "Ford":
        if re.search(r"\bf[\s-]*150\b", low):
            add("F-150")
        if re.search(r"\bf[\s-]*250\b", low):
            add("F-250")
        if re.search(r"\bf[\s-]*350\b", low):
            add("F-350")
        if "super duty" in low:
            add("Super Duty")
    elif make_label == "GM":
        if "2500hd" in low:
            add("2500HD")
        if "3500hd" in low:
            add("3500HD")
        for token in ["1500", "2500", "3500"]:
            if re.search(rf"\b{re.escape(token)}\b", low):
                add(token)
        if "colorado" in low:
            add("Colorado")
        if "canyon" in low:
            add("Canyon")
    elif make_label == "Jeep":
        if "gladiator" in low:
            add("Gladiator")
        if "wrangler" in low:
            add("Wrangler")

    return found


def _parse_year_range_token(token: str) -> tuple[float, float] | None:
    text = _clean_text(token)
    if not text:
        return None
    if "-" in text:
        start_token, end_token = text.split("-", 1)
    else:
        start_token = text
        end_token = text
    start = _year_token_to_value(start_token)
    end = _year_token_to_value(end_token)
    if start is None or end is None:
        return None
    return (min(start, end), max(start, end))


def _format_short_year_value(value: float) -> str:
    base_year = int(value)
    short = f"{base_year % 100:02d}"
    if abs(value - (base_year + 0.5)) < 0.01:
        return f"{short}.5"
    return short


def _format_short_year_range(start: float, end: float) -> str:
    start_text = _format_short_year_value(start)
    end_text = _format_short_year_value(end)
    if start_text == end_text:
        return start_text
    return f"{start_text}-{end_text}"


def _sort_liter_tokens(tokens: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        value = _clean_text(token)
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        unique.append(value)

    def sort_key(value: str) -> tuple[float, str]:
        match = re.search(r"(\d\.\d)", value)
        if not match:
            return (999.0, value.lower())
        try:
            return (float(match.group(1)), value.lower())
        except Exception:
            return (999.0, value.lower())

    return sorted(unique, key=sort_key)


def _looks_like_body_part(title: str, description: str) -> bool:
    low = _sanitize_text_for_parse(" ".join([title, description])).lower()
    if not low:
        return False
    # Keep this conservative; many engine parts include words like "cap/plug/cover".
    body_patterns = [
        r"\bbumper\b",
        r"\bfender\b",
        r"\bgrille\b",
        r"\bgrill\b",
        r"\bmirror\b",
        r"\bbody\s*panel\b",
        r"\btailgate\b",
        r"\bheadlight\b",
        r"\btail\s*light\b",
        r"\bdoor\s*handle\b",
        r"\brunning\s*board\b",
        r"\bstep\s*bar\b",
        r"\bmud\s*flap\b",
        r"\bflare\b",
    ]
    return any(re.search(pattern, low) for pattern in body_patterns)


def _has_contiguous_year_coverage(items: list[dict]) -> bool:
    intervals: list[tuple[int, int]] = []
    for item in items:
        start = int(float(item.get("start", 0.0)))
        end = int(float(item.get("end", 0.0)))
        intervals.append((min(start, end), max(start, end)))
    if not intervals:
        return False
    intervals.sort(key=lambda pair: (pair[0], pair[1]))
    merged: list[list[int]] = [[intervals[0][0], intervals[0][1]]]
    for start, end in intervals[1:]:
        current = merged[-1]
        if start <= current[1] + 1:
            current[1] = max(current[1], end)
        else:
            merged.append([start, end])
    return len(merged) == 1


def _build_fitment_items(
    application: str,
    title: str,
    description: str,
    required_root: Path | None = None,
) -> list[dict]:
    source = _sanitize_text_for_parse(application)
    if not source:
        source = _sanitize_text_for_parse(" ".join([application, title, description]))
    if not source:
        return []

    segments = [item.strip() for item in re.split(r"\s*\|\s*", source) if _clean_text(item)]
    if not segments:
        segments = [source]

    fallback_context = _sanitize_text_for_parse(" ".join([application, title, description]))
    rows: list[dict] = []
    for segment_index, segment in enumerate(segments):
        chunk = _sanitize_text_for_parse(segment)
        if not chunk:
            continue
        segment_make = _detect_make_label(chunk) or _detect_make_label(fallback_context)
        segment_family = _detect_engine_family_token(chunk) or _detect_drive_family(chunk, segment_make)
        segment_liters = _extract_liter_tokens(chunk)
        segment_models = _extract_model_tokens(chunk, segment_make)
        parts = [item.strip() for item in re.split(r"\s*/\s*(?=\d{2,4}(?:\.5)?\s*[-/])", chunk) if _clean_text(item)]
        if not parts:
            parts = [chunk]

        carry_make = segment_make
        carry_family = segment_family
        for part in parts:
            year_tokens = _extract_year_ranges(part)
            if not year_tokens:
                continue

            part_make = _detect_make_label(part) or carry_make or segment_make
            part_family = _detect_engine_family_token(part) or _detect_drive_family(part, part_make) or carry_family or segment_family
            part_liters = _extract_liter_tokens(part) or list(segment_liters)
            if not part_liters and part_make:
                inferred_liter = _infer_engine_liter_from_fitment(
                    years=[year_tokens[0]],
                    make_label=part_make,
                    drive=part_family,
                    context_text=" ".join([part, chunk, fallback_context]),
                    required_root=required_root,
                )
                if inferred_liter:
                    part_liters = [inferred_liter]
            part_models = _extract_model_tokens(part, part_make) or list(segment_models)

            for token in year_tokens:
                parsed_range = _parse_year_range_token(token)
                if parsed_range is None:
                    continue
                start, end = parsed_range
                rows.append(
                    {
                        "segment_index": segment_index,
                        "make_label": part_make,
                        "family": part_family,
                        "liters": _sort_liter_tokens(part_liters),
                        "models": part_models,
                        "start": start,
                        "end": end,
                    }
                )
            carry_make = part_make or carry_make
            carry_family = part_family or carry_family

    deduped: list[dict] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (
            int(float(row.get("segment_index", 0))),
            _clean_text(row.get("make_label", "")),
            _clean_text(row.get("family", "")),
            tuple(_sort_liter_tokens([_clean_text(item) for item in row.get("liters", [])])),
            tuple([_clean_text(item) for item in row.get("models", [])]),
            float(row.get("start", 0.0)),
            float(row.get("end", 0.0)),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _format_fitment_for_make(items: list[dict], make_label: str, is_body_part: bool) -> str:
    if not items:
        return ""
    ordered = sorted(items, key=lambda item: (float(item.get("start", 0.0)), float(item.get("end", 0.0))))

    families = [_clean_text(item.get("family", "")) for item in ordered if _clean_text(item.get("family", ""))]
    family = families[-1] if families else ""
    all_liters = _sort_liter_tokens([_clean_text(lit) for item in ordered for lit in item.get("liters", []) if _clean_text(lit)])
    all_models: list[str] = []
    seen_models: set[str] = set()
    for item in ordered:
        for model in item.get("models", []):
            token = _clean_text(model)
            key = token.lower()
            if not token or key in seen_models:
                continue
            seen_models.add(key)
            all_models.append(token)

    contiguous = _has_contiguous_year_coverage(ordered)
    if contiguous:
        start = min(float(item.get("start", 0.0)) for item in ordered)
        end = max(float(item.get("end", 0.0)) for item in ordered)
        year_text = _format_short_year_range(start, end)
        parts: list[str] = [year_text, make_label]
        if is_body_part:
            if all_models:
                parts.append(" / ".join(all_models))
        else:
            if all_liters:
                parts.append(" / ".join(all_liters))
            if family:
                parts.append(family)
        return re.sub(r"\s+", " ", " ".join([item for item in parts if _clean_text(item)])).strip()

    segments: list[str] = []
    for index, item in enumerate(ordered):
        year_text = _format_short_year_range(float(item.get("start", 0.0)), float(item.get("end", 0.0)))
        item_liters = _sort_liter_tokens([_clean_text(lit) for lit in item.get("liters", []) if _clean_text(lit)])
        item_models: list[str] = []
        seen_local_models: set[str] = set()
        for model in item.get("models", []):
            token = _clean_text(model)
            key = token.lower()
            if not token or key in seen_local_models:
                continue
            seen_local_models.add(key)
            item_models.append(token)

        if is_body_part:
            model_text = " / ".join(item_models)
            if index == 0:
                piece = " ".join([year_text, make_label, model_text]).strip()
            else:
                piece = " ".join([year_text, model_text]).strip()
            segments.append(re.sub(r"\s+", " ", piece).strip())
            continue

        liter_text = " / ".join(item_liters)
        if index == 0:
            piece = " ".join([year_text, make_label, liter_text]).strip()
        else:
            tail_family = family if index == (len(ordered) - 1) else ""
            piece = " ".join([year_text, liter_text, tail_family]).strip()
        segments.append(re.sub(r"\s+", " ", piece).strip())

    return " / ".join([item for item in segments if _clean_text(item)]).strip()


def _build_fitment_suffix_legacy(
    application: str,
    title: str,
    description: str,
    required_root: Path | None = None,
) -> str:
    combined = " ".join([application, title, description]).strip()
    if not combined or _is_universal_fitment(combined):
        return ""

    years = _extract_year_ranges(application) or _extract_year_ranges(title) or _extract_year_ranges(description)
    make_label = _detect_make_label(combined)
    drive = _detect_drive_family(combined, make_label)
    liter = _infer_engine_liter_from_fitment(
        years=years,
        make_label=make_label,
        drive=drive,
        context_text=combined,
        required_root=required_root,
    )
    if not liter:
        liter = _detect_engine_liter(application) or _detect_engine_liter(title) or _detect_engine_liter(combined)

    parts: list[str] = []
    if years:
        parts.append(years[0])
    if make_label:
        parts.append(make_label)
    if liter:
        parts.append(liter)
    if drive:
        parts.append(drive)
    return " ".join([item for item in parts if item]).strip()


def _build_fitment_suffix(
    application: str,
    title: str,
    description: str,
    required_root: Path | None = None,
) -> str:
    combined = " ".join([application, title, description]).strip()
    if not combined or _is_universal_fitment(combined):
        return ""

    fitment_items = _build_fitment_items(
        application=application,
        title=title,
        description=description,
        required_root=required_root,
    )
    if not fitment_items:
        return _build_fitment_suffix_legacy(
            application=application,
            title=title,
            description=description,
            required_root=required_root,
        )

    engine_families = {"Cummins", "Duramax", "Powerstroke", "EcoDiesel"}
    has_liters = any(bool(item.get("liters", [])) for item in fitment_items)
    has_engine_family = any(_clean_text(item.get("family", "")) in engine_families for item in fitment_items)
    has_models = any(bool(item.get("models", [])) for item in fitment_items)
    fitment_looks_like_body = has_models and not has_liters and not has_engine_family
    is_body_part = fitment_looks_like_body or _looks_like_body_part(title=title, description=description)
    grouped: dict[str, list[dict]] = {}
    make_order: list[str] = []
    for item in fitment_items:
        make_label = _clean_text(item.get("make_label", "")) or _detect_make_label(combined)
        if not make_label:
            continue
        if make_label not in grouped:
            grouped[make_label] = []
            make_order.append(make_label)
        grouped[make_label].append(item)

    formatted: list[str] = []
    for make_label in make_order:
        segment_text = _format_fitment_for_make(grouped.get(make_label, []), make_label=make_label, is_body_part=is_body_part)
        if segment_text:
            formatted.append(segment_text)

    if formatted:
        return " | ".join(formatted)
    return _build_fitment_suffix_legacy(
        application=application,
        title=title,
        description=description,
        required_root=required_root,
    )


def _strip_vendor_tokens(text: str, vendor_tokens: list[str]) -> str:
    output = f" {text} "
    phrase_candidates: set[str] = set()
    for token in vendor_tokens:
        key = _sanitize_text_for_parse(token).lower()
        if not key:
            continue
        words = [part for part in key.split(" ") if part]
        if not words:
            continue
        phrase_candidates.add(" ".join(words))
        if len(words) >= 3:
            phrase_candidates.add(" ".join(words[:2]))
    for phrase in sorted(phrase_candidates, key=lambda item: (-len(item.split(" ")), -len(item))):
        phrase_pattern = r"\s+".join(re.escape(part) for part in phrase.split(" ") if part)
        if not phrase_pattern:
            continue
        pattern = re.compile(rf"\b{phrase_pattern}\b", flags=re.IGNORECASE)
        output = pattern.sub(" ", output)
    return _sanitize_text_for_parse(output)


def _build_concise_description(
    title: str,
    application: str,
    description: str,
    vendor_tokens: list[str],
) -> str:
    source = _sanitize_text_for_parse(title)
    if not source:
        source = _sanitize_text_for_parse(description)
    if not source:
        return ""

    source = _strip_vendor_tokens(source, vendor_tokens)
    source = re.sub(r"\b\d{2,4}(?:\.5)?\s*[-/]\s*\d{2,4}(?:\.5)?\b", " ", source)
    source = re.sub(r"\b(?:ford|gm|gmc|chevy|chevrolet|ram|dodge)\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(r"\b\d\.\d\s*l\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(
        r"\b\d\.\d\s*(?=(?:power\s*stroke|powerstroke|duramax|cummins|eco\s*diesel|ecodiesel)\b)",
        " ",
        source,
        flags=re.IGNORECASE,
    )
    source = re.sub(
        r"\b(?:power\s*stroke|powerstroke|duramax|cummins|eco\s*diesel|ecodiesel)\s*\d\.\d\b",
        " ",
        source,
        flags=re.IGNORECASE,
    )
    source = re.sub(
        r"\b(?:power\s*stroke|powerstroke|duramax|cummins|eco\s*diesel|ecodiesel|super\s*duty)\b",
        " ",
        source,
        flags=re.IGNORECASE,
    )
    # Vendor brand already carries diesel context; avoid repeating "diesel" in the product phrase.
    source = re.sub(r"\bdiesel\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(r"\b(?:lly|lbz|lmm|lb7|l5p|lml|6r140|68rfe|48re|47re)(?:/[a-z0-9]+)*\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(
        r"\b(?:no\s+valve\s+seat\s+machining|no\s+seats?\s+or\s+valves?|w/?o\s+valves?)\b",
        " ",
        source,
        flags=re.IGNORECASE,
    )
    source = re.sub(r"[|]+", " ", source)
    source = re.sub(r"\s*-\s*", "-", source)
    source = re.sub(r"(?<!\d)-(?!\d)", " ", source)
    source = re.sub(r"\s+", " ", source).strip(" ,;/")

    if not source:
        return ""

    words = source.split(" ")
    if len(words) > 9:
        words = words[:9]
    concise = " ".join(words)
    concise = re.sub(r"\s+", " ", concise).strip()
    return concise


def _strip_leading_vendor_phrases(text: str, vendor_tokens: list[str]) -> str:
    output = _sanitize_text_for_parse(text)
    if not output:
        return ""

    phrase_candidates: set[str] = set()
    for token in vendor_tokens:
        key = _sanitize_text_for_parse(token).lower()
        if not key:
            continue
        words = [part for part in key.split(" ") if part]
        if len(words) < 2:
            continue
        phrase_candidates.add(" ".join(words))
        if len(words) >= 3:
            phrase_candidates.add(" ".join(words[:2]))

    if not phrase_candidates:
        return output

    ordered = sorted(phrase_candidates, key=lambda item: (-len(item.split(" ")), -len(item)))
    while True:
        changed = False
        for phrase in ordered:
            phrase_pattern = r"\s+".join(re.escape(part) for part in phrase.split(" ") if part)
            if not phrase_pattern:
                continue
            pattern = re.compile(rf"^(?:{phrase_pattern})(?:\b|[\s:/|,-])+", flags=re.IGNORECASE)
            updated = pattern.sub("", output).strip()
            if updated != output:
                output = updated
                changed = True
                break
        if not changed or not output:
            break
    return output


def _normalize_title(
    vendor: str,
    title: str,
    application: str,
    description: str,
    required_root: Path | None = None,
) -> str:
    canonical_vendor = _normalize_vendor_name(vendor, required_root=required_root)
    title_vendor = resolve_vendor_title_name(canonical_vendor or vendor, required_root=required_root)
    brand_label = title_vendor or canonical_vendor or _clean_text(vendor)

    vendor_tokens = [vendor, canonical_vendor, title_vendor]
    concise = _build_concise_description(
        title=title,
        application=application,
        description=description,
        vendor_tokens=[token for token in vendor_tokens if _clean_text(token)],
    )
    concise = _strip_leading_vendor_phrases(
        concise,
        vendor_tokens=[brand_label] + [token for token in vendor_tokens if _clean_text(token)],
    )
    fitment = _build_fitment_suffix(
        application=application,
        title=title,
        description=description,
        required_root=required_root,
    )

    parts: list[str] = []
    if brand_label:
        parts.append(brand_label)
    if concise:
        parts.append(concise)
    if fitment:
        parts.append(fitment)

    if not parts:
        return _sanitize_text_for_parse(title)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _normalize_media_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in values:
        url = _clean_text(raw)
        if not url:
            continue
        if url.lower().startswith("www."):
            url = f"https://{url}"
        if not re.match(r"^https?://", url, flags=re.IGNORECASE):
            continue
        if url in seen:
            continue
        seen.add(url)
        normalized.append(url)
    return normalized


def _normalize_weight(weight: str) -> str:
    value = _to_float(weight)
    if value is None:
        return "2"
    value = max(2.0, min(value, 149.0))
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _format_collection_titles(items: list[dict]) -> str:
    titles: list[str] = []
    seen: set[str] = set()
    for item in items:
        title = _clean_text((item or {}).get("collection_title", ""))
        key = re.sub(r"\s+", " ", title).strip().lower()
        if not title or key in seen:
            continue
        seen.add(key)
        titles.append(title)
    return ", ".join(titles)


def _format_currency(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _normalize_currency_text(value: str) -> str:
    return _format_currency(_to_float(value))


def _resolve_effective_price(product: Product) -> tuple[str, str]:
    map_value = _to_float(product.map_price)
    base_value = _to_float(product.price)
    msrp_value = _to_float(product.msrp_price)
    jobber_value = _to_float(product.jobber_price)

    if map_value is not None:
        return _format_currency(map_value), "map_price"
    if base_value is not None:
        return _format_currency(base_value), "price"
    if msrp_value is not None:
        return _format_currency(msrp_value), "msrp_price"
    if jobber_value is not None:
        return _format_currency(jobber_value), "jobber_price"
    return "", "missing"


def _infer_cost_if_missing(product: Product, discounts_root: Path) -> tuple[str, str]:
    price_value = _to_float(product.price)
    if price_value is None:
        return "", "missing_price"
    if _to_float(product.cost) is not None:
        return _format_currency(_to_float(product.cost)), "already_present"

    discount_file = find_vendor_discount_file(discounts_root)
    if discount_file is None:
        return "", "discount_file_missing"

    discounts_df = load_vendor_discounts(discount_file)
    profile = resolve_vendor_profile(product.vendor, required_root=discounts_root)
    vendor_for_discount = (
        (profile.discount_vendor_key if profile is not None else "")
        or normalize_vendor_from_rules(product.vendor, required_root=discounts_root)
        or product.vendor
    )
    discount_percent, _, status = choose_best_discount(
        discounts_df=discounts_df,
        vendor_name=vendor_for_discount,
        product_title=product.title,
        product_type=product.type,
    )
    if discount_percent is None:
        return "", status
    inferred = calculate_cost_from_price(price=price_value, discount_percent=discount_percent)
    return _format_currency(inferred), "discount_applied"


def normalize_product(
    product: Product,
    required_root: Path,
    mode: str = "new",
    update_fields: set[str] | None = None,
    default_inventory: int | None = None,
) -> Product:
    selected = set(update_fields or [])

    def should(field_name: str) -> bool:
        if mode != "update":
            return True
        return field_name in selected

    normalized_vendor = _normalize_vendor_name(product.vendor, required_root=required_root)
    profile = resolve_vendor_profile(normalized_vendor or product.vendor, required_root=required_root)
    profile_vendor = _clean_text(profile.shopify_vendor_value) if profile is not None else ""
    if not profile_vendor:
        profile_vendor = _clean_text(profile.canonical_vendor) if profile is not None else ""
    profile_brand = _clean_text(profile.brand_name) if profile is not None else ""
    profile_sku_prefix = _clean_text(profile.sku_prefix) if profile is not None else ""

    if should("vendor"):
        product.vendor = profile_vendor or normalized_vendor
    if should("application"):
        product.application = _clean_text(product.application)
    if should("description_html"):
        product.description_html = _normalize_description_html(product.description_html)
    if should("title"):
        product.title = _normalize_title(
            vendor=product.vendor,
            title=product.title,
            application=product.application,
            description=product.description_html,
            required_root=required_root,
        )
    if should("application") and mode != "update":
        derived_application = _derive_application_from_title(product.title)
        if derived_application and (not _clean_text(product.application) or not _looks_like_fitment_text(product.application)):
            product.application = derived_application
    if should("media_urls"):
        product.media_urls = _normalize_media_urls(product.media_urls)
    if should("price"):
        product.map_price = _normalize_currency_text(product.map_price)
        product.msrp_price = _normalize_currency_text(product.msrp_price)
        product.jobber_price = _normalize_currency_text(product.jobber_price)
        resolved_price, price_source = _resolve_effective_price(product)
        product.price = resolved_price
        product.field_sources["price_rule"] = price_source
    if should("weight"):
        product.weight = _normalize_weight(product.weight)
    if mode != "update" or "core_charge_product_code" in selected:
        product.core_charge_product_code = normalize_core_charge_product_code(
            product.core_charge_product_code,
            required_root=required_root,
        )
    inventory_value: int | None = None
    if default_inventory is not None:
        try:
            inventory_value = int(float(default_inventory))
        except Exception:
            inventory_value = None
    if inventory_value is None or inventory_value <= 0:
        try:
            current_inventory = int(float(getattr(product, "inventory", 0)))
        except Exception:
            current_inventory = 0
        inventory_value = current_inventory if current_inventory > 0 else 3000000
    product.inventory = inventory_value

    if mode != "update":
        product.sku = _apply_vendor_sku_prefix(product.sku, profile_sku_prefix)
        if not _clean_text(product.brand) and profile_brand:
            product.brand = profile_brand
        product.tags = []
        product.field_sources.pop("tags", None)
        product.field_sources.pop("tags_ymm_warning", None)

    should_autofill_collections = mode != "update" or should("type") or should("application")
    if should_autofill_collections and not _clean_text(getattr(product, "collections", "")):
        collection_targets, collection_warnings = resolve_collection_assignments(
            product_type=_clean_text(product.type),
            application_text=_clean_text(product.application),
            required_root=required_root,
            title_text=_clean_text(product.title),
            description_text=_clean_text(product.description_html),
        )
        product.collections = _format_collection_titles(collection_targets)
        if product.collections:
            product.field_sources["collections"] = "collection_rules"
            product.field_sources.pop("collections_warning", None)
        elif collection_warnings:
            product.field_sources["collections_warning"] = "; ".join(collection_warnings[:2])

    if should("cost"):
        cost_value = _to_float(product.cost)
        dealer_cost_value = _to_float(product.dealer_cost)
        if dealer_cost_value is not None:
            product.dealer_cost = _format_currency(dealer_cost_value)

        known_costs = [value for value in [cost_value, dealer_cost_value] if value is not None]
        if known_costs:
            lowest_cost = min(known_costs)
            product.cost = _format_currency(lowest_cost)
            if dealer_cost_value is not None and (cost_value is None or dealer_cost_value <= cost_value):
                product.field_sources["cost_inference"] = "dealer_lowest"
            else:
                product.field_sources["cost_inference"] = "already_present"
        else:
            inferred_cost, reason = _infer_cost_if_missing(product, required_root)
            if inferred_cost:
                product.cost = inferred_cost
            product.field_sources["cost_inference"] = reason

    product.finalize_defaults()
    return product
