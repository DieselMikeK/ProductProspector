from __future__ import annotations

import csv
import re
from difflib import SequenceMatcher
from pathlib import Path


_STOP_WORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "can",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "they",
    "them",
    "their",
    "we",
    "our",
    "you",
    "your",
}

_PRODUCT_TAG_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _catalog_path(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    return required_root / "mappings" / "ProductTags.csv"


def is_valid_product_tag(tag: str) -> bool:
    value = _clean_text(tag)
    if not value:
        return False
    return bool(_PRODUCT_TAG_PATTERN.fullmatch(value))


def _normalize_tag_key(tag: str) -> str:
    return _clean_text(tag).lower()


def _read_catalog(path: Path) -> list[str]:
    if not path.exists():
        return []
    rows: list[str] = []
    seen: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames and "tag" in [str(item or "").strip().lower() for item in reader.fieldnames]:
                for row in reader:
                    value = _clean_text((row or {}).get("tag", ""))
                    key = _normalize_tag_key(value)
                    if not is_valid_product_tag(value) or key in seen:
                        continue
                    seen.add(key)
                    rows.append(value)
            else:
                handle.seek(0)
                for line in handle:
                    value = _clean_text(line)
                    key = _normalize_tag_key(value)
                    if not is_valid_product_tag(value) or key in seen:
                        continue
                    seen.add(key)
                    rows.append(value)
    except Exception:
        return []
    rows.sort(key=lambda item: item.lower())
    return rows


def _write_catalog(path: Path, tags: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in sorted(tags, key=lambda item: str(item).lower()):
        value = _clean_text(raw)
        key = _normalize_tag_key(value)
        if not is_valid_product_tag(value) or key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["tag"])
        for tag in ordered:
            writer.writerow([tag])


def load_tag_catalog(required_root: Path | None) -> list[str]:
    path = _catalog_path(required_root)
    if path is None:
        return []
    return _read_catalog(path)


def append_tags_to_catalog(required_root: Path | None, tags: list[str]) -> None:
    path = _catalog_path(required_root)
    if path is None:
        return
    current = _read_catalog(path)
    merged = list(current)
    seen = {_normalize_tag_key(item) for item in current}
    for raw in tags or []:
        value = _clean_text(raw)
        key = _normalize_tag_key(value)
        if not is_valid_product_tag(value) or key in seen:
            continue
        seen.add(key)
        merged.append(value)
    _write_catalog(path, merged)


def _extract_keywords(text: str) -> list[str]:
    source = _clean_text(text)
    if not source:
        return []
    source = re.sub(r"<[^>]+>", " ", source)
    source = source.replace("&nbsp;", " ").replace("&amp;", "&")
    source = re.sub(r"[^\w\s\-_\.]", " ", source)
    words = source.lower().split()
    return [item for item in words if len(item) > 2 and item not in _STOP_WORDS]


def _normalize_compare_tag(tag: str) -> str:
    return _clean_text(tag).lower().replace(" ", "").replace("-", "").replace("_", "")


def suggest_tags_for_product(
    title: str,
    description_html: str,
    application: str,
    vendor: str = "",
    product_type: str = "",
    tags_list: list[str] | None = None,
    threshold: float = 0.80,
    max_tags: int = 10,
) -> list[str]:
    source_tags = [_clean_text(item) for item in (tags_list or []) if is_valid_product_tag(_clean_text(item))]
    if not source_tags:
        return []

    full_text_source = " ".join(
        [
            _clean_text(title),
            _clean_text(description_html),
            _clean_text(application),
            _clean_text(vendor),
            _clean_text(product_type),
        ]
    )
    keywords = _extract_keywords(full_text_source)
    if not keywords:
        return []

    tag_scores: dict[str, float] = {}
    full_text = " ".join(keywords)
    normalized_full = full_text.replace(" ", "").replace("-", "").replace("_", "")

    # Exact matches are strongest.
    for tag in source_tags:
        compare = _normalize_compare_tag(tag)
        if not compare:
            continue
        if tag.lower() in full_text or compare in normalized_full:
            tag_scores[tag] = tag_scores.get(tag, 0.0) + 15.0

    # Fuzzy token matches.
    for keyword in keywords:
        if len(keyword) < 4:
            continue
        for tag in source_tags:
            score = SequenceMatcher(None, keyword.lower(), tag.lower()).ratio()
            if tag.lower() in keyword or keyword in tag.lower():
                score = max(score, 0.90)
            if score >= threshold:
                tag_scores[tag] = tag_scores.get(tag, 0.0) + (score * 5.0)

    # Substring support for model-number style tags.
    for keyword in keywords:
        keyword_clean = keyword.replace(".", "").replace(" ", "")
        if len(keyword_clean) < 2:
            continue
        for tag in source_tags:
            if keyword_clean in _normalize_compare_tag(tag):
                tag_scores[tag] = tag_scores.get(tag, 0.0) + 3.0

    ordered = sorted(tag_scores.items(), key=lambda item: item[1], reverse=True)
    output: list[str] = []
    for tag, _score in ordered:
        if tag in output:
            continue
        output.append(tag)
        if len(output) >= max(1, int(max_tags)):
            break
    return output
