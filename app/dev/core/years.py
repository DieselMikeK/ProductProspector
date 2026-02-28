from __future__ import annotations

import re


YEAR_MIN = 1950
YEAR_MAX = 2035

SINGLE_YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")
RANGE_PATTERN = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\s*(?:-|to|–)\s*(19\d{2}|20\d{2}|21\d{2})\b", re.IGNORECASE)
YEAR_CLUSTER_PATTERN = re.compile(
    r"(?:19\d{2}|20\d{2}|21\d{2})(?:\s*(?:-|to|–)\s*(?:19\d{2}|20\d{2}|21\d{2}))?(?:\s*,\s*(?:19\d{2}|20\d{2}|21\d{2})(?:\s*(?:-|to|–)\s*(?:19\d{2}|20\d{2}|21\d{2}))?)*",
    re.IGNORECASE,
)


def _is_valid_year(year: int) -> bool:
    return YEAR_MIN <= year <= YEAR_MAX


def parse_years_from_text(text: str | None) -> list[int]:
    if not text:
        return []

    years: set[int] = set()
    content = str(text)

    for match in RANGE_PATTERN.finditer(content):
        start = int(match.group(1))
        end = int(match.group(2))
        if start > end:
            start, end = end, start
        for year in range(start, end + 1):
            if _is_valid_year(year):
                years.add(year)

    for match in SINGLE_YEAR_PATTERN.finditer(content):
        year = int(match.group(1))
        if _is_valid_year(year):
            years.add(year)

    return sorted(years)


def parse_years_from_many(values: list[str | None]) -> list[int]:
    combined: set[int] = set()
    for value in values:
        combined.update(parse_years_from_text(value))
    return sorted(combined)


def apply_year_policy(current: list[int], incoming: list[int], policy: str) -> list[int]:
    current_set = set(current)
    incoming_set = set(incoming)

    if policy == "replace":
        return sorted(incoming_set)
    if policy == "merge":
        return sorted(current_set | incoming_set)
    if policy == "add_missing":
        return sorted(current_set | (incoming_set - current_set))

    return sorted(current_set)


def format_years_compact(years: list[int]) -> str:
    if not years:
        return ""

    sorted_years = sorted(set(years))
    ranges: list[tuple[int, int]] = []
    start = sorted_years[0]
    end = sorted_years[0]

    for year in sorted_years[1:]:
        if year == end + 1:
            end = year
        else:
            ranges.append((start, end))
            start = year
            end = year
    ranges.append((start, end))

    parts: list[str] = []
    for first, last in ranges:
        if first == last:
            parts.append(str(first))
        else:
            parts.append(f"{first}-{last}")
    return ", ".join(parts)


def replace_years_in_text(text: str | None, years: list[int]) -> str:
    if not text:
        return ""

    replacement = format_years_compact(years)
    if not replacement:
        return str(text)

    content = str(text)
    if YEAR_CLUSTER_PATTERN.search(content):
        return YEAR_CLUSTER_PATTERN.sub(replacement, content, count=1)
    return f"{content} ({replacement})"

