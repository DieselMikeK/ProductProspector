from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_phrase(value: object) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", _clean_text(value).lower())).strip()


@dataclass(frozen=True)
class PricePriorityRules:
    priority: tuple[str, ...]
    aliases: dict[str, tuple[str, ...]]


def _default_price_priority_rules() -> PricePriorityRules:
    aliases = {
        "map": ("map", "minimum advertised price", "m a p", "m.a.p"),
        "jobber": ("jobber", "jobber price", "jobber net", "jobber cost"),
        "msrp": ("msrp", "retail", "retail price", "list price", "suggested retail"),
        "price": ("price", "unit price", "sell price", "our price"),
        "dealer": ("dealer", "dealer cost", "dealer price", "dealer net", "dealer t1", "dealer t2"),
    }
    return PricePriorityRules(priority=("map", "jobber", "msrp", "price"), aliases=aliases)


def find_price_priority_rules_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "rules" / "pricing_priority_rules.json",
        required_root / "rules" / "price_priority_rules.json",
        required_root / "rules" / "pricing_rules.json",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def load_price_priority_rules(required_root: Path | None) -> PricePriorityRules:
    defaults = _default_price_priority_rules()
    path = find_price_priority_rules_file(required_root)
    if path is None:
        return defaults
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return defaults

    requested_priority = raw.get("price_priority", [])
    priority: list[str] = []
    for item in requested_priority if isinstance(requested_priority, list) else []:
        text = _normalize_phrase(item)
        if text in {"map", "jobber", "msrp", "price"} and text not in priority:
            priority.append(text)
    for role in defaults.priority:
        if role not in priority:
            priority.append(role)

    aliases_block = raw.get("column_aliases", {})
    aliases: dict[str, tuple[str, ...]] = {}
    for role, default_values in defaults.aliases.items():
        configured_values = aliases_block.get(role, []) if isinstance(aliases_block, dict) else []
        values: list[str] = []
        for entry in configured_values if isinstance(configured_values, list) else []:
            normalized = _normalize_phrase(entry)
            if normalized and normalized not in values:
                values.append(normalized)
        for entry in default_values:
            normalized = _normalize_phrase(entry)
            if normalized and normalized not in values:
                values.append(normalized)
        aliases[role] = tuple(values)

    return PricePriorityRules(priority=tuple(priority), aliases=aliases)


def classify_price_column_role(column_name: object, rules: PricePriorityRules) -> str:
    normalized = _normalize_phrase(column_name)
    if not normalized:
        return ""

    for role in ["map", "jobber", "msrp", "dealer", "price"]:
        aliases = rules.aliases.get(role, ())
        for alias in aliases:
            if normalized == alias:
                return role

    for role in ["map", "jobber", "msrp", "dealer", "price"]:
        aliases = rules.aliases.get(role, ())
        for alias in aliases:
            if alias and alias in normalized:
                return role

    tokens = set(normalized.split())
    if "map" in tokens:
        return "map"
    if "jobber" in tokens:
        return "jobber"
    if "msrp" in tokens:
        return "msrp"
    if "dealer" in tokens:
        return "dealer"
    if "price" in tokens and "cost" not in tokens:
        return "price"
    return ""
