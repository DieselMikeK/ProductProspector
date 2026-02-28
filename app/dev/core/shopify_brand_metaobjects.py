from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

import pandas as pd

from product_prospector.core.config_store import ShopifyConfig


DEFAULT_BRAND_METAOBJECT_FILE = "ShopifyBrandMetaobjects.csv"


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_aliases(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    return [_clean_text(item) for item in parts if _clean_text(item)]


def _request_graphql(config: ShopifyConfig, access_token: str, query: str, variables: dict) -> tuple[dict | None, str | None]:
    # Hard safety guard: this module is read-only by design.
    query_text = _clean_text(query).lower()
    if "mutation" in query_text or "delete" in query_text or "update" in query_text:
        return None, "Read-only guard blocked non-query GraphQL operation."
    if not query_text.startswith("query"):
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


_PRODUCT_BRAND_TYPE_QUERY = """
query ProductBrandType($cursor: String) {
  products(first: 75, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        metafield(namespace: "custom", key: "brand") {
          reference {
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
"""

_METAOBJECTS_QUERY = """
query BrandMetaobjects($type: String!, $cursor: String) {
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


def detect_brand_metaobject_type(
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
            query=_PRODUCT_BRAND_TYPE_QUERY,
            variables={"cursor": cursor},
        )
        if error:
            break
        products = (data or {}).get("products") or {}
        edges = products.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            metafield = node.get("metafield") or {}
            reference = metafield.get("reference") or {}
            if _clean_text(reference.get("__typename")) != "Metaobject":
                continue
            reference_type = _clean_text(reference.get("type"))
            if reference_type:
                return reference_type, None
        page_info = products.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

    for candidate in ["brand", "brands", "shopify--brand"]:
        _, ping_error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_METAOBJECT_PING_QUERY,
            variables={"type": candidate},
        )
        if ping_error is None:
            return candidate, "Brand type inferred from fallback candidate."

    return "brand", "Could not auto-detect brand type from products; using default type 'brand'."


def fetch_brand_metaobjects(
    config: ShopifyConfig,
    access_token: str,
    metaobject_type: str | None = None,
    max_pages: int = 200,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None, str]:
    resolved_type = _clean_text(metaobject_type)
    detect_note: str | None = None
    if not resolved_type:
        resolved_type, detect_note = detect_brand_metaobject_type(config=config, access_token=access_token)

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
            gid = _clean_text((node or {}).get("id"))
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

            rows.append(
                {
                    "brand_name": _clean_text((node or {}).get("displayName")),
                    "brand_handle": _clean_text((node or {}).get("handle")),
                    "brand_gid": gid,
                    "metaobject_type": _clean_text((node or {}).get("type")) or resolved_type,
                    "aliases": "",
                    "updated_at": _clean_text((node or {}).get("updatedAt")),
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

    output = pd.DataFrame(
        rows,
        columns=["brand_name", "brand_handle", "brand_gid", "metaobject_type", "aliases", "updated_at", "fields_json"],
    )
    if output.empty:
        output = pd.DataFrame(
            columns=["brand_name", "brand_handle", "brand_gid", "metaobject_type", "aliases", "updated_at", "fields_json"]
        )
    else:
        output = output.fillna("")
        output["brand_name"] = output["brand_name"].astype(str).str.strip()
        output["brand_handle"] = output["brand_handle"].astype(str).str.strip()
        output["brand_gid"] = output["brand_gid"].astype(str).str.strip()
        output = output.drop_duplicates(subset=["brand_gid"], keep="first")
        output = output.sort_values(by=["brand_name", "brand_handle"], kind="stable").reset_index(drop=True)
    if detect_note:
        return output, detect_note, resolved_type
    return output, None, resolved_type


def default_brand_metaobject_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_BRAND_METAOBJECT_FILE


def save_brand_metaobjects_table(df: pd.DataFrame, output_path: Path) -> tuple[int, str | None]:
    out = df.copy() if df is not None else pd.DataFrame()
    if out.empty:
        out = pd.DataFrame(
            columns=["brand_name", "brand_handle", "brand_gid", "metaobject_type", "aliases", "updated_at", "fields_json"]
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if output_path.suffix.lower() == ".xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                out.to_excel(writer, index=False, sheet_name="brands")
        else:
            out.to_csv(output_path, index=False, encoding="utf-8-sig")
    except Exception as exc:
        return 0, str(exc)
    return int(len(out)), None


def find_brand_metaobject_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "ShopifyBrandMetaobjects.csv",
        required_root / "mappings" / "shopify_brand_metaobjects.csv",
        required_root / "mappings" / "BrandMetaobjects.csv",
        required_root / "mappings" / "brand_metaobjects.csv",
        required_root / "mappings" / "ShopifyBrandMetaobjects.xlsx",
        required_root / "mappings" / "brand_metaobjects.xlsx",
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


@lru_cache(maxsize=16)
def _load_brand_gid_lookup_cached(path_text: str, mtime_ns: int, size_bytes: int) -> dict[str, str]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    table = _read_table(path)
    if table.empty:
        return {}

    columns = list(table.columns)
    normalized_columns = [_normalize_key(column).replace(" ", "_") for column in columns]

    gid_col: str | None = None
    name_col: str | None = None
    handle_col: str | None = None
    alias_cols: list[str] = []

    for index, normalized in enumerate(normalized_columns):
        col = columns[index]
        if gid_col is None and normalized in {"brand_gid", "metaobject_gid", "gid", "id", "metaobject_id"}:
            gid_col = col
        if name_col is None and normalized in {"brand_name", "display_name", "displayname", "brand", "name"}:
            name_col = col
        if handle_col is None and normalized in {"brand_handle", "handle"}:
            handle_col = col
        if normalized in {"aliases", "alias", "aka", "alternate_names", "alt_names"}:
            alias_cols.append(col)

    if gid_col is None:
        for index, normalized in enumerate(normalized_columns):
            if normalized.endswith("gid") or normalized == "id":
                gid_col = columns[index]
                break

    if name_col is None and columns:
        name_col = columns[0]
    if gid_col is None or name_col is None:
        return {}

    lookup: dict[str, str] = {}
    for _, row in table.iterrows():
        gid = _coerce_gid(_clean_text(row.get(gid_col, "")))
        if not gid:
            continue
        keys: list[str] = []
        name_value = _clean_text(row.get(name_col, ""))
        if name_value:
            keys.append(name_value)
        if handle_col:
            handle_value = _clean_text(row.get(handle_col, ""))
            if handle_value:
                keys.append(handle_value)
        for alias_col in alias_cols:
            keys.extend(_split_aliases(row.get(alias_col, "")))

        for key in keys:
            normalized = _normalize_key(key)
            if not normalized:
                continue
            lookup.setdefault(normalized, gid)
    return lookup


def load_brand_gid_lookup(required_root: Path | None) -> dict[str, str]:
    path = find_brand_metaobject_file(required_root=required_root)
    if path is None:
        return {}
    try:
        stat = path.stat()
    except Exception:
        return {}
    try:
        return _load_brand_gid_lookup_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return {}


def resolve_brand_metaobject_gid(brand_name: str, required_root: Path | None) -> str:
    direct = _coerce_gid(brand_name)
    if direct:
        return direct
    lookup = load_brand_gid_lookup(required_root=required_root)
    if not lookup:
        return ""
    key = _normalize_key(brand_name)
    if not key:
        return ""
    return _clean_text(lookup.get(key, ""))
