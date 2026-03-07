from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

from product_prospector.core.mapping import FIELD_ALIASES, normalize_header

try:
    import pyxlsb  # noqa: F401

    _HAS_PYXLSB = True
except Exception:
    _HAS_PYXLSB = False


_MAX_HEADER_SCAN_ROWS = 35
_EXTRA_HEADER_ALIASES = [
    "vendor",
    "manufacturer",
    "brand",
    "weight",
    "lbs",
    "pounds",
]
_HEADER_ALIASES = sorted(
    {
        normalize_header(alias)
        for aliases in FIELD_ALIASES.values()
        for alias in aliases
        if normalize_header(alias)
    }.union({normalize_header(alias) for alias in _EXTRA_HEADER_ALIASES if normalize_header(alias)})
)
_HEADER_TOKENS = {
    token
    for alias in _HEADER_ALIASES
    for token in alias.split()
    if token and len(token) >= 3
}
_STRONG_HEADER_TOKENS = {
    "sku",
    "mpn",
    "title",
    "name",
    "description",
    "price",
    "msrp",
    "map",
    "jobber",
    "dealer",
    "cost",
    "barcode",
    "upc",
    "ean",
    "fitment",
    "application",
    "image",
    "media",
    "weight",
    "core",
}


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _looks_numeric(value: str) -> bool:
    compact = value.replace(",", "").replace("$", "").replace("%", "").strip()
    if not compact:
        return False
    return compact.replace(".", "", 1).isdigit()


def _score_header_row(row: pd.Series) -> tuple[float, int, int, int]:
    non_empty = [_cell_text(item) for item in row.tolist() if _cell_text(item)]
    if not non_empty:
        return 0.0, 0, 0, 0

    exact_hits = 0
    contains_hits = 0
    token_hits = 0
    strong_hits = 0
    numeric_cells = 0
    long_cells = 0
    seen_norm: set[str] = set()

    for value in non_empty:
        if _looks_numeric(value):
            numeric_cells += 1
        if len(value) >= 52:
            long_cells += 1
        normalized = normalize_header(value)
        if not normalized:
            continue
        seen_norm.add(normalized)
        if normalized in _HEADER_ALIASES:
            exact_hits += 1
            continue
        if any(alias in normalized for alias in _HEADER_ALIASES if len(alias) >= 4):
            contains_hits += 1
        tokens = normalized.split()
        if any(token in _HEADER_TOKENS for token in tokens):
            token_hits += 1
        if any(token in _STRONG_HEADER_TOKENS for token in tokens):
            strong_hits += 1

    unique_ratio = len(seen_norm) / max(len(non_empty), 1)
    numeric_ratio = numeric_cells / max(len(non_empty), 1)
    long_ratio = long_cells / max(len(non_empty), 1)
    match_hits = exact_hits + contains_hits + token_hits
    score = (
        (exact_hits * 3.0)
        + (contains_hits * 1.5)
        + (token_hits * 1.0)
        + (strong_hits * 2.2)
        + min(len(non_empty), 12) * 0.12
        + (unique_ratio * 0.5)
        - (numeric_ratio * 2.0)
        - (long_ratio * 1.5)
    )
    return score, match_hits, len(non_empty), strong_hits


def _pick_header_row(raw_df: pd.DataFrame) -> int | None:
    if raw_df.empty:
        return None

    scan_rows = min(len(raw_df.index), _MAX_HEADER_SCAN_ROWS)
    best_idx: int | None = None
    best_score = -1.0
    best_hits = 0
    best_non_empty = 0
    best_strong_hits = 0

    for idx in range(scan_rows):
        score, hits, non_empty, strong_hits = _score_header_row(raw_df.iloc[idx])
        if non_empty == 0:
            continue
        if score > best_score or (score == best_score and strong_hits > best_strong_hits):
            best_idx = idx
            best_score = score
            best_hits = hits
            best_non_empty = non_empty
            best_strong_hits = strong_hits

    if best_idx is None:
        return None

    if best_strong_hits >= 2 and best_score >= 2.5:
        return best_idx
    if best_strong_hits >= 1 and best_non_empty >= 4 and best_score >= 3.5:
        return best_idx
    if best_hits >= 2 and best_score >= 3.0:
        return best_idx
    if best_hits >= 1 and best_non_empty >= 3 and best_score >= 4.5:
        return best_idx
    return None


def _unique_headers(values: list[object], column_count: int) -> list[str]:
    headers: list[str] = []
    used: set[str] = set()
    for index in range(column_count):
        base = _cell_text(values[index] if index < len(values) else "")
        if not base:
            base = f"column_{index + 1}"
        header = base
        suffix = 2
        while header in used:
            header = f"{base}_{suffix}"
            suffix += 1
        used.add(header)
        headers.append(header)
    return headers


def _drop_empty_rows_and_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or (df.empty and len(df.columns) == 0):
        return pd.DataFrame()

    text_df = df.apply(lambda column: column.map(_cell_text))
    row_has_value = text_df.apply(lambda row: any(bool(item) for item in row), axis=1)
    out = text_df.loc[row_has_value].reset_index(drop=True)
    if out.empty:
        return out

    keep_columns = [column for column in out.columns if out[column].astype(str).str.strip().ne("").any()]
    if not keep_columns:
        return out
    return out.loc[:, keep_columns]


def _normalize_raw_table(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or len(raw_df.columns) == 0:
        return pd.DataFrame()

    cleaned = raw_df.apply(lambda column: column.map(_cell_text))
    header_idx = _pick_header_row(cleaned)
    if header_idx is None:
        generated_headers = [f"column_{index + 1}" for index in range(len(cleaned.columns))]
        data_df = cleaned.copy().reset_index(drop=True)
        data_df.columns = generated_headers
        return _drop_empty_rows_and_columns(data_df)

    header_values = cleaned.iloc[header_idx].tolist()
    headers = _unique_headers(header_values, len(cleaned.columns))
    data_df = cleaned.iloc[header_idx + 1 :].copy().reset_index(drop=True)
    data_df.columns = headers
    return _drop_empty_rows_and_columns(data_df)


def _read_csv_raw_from_bytes(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw), header=None, dtype=str, keep_default_na=False)
    if len(df.columns) == 1 and not df.empty:
        first_cell = _cell_text(df.iloc[0, 0])
        if any(token in first_cell for token in [";", "|", "\t"]):
            auto_df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", header=None, dtype=str, keep_default_na=False)
            if len(auto_df.columns) >= len(df.columns):
                return auto_df
    return df


def _read_csv_raw_from_path(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path, header=None, dtype=str, keep_default_na=False)
    if len(df.columns) == 1 and not df.empty:
        first_cell = _cell_text(df.iloc[0, 0])
        if any(token in first_cell for token in [";", "|", "\t"]):
            auto_df = pd.read_csv(file_path, sep=None, engine="python", header=None, dtype=str, keep_default_na=False)
            if len(auto_df.columns) >= len(df.columns):
                return auto_df
    return df


def _pick_best_excel_sheet(all_sheets: dict[object, pd.DataFrame]) -> pd.DataFrame:
    best: pd.DataFrame | None = None
    best_key = (-1, -1.0, -1, -1)
    fallback: pd.DataFrame | None = None

    for _, sheet_df in all_sheets.items():
        if sheet_df is None:
            continue
        normalized = _normalize_raw_table(sheet_df)
        if fallback is None and (not normalized.empty or len(normalized.columns) > 0):
            fallback = normalized
        if len(sheet_df.columns) == 0:
            continue
        raw_cleaned = sheet_df.apply(lambda column: column.map(_cell_text))
        header_idx = _pick_header_row(raw_cleaned)
        header_found = 1 if header_idx is not None else 0
        score = 0.0
        if header_idx is not None:
            score, _, _, _ = _score_header_row(raw_cleaned.iloc[header_idx])

        key = (header_found, score, len(normalized.index), len(normalized.columns))
        if key > best_key:
            best_key = key
            best = normalized

    if best is not None:
        return best
    return fallback if fallback is not None else pd.DataFrame()


def _read_excel_sheets_from_bytes(raw: bytes, suffix: str) -> dict[object, pd.DataFrame]:
    kwargs = {
        "sheet_name": None,
        "header": None,
        "dtype": str,
        "keep_default_na": False,
    }
    if suffix == ".xlsb":
        if not _HAS_PYXLSB:
            raise ValueError("Reading .xlsb requires 'pyxlsb'. Install with: pip install pyxlsb")
        try:
            return pd.read_excel(io.BytesIO(raw), engine="pyxlsb", **kwargs)
        except ImportError as exc:
            raise ValueError("Reading .xlsb requires 'pyxlsb'. Install with: pip install pyxlsb") from exc
    return pd.read_excel(io.BytesIO(raw), **kwargs)


def _read_excel_sheets_from_path(file_path: Path, suffix: str) -> dict[object, pd.DataFrame]:
    kwargs = {
        "sheet_name": None,
        "header": None,
        "dtype": str,
        "keep_default_na": False,
    }
    if suffix == ".xlsb":
        if not _HAS_PYXLSB:
            raise ValueError("Reading .xlsb requires 'pyxlsb'. Install with: pip install pyxlsb")
        try:
            return pd.read_excel(file_path, engine="pyxlsb", **kwargs)
        except ImportError as exc:
            raise ValueError("Reading .xlsb requires 'pyxlsb'. Install with: pip install pyxlsb") from exc
    return pd.read_excel(file_path, **kwargs)


def read_table_from_upload(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    raw = uploaded_file.getvalue()

    if name.endswith(".csv"):
        return _normalize_raw_table(_read_csv_raw_from_bytes(raw))
    if name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".xlsb"):
        suffix = ".xlsb" if name.endswith(".xlsb") else (".xlsx" if name.endswith(".xlsx") else ".xls")
        all_sheets = _read_excel_sheets_from_bytes(raw, suffix=suffix)
        return _pick_best_excel_sheet(all_sheets)
    raise ValueError("Unsupported file type. Upload .csv, .xlsx, .xls, or .xlsb.")


def read_table_from_path(path: str) -> pd.DataFrame:
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return _normalize_raw_table(_read_csv_raw_from_path(file_path))
    if suffix in {".xlsx", ".xls", ".xlsb"}:
        all_sheets = _read_excel_sheets_from_path(file_path, suffix=suffix)
        if not all_sheets:
            return pd.DataFrame()
        return _pick_best_excel_sheet(all_sheets)
    raise ValueError("Unsupported file type. Use .csv, .xlsx, .xls, or .xlsb.")


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")
