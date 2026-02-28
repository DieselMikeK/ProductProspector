from __future__ import annotations

import io
from pathlib import Path

import pandas as pd


def read_table_from_upload(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    raw = uploaded_file.getvalue()

    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(raw), dtype=str, keep_default_na=False)
    raise ValueError("Unsupported file type. Upload CSV or XLSX.")


def read_table_from_path(path: str) -> pd.DataFrame:
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        if len(df.columns) == 1:
            header_text = str(df.columns[0])
            if any(token in header_text for token in [";", "|", "\t"]):
                auto_df = pd.read_csv(file_path, sep=None, engine="python", dtype=str, keep_default_na=False)
                if len(auto_df.columns) >= len(df.columns):
                    return auto_df
        return df
    if suffix in {".xlsx", ".xls"}:
        all_sheets = pd.read_excel(file_path, sheet_name=None, dtype=str, keep_default_na=False)
        if not all_sheets:
            return pd.DataFrame()
        for _, sheet_df in all_sheets.items():
            if sheet_df is None:
                continue
            if (not sheet_df.empty) or len(sheet_df.columns) > 0:
                return sheet_df
        return next(iter(all_sheets.values()))
    raise ValueError("Unsupported file type. Use .csv or .xlsx.")


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")
