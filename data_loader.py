"""
data_loader.py  ·  FinOps Optimizer
==================================
Preserves ALL original column names, order, and cell values (zero mutation).

Detection uses normalized header names only; physical columns are never renamed
for processing output — bindings point to original names.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import BinaryIO

import pandas as pd


def _norm_header(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


# ── Alias sets: normalized header → True if matches semantic ──────────────
INSTANCE_HINTS: frozenset[str] = frozenset({
    "instance type", "instancetype", "instance_type", "instance",
    "ec2 type", "ec2type", "ec2_type", "type", "instance size",
    "resource type", "vm type", "ec2 instance type", "instance class",
    "db instance class", "database class",
})

OS_HINTS: frozenset[str] = frozenset({
    "os", "o/s", "operating system", "operating_system", "platform",
    "os type", "os_type", "system", "engine", "database engine",
})

# cost / spend / amount — dynamic actual-cost detection
COST_HINTS: frozenset[str] = frozenset({
    "cost", "monthly cost", "total cost", "charge", "charges",
    "cost ($)", "cost(usd)", "cost (usd)", "cost_usd", "billed cost",
    "blended cost", "unblended cost", "amortized cost", "spend",
    "amount", "total amount", "billed amount", "usage cost",
    "line item cost", "cost usd", "usd cost", "monthly spend",
})


def _header_matches(h: str, hints: frozenset[str]) -> bool:
    n = _norm_header(h)
    if n in hints:
        return True
    # substring for tags like "tag:cost"
    for hint in hints:
        if len(hint) >= 4 and hint in n:
            return True
    return False


def find_instance_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _header_matches(str(c), INSTANCE_HINTS)]


def find_os_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _header_matches(str(c), OS_HINTS)]


def find_cost_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _header_matches(str(c), COST_HINTS)]


@dataclass
class ColumnBinding:
    """Maps semantics to physical column names (original headers)."""
    instance: str
    os: str
    actual_cost: str | None = None


@dataclass
class LoadResult:
    df: pd.DataFrame
    warnings: list[str] = field(default_factory=list)
    instance_candidates: list[str] = field(default_factory=list)
    os_candidates: list[str] = field(default_factory=list)
    cost_candidates: list[str] = field(default_factory=list)
    binding: ColumnBinding | None = None
    needs_instance_pick: bool = False
    needs_os_pick: bool = False
    needs_cost_pick: bool = False

    @property
    def needs_manual_mapping(self) -> bool:
        return self.needs_instance_pick or self.needs_os_pick

    def with_binding(
        self,
        instance: str,
        os: str,
        actual_cost: str | None,
    ) -> LoadResult:
        b = ColumnBinding(instance=instance, os=os, actual_cost=actual_cost)
        return LoadResult(
            df=self.df,
            warnings=self.warnings.copy(),
            instance_candidates=self.instance_candidates,
            os_candidates=self.os_candidates,
            cost_candidates=self.cost_candidates,
            binding=b,
            needs_instance_pick=False,
            needs_os_pick=False,
            needs_cost_pick=False,
        )


def _parse_dataframe(raw_bytes: bytes, ext: str) -> pd.DataFrame:
    if ext in ("xlsx", "xls", "xlsm"):
        return pd.read_excel(
            io.BytesIO(raw_bytes), engine="openpyxl", dtype=object, keep_default_na=False
        )
    if ext == "csv":
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return pd.read_csv(
                    io.BytesIO(raw_bytes), encoding=enc, dtype=object, keep_default_na=False
                )
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError("Could not decode CSV file (tried utf-8, latin-1, cp1252).")
    raise ValueError(
        f"Unsupported format '.{ext}'. Upload CSV (.csv) or Excel (.xlsx / .xls)."
    )


def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def analyze_load(df: pd.DataFrame, base_warnings: list[str]) -> LoadResult:
    """Detect columns; never rename or reorder df."""
    warnings = base_warnings[:]
    inst_c = find_instance_columns(df)
    os_c = find_os_columns(df)
    cost_c = find_cost_columns(df)

    needs_i = len(inst_c) != 1
    needs_o = len(os_c) != 1
    needs_cost_pick = len(cost_c) > 1

    if len(inst_c) == 0:
        warnings.append("No instance column auto-detected — manual selection required.")
    if len(os_c) == 0:
        warnings.append("No OS/engine column auto-detected — manual selection required.")
    if len(cost_c) == 0:
        warnings.append(
            "No cost/spend/amount column auto-detected — savings will be N/A without selection."
        )
    elif len(cost_c) == 1:
        pass  # single candidate OK
    else:
        warnings.append(
            f"Multiple cost-like columns found ({len(cost_c)}) — please choose Actual Cost column."
        )

    binding: ColumnBinding | None = None
    if not needs_i and not needs_o:
        binding = ColumnBinding(
            instance=inst_c[0],
            os=os_c[0],
            actual_cost=cost_c[0] if len(cost_c) == 1 else None,
        )

    return LoadResult(
        df=df,
        warnings=warnings,
        instance_candidates=inst_c,
        os_candidates=os_c,
        cost_candidates=cost_c,
        binding=binding,
        needs_instance_pick=needs_i,
        needs_os_pick=needs_o,
        needs_cost_pick=needs_cost_pick and len(cost_c) > 1,
    )


def load_file(file_obj: BinaryIO, filename: str) -> LoadResult:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    try:
        raw = file_obj.read() if hasattr(file_obj, "read") else bytes(file_obj)
        df = _parse_dataframe(raw, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc

    if df.empty:
        raise ValueError("The uploaded file contains no data rows.")

    df = df.copy()
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("All rows are empty after stripping blank lines.")

    # Preserve object dtype; no column renames
    return analyze_load(df, [])


def finalize_binding(
    lr: LoadResult,
    instance_col: str,
    os_col: str,
    actual_cost_col: str | None,
) -> LoadResult:
    """Apply user-confirmed bindings; df still unchanged."""
    if instance_col not in lr.df.columns or os_col not in lr.df.columns:
        raise ValueError("Selected column not found in file.")
    if actual_cost_col is not None and actual_cost_col not in lr.df.columns:
        raise ValueError("Selected cost column not found in file.")
    return lr.with_binding(instance_col, os_col, actual_cost_col)
