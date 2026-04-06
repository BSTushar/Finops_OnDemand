from __future__ import annotations
import io
import re
from dataclasses import dataclass, field
from typing import BinaryIO
import pandas as pd
from instance_api import canonicalize_instance_api_name
from os_resolve import cell_matches_valid_os_pattern
_VALUE_SAMPLE_CAP = 2000
_MIN_AUTO_CONF = 0.48
_TIE_BAND = 0.09

def _norm_header(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub('[_\\-]+', ' ', s)
    s = re.sub('\\s+', ' ', s)
    return s
INSTANCE_HINTS: frozenset[str] = frozenset({'instance type', 'instancetype', 'instance', 'instance type id', 'ec2 type', 'ec2type', 'ec2 type id', 'instance size', 'resource type', 'vm type', 'vm size', 'vmsize', 'ec2 instance type', 'instance class', 'db instance class', 'database class', 'compute class', 'computeclass', 'instance type name', 'api name', 'ec2 api name', 'instance api name'})
COST_HINTS: frozenset[str] = frozenset({'cost', 'monthly cost', 'total cost', 'charge', 'charges', 'cost ($)', 'cost(usd)', 'cost (usd)', 'cost_usd', 'billed cost', 'blended cost', 'unblended cost', 'amortized cost', 'spend', 'amount', 'total amount', 'billed amount', 'usage cost', 'line item cost', 'cost usd', 'usd cost', 'monthly spend'})

def _header_matches(h: str, hints: frozenset[str]) -> bool:
    n = _norm_header(h)
    if n in hints:
        return True
    for hint in hints:
        if len(hint) >= 4 and hint in n:
            return True
    return False
def _cell_looks_like_instance_type(cell: object) -> bool:
    if cell is None:
        return False
    try:
        if pd.isna(cell):
            return False
    except (TypeError, ValueError):
        pass
    return canonicalize_instance_api_name(cell) is not None

def _value_match_ratio(df: pd.DataFrame, col: str, predicate) -> float:
    n = min(len(df), _VALUE_SAMPLE_CAP)
    if n == 0:
        return 0.0
    ser = df[col].iloc[:n]
    mask = ser.notna()
    sstr = ser.astype(str).str.strip()
    mask &= ~sstr.str.lower().isin(('nan', 'none', 'n/a', ''))
    if not mask.any():
        return 0.0
    sub = ser[mask]
    hits = sum((1 for v in sub if predicate(v)))
    return hits / len(sub)

def _score_instance_columns(df: pd.DataFrame) -> list[tuple[str, float]]:
    scored: list[tuple[str, float]] = []
    for col in df.columns:
        hdr = _header_matches(str(col), INSTANCE_HINTS)
        vr = _value_match_ratio(df, col, _cell_looks_like_instance_type)
        if hdr and vr >= 0.25:
            sc = 0.5 + 0.5 * min(1.0, vr / 0.95)
        elif hdr:
            sc = 0.45 + 0.15 * min(1.0, vr * 3.0) if vr > 0 else 0.44
        else:
            sc = vr
        scored.append((col, min(1.0, sc)))
    scored.sort(key=lambda x: (-x[1], str(x[0])))
    return scored

def _score_os_columns(df: pd.DataFrame) -> list[tuple[str, float]]:
    """Score columns by share of cells matching allowed OS value patterns only (no header hints)."""
    scored: list[tuple[str, float]] = []
    for col in df.columns:
        vr = _value_match_ratio(df, col, cell_matches_valid_os_pattern)
        scored.append((col, min(1.0, vr)))
    scored.sort(key=lambda x: (-x[1], str(x[0])))
    return scored


_MIN_OS_AUTO_CONF = 0.36
_OS_TIE_BAND = 0.09

def _resolve_os_column(df: pd.DataFrame, scored: list[tuple[str, float]]) -> tuple[str | None, bool, list[str]]:
    all_cols = list(df.columns)
    if not scored:
        return (None, False, [])
    (best_c, best_s) = scored[0]
    second_s = scored[1][1] if len(scored) > 1 else -1.0
    if best_s < _MIN_OS_AUTO_CONF:
        return (None, False, [])
    if second_s >= best_s - _OS_TIE_BAND and second_s >= 0.3:
        tied = [c for (c, s) in scored[:12] if s >= second_s - 0.005]
        return (None, True, tied or [best_c])
    return (best_c, False, [best_c])

def _resolve_best_column(df: pd.DataFrame, scored: list[tuple[str, float]]) -> tuple[str | None, bool, list[str]]:
    all_cols = list(df.columns)
    if not scored:
        return (None, True, all_cols)
    (best_c, best_s) = scored[0]
    second_s = scored[1][1] if len(scored) > 1 else -1.0
    ui_cands = [c for (c, s) in scored if s >= 0.22]
    if len(ui_cands) < 2 and best_s >= 0.35:
        ui_cands = [c for (c, s) in scored[:min(12, len(scored))] if s > 0.15]
    if not ui_cands:
        ui_cands = all_cols
    if best_s < _MIN_AUTO_CONF:
        return (None, True, ui_cands)
    if second_s >= best_s - _TIE_BAND and second_s >= 0.42:
        tied = [c for (c, s) in scored[:12] if s >= second_s - 0.005]
        return (None, True, tied or ui_cands)
    return (best_c, False, [best_c])

def find_cost_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _header_matches(str(c), COST_HINTS)]

OS_COLUMN_NONE_OPTION: str = '— No OS column (Linux pricing for all rows) —'


@dataclass
class ColumnBinding:
    instance: str
    os: str | None = None
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

    def with_binding(self, instance: str, os: str | None, actual_cost: str | None) -> LoadResult:
        b = ColumnBinding(instance=instance, os=os, actual_cost=actual_cost)
        return LoadResult(df=self.df, warnings=self.warnings.copy(), instance_candidates=self.instance_candidates, os_candidates=self.os_candidates, cost_candidates=self.cost_candidates, binding=b, needs_instance_pick=False, needs_os_pick=False, needs_cost_pick=False)

def _parse_dataframe(raw_bytes: bytes, ext: str) -> pd.DataFrame:
    if ext in ('xlsx', 'xls', 'xlsm'):
        return pd.read_excel(io.BytesIO(raw_bytes), engine='openpyxl', dtype=object, keep_default_na=False)
    if ext == 'csv':
        for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
            try:
                return pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, dtype=object, keep_default_na=False)
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError('Could not decode CSV file (tried utf-8, latin-1, cp1252).')
    raise ValueError(f"Unsupported format '.{ext}'. Upload CSV (.csv) or Excel (.xlsx / .xls).")

def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(',', '', regex=False), errors='coerce')

def analyze_load(df: pd.DataFrame, base_warnings: list[str]) -> LoadResult:
    warnings = base_warnings[:]
    inst_scored = _score_instance_columns(df)
    os_scored = _score_os_columns(df)
    (inst_col, inst_amb, inst_ui) = _resolve_best_column(df, inst_scored)
    (os_col, os_amb, os_ui) = _resolve_os_column(df, os_scored)
    cost_c = find_cost_columns(df)
    needs_cost_pick = len(cost_c) > 1
    needs_i = inst_amb or inst_col is None
    needs_o = os_amb
    inst_c_list = list(dict.fromkeys(inst_ui if needs_i else ([inst_col] if inst_col is not None else [])))
    os_c_list = list(dict.fromkeys(os_ui if needs_o else ([os_col] if os_col is not None else [])))
    if needs_i:
        warnings.append('Instance column ambiguous or low-confidence — pick the column with AWS API Name values (e.g. m5.large, db.r5.xlarge).')
    if needs_o:
        warnings.append('Multiple columns match OS-like values — pick the one that represents OS / engine.')
    if (not needs_o) and os_col is None and (not needs_i):
        warnings.append('No OS-like column detected from cell values — pricing uses Linux for all rows (see Pricing OS column).')
    if len(cost_c) == 0:
        warnings.append('No cost/spend/amount column auto-detected — savings will be N/A without selection.')
    elif len(cost_c) > 1:
        warnings.append(f'Multiple cost-like columns found ({len(cost_c)}) — please choose Actual Cost column.')
    binding: ColumnBinding | None = None
    if not needs_i and (not needs_o) and inst_col is not None:
        binding = ColumnBinding(instance=inst_col, os=os_col, actual_cost=cost_c[0] if len(cost_c) == 1 else None)
    return LoadResult(df=df, warnings=warnings, instance_candidates=inst_c_list, os_candidates=os_c_list, cost_candidates=cost_c, binding=binding, needs_instance_pick=needs_i, needs_os_pick=needs_o, needs_cost_pick=needs_cost_pick and len(cost_c) > 1)

def _normalize_loaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Blank strings → NA without deprecated replace downcasting; keeps non-object dtypes."""
    df = df.copy()

    def _is_blank(x: object) -> bool:
        if x is None:
            return True
        try:
            if pd.isna(x):
                return True
        except (TypeError, ValueError):
            pass
        return isinstance(x, str) and x.strip() == ''

    for col in df.columns:
        s = df[col]
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue
        df[col] = s.mask(s.map(_is_blank), pd.NA)
    return df


def load_file(file_obj: BinaryIO, filename: str) -> LoadResult:
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        raw = file_obj.read() if hasattr(file_obj, 'read') else bytes(file_obj)
        df = _parse_dataframe(raw, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc
    if df.empty:
        raise ValueError('The uploaded file contains no data rows.')
    df = _normalize_loaded_dataframe(df)
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError('All rows are empty after stripping blank lines.')
    return analyze_load(df, [])

def dataframe_from_bytes(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse upload bytes to a cleaned DataFrame (no column analysis). For Fix Your Sheet merge."""
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        df = _parse_dataframe(raw_bytes, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc
    if df.empty:
        raise ValueError('The uploaded file contains no data rows.')
    df = _normalize_loaded_dataframe(df)
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError('All rows are empty after stripping blank lines.')
    return df

def finalize_binding(lr: LoadResult, instance_col: str, os_col: str | None, actual_cost_col: str | None) -> LoadResult:
    if instance_col not in lr.df.columns:
        raise ValueError('Selected column not found in file.')
    if os_col is not None and os_col not in lr.df.columns:
        raise ValueError('Selected OS column not found in file.')
    if actual_cost_col is not None and actual_cost_col not in lr.df.columns:
        raise ValueError('Selected cost column not found in file.')
    return lr.with_binding(instance_col, os_col, actual_cost_col)
