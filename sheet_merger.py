from __future__ import annotations
import re
import pandas as pd

MERGE_KEY_HINTS: frozenset[str] = frozenset(
    {
        'id',
        'resource id',
        'resourceid',
        'instance id',
        'instanceid',
        'vm id',
        'vmid',
        'asset id',
        'assetid',
        'line item id',
        'lineitemid',
        'resource identifier',
        'arn',
        'uuid',
        'guid',
        'name',
        'hostname',
        'host name',
        'resource name',
    }
)


def _norm_header(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub('[_\\-]+', ' ', s)
    s = re.sub('\\s+', ' ', s)
    return s


def column_looks_like_merge_key(col_name: str) -> bool:
    n = _norm_header(col_name)
    if n in MERGE_KEY_HINTS:
        return True
    for hint in MERGE_KEY_HINTS:
        if len(hint) >= 3 and hint in n:
            return True
    return False


def suggest_key_pairs(cols1: list[str], cols2: list[str]) -> list[tuple[str, str]]:
    """Ordered suggestions (key in D1, key in D2). Same-name keys first, then cross-name key-like columns."""
    s2 = set(cols2)
    common = [c for c in cols1 if c in s2]
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for c in common:
        if column_looks_like_merge_key(c):
            t = (c, c)
            if t not in seen:
                out.append(t)
                seen.add(t)
    for c in common:
        t = (c, c)
        if t not in seen:
            out.append(t)
            seen.add(t)
    k1 = [c for c in cols1 if column_looks_like_merge_key(c)]
    k2 = [c for c in cols2 if column_looks_like_merge_key(c)]
    for a in k1:
        for b in k2:
            t = (a, b)
            if t not in seen:
                out.append(t)
                seen.add(t)
    return out


def _is_empty_cell(v: object) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(v, str) and (not v.strip() or v.strip().lower() in ('nan', 'none', 'n/a')):
        return True
    return False


def _norm_key_value(v: object) -> str | None:
    if _is_empty_cell(v):
        return None
    s = str(v).strip()
    if s.lower() in ('nan', 'none', 'n/a', ''):
        return None
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    return s


def merge_primary_with_secondary(
    d1: pd.DataFrame,
    d2: pd.DataFrame,
    key_left: str,
    key_right: str,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Build D3: all D1 columns in order, then columns only in D2.
    For each row of D1, match D2 on normalized key; duplicate D2 keys use first row.
    Shared columns: keep D1 value unless empty, then D2.
    """
    warnings: list[str] = []
    if key_left not in d1.columns:
        raise ValueError(f"Key column '{key_left}' not found in primary dataset.")
    if key_right not in d2.columns:
        raise ValueError(f"Key column '{key_right}' not found in secondary dataset.")
    d1 = d1.copy()
    d2 = d2.copy()
    dup_drop = int(d2.duplicated(subset=[key_right], keep='first').sum())
    if dup_drop:
        warnings.append(f'Secondary dataset: dropped {dup_drop} duplicate key row(s) (kept first).')
    d2u = d2.drop_duplicates(subset=[key_right], keep='first')
    lookup: dict[str, pd.Series] = {}
    for _, r in d2u.iterrows():
        nk = _norm_key_value(r[key_right])
        if nk is not None and nk not in lookup:
            lookup[nk] = r
    extra_cols = [c for c in d2.columns if c not in d1.columns]
    out_cols = list(d1.columns) + extra_cols
    rows: list[dict] = []
    unmatched = 0
    for _, r1 in d1.iterrows():
        nk = _norm_key_value(r1[key_left])
        r2 = lookup.get(nk) if nk is not None else None
        if nk is not None and r2 is None:
            unmatched += 1
        row: dict = {}
        for c in d1.columns:
            v1 = r1[c]
            if not _is_empty_cell(v1):
                row[c] = v1
            elif r2 is not None and c in d2.columns:
                row[c] = r2[c]
            else:
                row[c] = v1
        for c in extra_cols:
            row[c] = r2[c] if r2 is not None else pd.NA
        rows.append(row)
    if unmatched:
        warnings.append(f'Primary rows with no secondary match on key: {unmatched} (extra columns left blank).')
    out = pd.DataFrame(rows, columns=out_cols)
    return (out, warnings)
