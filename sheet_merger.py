from __future__ import annotations
from collections import Counter, defaultdict
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

FLAG_DUP_SECONDARY = 'FinOps_Merge_DuplicateSecondaryRows'
FLAG_SECONDARY_REPLICA = 'FinOps_Merge_SecondaryRowGroupIndex'
FLAG_DUP_PRIMARY_KEY = 'FinOps_Merge_DuplicatePrimaryKey'


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
    return s.lower()


def _fuzzy_key_match(short: str, long: str) -> bool:
    """True if long embeds short as a merge key suffix / token (short is the shorter string)."""
    if len(short) < 4 or len(long) < len(short):
        return False
    if long.endswith(short):
        return True
    for sep in ('_', '-', '.'):
        if f'{sep}{short}' in long:
            return True
    if len(short) >= 5 and short in long:
        return True
    return False


def _fuzzy_keys_match(a: str, b: str) -> bool:
    """Case-normalized keys already; false when identical (caller uses exact map first)."""
    if a == b:
        return False
    if len(a) < 4 or len(b) < 4:
        return False
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    return _fuzzy_key_match(short, long)


def _flag_column_names(d1_columns: list) -> tuple[str, str, str]:
    """Avoid clashing with existing D1 / D2 names."""
    taken = set(d1_columns)
    out: list[str] = []
    for base in (FLAG_DUP_SECONDARY, FLAG_SECONDARY_REPLICA, FLAG_DUP_PRIMARY_KEY):
        name = base
        while name in taken or name in out:
            name = f'{name}_'
        out.append(name)
    return (out[0], out[1], out[2])


def merge_primary_with_secondary(
    d1: pd.DataFrame,
    d2: pd.DataFrame,
    key_left: str,
    key_right: str,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Build D3: all D1 columns in order, then columns only in D2, then merge flag columns.
    For each row of D1, match D2 on normalized key. Multiple secondary rows with the same key
    are all kept (one output row per secondary match). Duplicate primary keys are flagged only.
    If no exact match, try fuzzy match when the shorter key appears in the longer string.
    Shared columns: keep D1 value unless empty, then D2.
    """
    warnings: list[str] = []
    if key_left not in d1.columns:
        raise ValueError(f"Key column '{key_left}' not found in primary dataset.")
    if key_right not in d2.columns:
        raise ValueError(f"Key column '{key_right}' not found in secondary dataset.")
    d1 = d1.copy()
    d2 = d2.copy()

    cnt_d2 = Counter((_norm_key_value(x) for x in d2[key_right]))
    dup_keys_d2 = {k for k, v in cnt_d2.items() if k is not None and v > 1}
    n_secondary_dup_groups = len(dup_keys_d2)
    rows_in_dup_groups = sum(cnt_d2[k] for k in dup_keys_d2)
    if n_secondary_dup_groups:
        warnings.append(
            f'Secondary dataset: {n_secondary_dup_groups} merge key value(s) repeat — all {rows_in_dup_groups} '
            'secondary rows are kept; see FinOps_Merge_* flag columns (output may have more rows than primary).'
        )

    exact_lists: dict[str, list[pd.Series]] = defaultdict(list)
    for _, r in d2.iterrows():
        nk2 = _norm_key_value(r[key_right])
        if nk2 is not None:
            exact_lists[nk2].append(r)

    def _fuzzy_matches(primary_nk: str) -> list[pd.Series]:
        out: list[pd.Series] = []
        for _, br in d2.iterrows():
            brk = _norm_key_value(br[key_right])
            if brk is None:
                continue
            if _fuzzy_keys_match(primary_nk, brk):
                out.append(br)
        return out

    dup_primary_norm = {
        k
        for (k, n) in Counter((_norm_key_value(x) for x in d1[key_left])).items()
        if k is not None and n > 1
    }
    if dup_primary_norm:
        warnings.append(
            f'Primary dataset: {len(dup_primary_norm)} merge key value(s) repeat on multiple rows — '
            'rows are not removed; see duplicate-primary flag column.'
        )

    (fname_sec, fname_rep, fname_pp) = _flag_column_names(list(d1.columns))
    extra_cols = [c for c in d2.columns if c not in d1.columns]
    out_cols = list(d1.columns) + extra_cols + [fname_sec, fname_rep, fname_pp]
    rows: list[dict] = []
    unmatched = 0
    fuzzy_primary_hits = 0

    for _, r1 in d1.iterrows():
        nk = _norm_key_value(r1[key_left])
        primary_dup = bool(nk is not None and nk in dup_primary_norm)
        matches = list(exact_lists.get(nk, [])) if nk is not None else []
        if nk is not None and not matches:
            matches = _fuzzy_matches(nk)
            if matches:
                fuzzy_primary_hits += 1

        if nk is not None and not matches:
            unmatched += 1

        def _emit_one(r2: pd.Series | None, sec_multi: bool, rep_label: str) -> None:
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
            row[fname_sec] = 'Yes' if sec_multi else 'No'
            row[fname_rep] = rep_label
            row[fname_pp] = 'Yes' if primary_dup else 'No'
            rows.append(row)

        if not matches:
            _emit_one(None, False, '')
        elif len(matches) == 1:
            _emit_one(matches[0], False, '')
        else:
            for j, r2 in enumerate(matches):
                _emit_one(r2, True, f'{j + 1}/{len(matches)}')

    if fuzzy_primary_hits:
        warnings.append(
            f'Matched {fuzzy_primary_hits} primary row(s) using fuzzy key '
            '(short code inside longer secondary key); verify joins are correct.'
        )
    if unmatched:
        warnings.append(f'Primary rows with no secondary match on key: {unmatched} (extra columns left blank).')
    out = pd.DataFrame(rows, columns=out_cols)
    return (out, warnings)
