from __future__ import annotations
from collections import Counter, defaultdict
import logging
import re
import pandas as pd
from data_loader import require_unique_column_names

logger = logging.getLogger(__name__)

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

# Core id token: one or more letters followed by one or more digits.
# Boundaries avoid partial/alphanumeric-overlap matches (e.g. no "ab101" match inside "ab1011").
_CORE_ID_TOKEN_RE = re.compile(r'(?<![a-z0-9])([a-z]+[0-9]+)(?![a-z0-9])')
_FULL_CORE_RE = re.compile(r'^[a-z]+[0-9]+$')


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
    """Lowercase, strip; None if empty."""
    if _is_empty_cell(v):
        return None
    s = str(v).strip().lower()
    if s in ('nan', 'none', 'n/a', ''):
        return None
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    return s


def _extract_core_tokens(norm: str) -> list[str]:
    """All strict core-id tokens in norm (left-to-right, non-overlapping)."""
    if not norm:
        return []
    return _CORE_ID_TOKEN_RE.findall(norm)


def _canonical_core_for_key(
    nk: str | None,
    warnings: list[str],
    multi_warned: set[str],
) -> str | None:
    """
    Single derived id for matching: full value if it is already a core id;
    otherwise leftmost extracted strict token [a-z]+[0-9]+.
    """
    if not nk:
        return None
    if _FULL_CORE_RE.match(nk):
        return nk
    cores = _extract_core_tokens(nk)
    if not cores:
        return None
    if len(cores) > 1 and nk not in multi_warned:
        multi_warned.add(nk)
        warnings.append(
            f'Merge key value contains multiple extractable core ids ({", ".join(cores[:5])}'
            f'{"…" if len(cores) > 5 else ""}) — using leftmost ({cores[0]!r}) for matching.'
        )
        logger.warning(
            'Merge: multiple core ids in key value; using leftmost (value prefix omitted).'
        )
    return cores[0]


def _helper_col_name(existing_cols: list[str], base: str) -> str:
    taken = set(existing_cols)
    name = base
    while name in taken:
        name = f'_{name}_'
    return name


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

    Matching (deterministic, one secondary row per primary row at most):
    - derive normalized strict core_id from both keys:
      * lowercase + trim first
      * full token or embedded token [a-z]+[0-9]+ only
    - merge on core_id equality only (no fuzzy/partial matching).
    - if several secondary rows share same core_id, use first row by original order.
    Output row count always equals D1 row count.
    """
    warnings: list[str] = []
    multi_warned: set[str] = set()
    if key_left not in d1.columns:
        raise ValueError(f"Key column '{key_left}' not found in primary dataset.")
    if key_right not in d2.columns:
        raise ValueError(f"Key column '{key_right}' not found in secondary dataset.")
    require_unique_column_names(d1.columns)
    require_unique_column_names(d2.columns)
    d1 = d1.copy()
    d2 = d2.copy()
    d1_base_cols = list(d1.columns)
    d2_base_cols = list(d2.columns)
    d1_core_col = _helper_col_name(d1_base_cols + d2_base_cols, 'core_id')
    d2_core_col = _helper_col_name(d1_base_cols + d2_base_cols + [d1_core_col], 'core_id')
    d1[d1_core_col] = [
        _canonical_core_for_key(_norm_key_value(v), warnings, multi_warned) for v in d1[key_left]
    ]
    d2[d2_core_col] = [
        _canonical_core_for_key(_norm_key_value(v), warnings, multi_warned) for v in d2[key_right]
    ]

    cnt_d2 = Counter((x for x in d2[d2_core_col] if x is not None))
    dup_keys_d2 = {k for k, v in cnt_d2.items() if v > 1}
    n_secondary_dup_groups = len(dup_keys_d2)
    rows_in_dup_groups = sum(cnt_d2[k] for k in dup_keys_d2)
    if n_secondary_dup_groups:
        warnings.append(
            f'Secondary dataset: {n_secondary_dup_groups} core_id value(s) repeat — '
            f'{rows_in_dup_groups} rows; only the first row per core_id is merged (see FinOps_Merge_* flags).'
        )

    core_lists: dict[str, list[tuple[int, pd.Series]]] = defaultdict(list)

    for idx, (_, br) in enumerate(d2.iterrows()):
        core2 = br[d2_core_col]
        if core2 is None:
            continue
        core_lists[core2].append((idx, br))

    dup_primary_norm = {
        k
        for (k, n) in Counter((x for x in d1[d1_core_col])).items()
        if k is not None and n > 1
    }
    if dup_primary_norm:
        warnings.append(
            f'Primary dataset: {len(dup_primary_norm)} core_id value(s) repeat on multiple rows — '
            'rows are not removed; see duplicate-primary flag column.'
        )

    (fname_sec, fname_rep, fname_pp) = _flag_column_names(d1_base_cols)
    extra_cols = [c for c in d2_base_cols if c not in d1_base_cols]
    out_cols = d1_base_cols + extra_cols + [fname_sec, fname_rep, fname_pp]
    rows: list[dict] = []
    unmatched = 0

    def _pick_one_match(
        core_id: str | None,
    ) -> tuple[pd.Series | None, bool, str, bool]:
        """
        Returns (secondary_row_or_none, duplicate_secondary_suppressed, replica_label, matched_via_core_id).
        """
        if core_id is None:
            return (None, False, '', False)
        if core_id not in core_lists:
            return (None, False, '', False)
        cands = sorted(core_lists[core_id], key=lambda t: t[0])
        chosen = cands[0]
        if len(cands) > 1:
            warnings.append(
                f'Multiple secondary rows share core_id {core_id!r} — using first row (index {chosen[0]}).'
            )
            logger.warning('Merge: duplicate secondary core_id values; using first row.')
        return (chosen[1], len(cands) > 1, f'1/{len(cands)}', True)

    for _, r1 in d1.iterrows():
        core1 = r1[d1_core_col]
        primary_dup = bool(core1 is not None and core1 in dup_primary_norm)
        r2, sec_multi, rep_label, _via_core = _pick_one_match(core1)
        if core1 is not None and r2 is None:
            unmatched += 1

        def _emit_one(r2b: pd.Series | None, sec_m: bool, rep_lbl: str) -> None:
            row: dict = {}
            for c in d1_base_cols:
                v1 = r1[c]
                if not _is_empty_cell(v1):
                    row[c] = v1
                elif r2b is not None and c in d2.columns:
                    row[c] = r2b[c]
                else:
                    row[c] = v1
            for c in extra_cols:
                row[c] = r2b[c] if r2b is not None else pd.NA
            row[fname_sec] = 'Yes' if sec_m else 'No'
            row[fname_rep] = rep_lbl
            row[fname_pp] = 'Yes' if primary_dup else 'No'
            rows.append(row)

        _emit_one(r2, sec_multi, rep_label)

    if unmatched:
        warnings.append(f'Primary rows with no secondary match on core_id: {unmatched} (extra columns left blank).')

    out = pd.DataFrame(rows, columns=out_cols)
    return (out, warnings)
