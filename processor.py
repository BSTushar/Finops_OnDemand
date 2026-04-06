from __future__ import annotations
import logging
import os
import math
import re
from decimal import Decimal
from typing import Literal
import pandas as pd
from data_loader import ColumnBinding, require_unique_column_names
from instance_api import canonicalize_instance_api_name
from os_resolve import cell_matches_valid_os_pattern, classify_os_kind
from pricing_engine import DEFAULT_REGION, PRICING_LOOKUP_REGION, get_price, get_rds_hourly
from pricing_normalize import LINUX_FALLBACK_LABEL, normalize_instance_string, normalize_os_engine_key, normalize_pricing_os_label
from recommender import CPUFilterMode, get_recommendations
from rds_recommender import get_rds_recommendations
logger = logging.getLogger(__name__)
ServiceMode = Literal['ec2', 'rds', 'both']
INSERT_COLS: list[str] = [
    'Pricing OS',
    'Actual Cost ($)',
    'Current Price ($/hr)',
    'Alt1 Instance',
    'Alt1 Price ($/hr)',
    'Alt1 Savings %',
    'Alt2 Instance',
    'Alt2 Price ($/hr)',
    'Alt2 Savings %',
]
NA = 'N/A'
NO_SAVINGS = 'No Savings'
ALT2_NO_DISTINCT = 'N/A (No distinct alternative)'

def _first_col_index(cols: list, name: str) -> int:
    for i, c in enumerate(cols):
        if c == name:
            return i
    raise ValueError(f'Column {name!r} not found.')


def _nonempty_cell(v: object) -> bool:
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    s = str(v).strip().lower()
    return bool(s) and s not in ('nan', 'n/a', 'none', '')


def _raw_os_cell_for_row(
    work: pd.DataFrame,
    row_i: int,
    cols: list,
    ins_idx: int,
    cc_idx: int | None,
    os_idx: int | None,
) -> object | None:
    """Use bound OS column when populated; otherwise scan row cells (value-based) for OS — e.g. Product."""
    if os_idx is not None:
        v = work.iat[row_i, os_idx]
        if _nonempty_cell(v):
            return v
    for j in range(len(cols)):
        if j == ins_idx:
            continue
        if cc_idx is not None and j == cc_idx:
            continue
        v = work.iat[row_i, j]
        if not _nonempty_cell(v):
            continue
        if cell_matches_valid_os_pattern(v) or classify_os_kind(v) is not None:
            return v
    return None


def _row_matches_service(inst: str, service: ServiceMode) -> bool:
    s = str(inst).strip().lower()
    if not s or s in ('nan', 'none'):
        return False
    is_rds = s.startswith('db.')
    if service == 'both':
        return True
    return is_rds if service == 'rds' else not is_rds

def _row_price_service(inst: str, mode: ServiceMode) -> Literal['ec2', 'rds']:
    if mode == 'rds':
        return 'rds'
    if mode == 'ec2':
        return 'ec2'
    return 'rds' if str(inst).strip().lower().startswith('db.') else 'ec2'

def _hourly_cur(inst: str, os_engine: str, backend: Literal['ec2', 'rds']) -> float | None:
    """Always use PRICING_LOOKUP_REGION (eu-west-1) for on-demand hourly SKUs."""
    inst_key = normalize_instance_string(inst)
    if not inst_key:
        return None
    if backend == 'rds':
        return get_rds_hourly(inst_key, region=PRICING_LOOKUP_REGION, os=os_engine)
    return get_price(inst_key, region=PRICING_LOOKUP_REGION, os=os_engine)


def _hourly_alt(alt: str | None, os_engine: str, backend: Literal['ec2', 'rds']) -> float | None:
    if not alt or not isinstance(alt, str):
        return None
    a = str(alt).strip()
    if a in (NA, ALT2_NO_DISTINCT) or not a or a.lower() in ('nan', 'none'):
        return None
    return _hourly_cur(normalize_instance_string(alt), os_engine, backend)


def _savings_from_hourly(current_hr: float | None, alt_hr: float | None) -> float | str:
    """Savings % from hourly list prices only; missing price → N/A; no hourly discount → No Savings."""
    if current_hr is None or alt_hr is None:
        return NA
    if not math.isfinite(current_hr) or not math.isfinite(alt_hr) or current_hr <= 0:
        return NA
    if alt_hr >= current_hr:
        return NO_SAVINGS
    pct = round((current_hr - alt_hr) / current_hr * 100, 1)
    return max(0.0, pct)

def _to_float(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, Decimal):
        try:
            x = float(v)
            return x if math.isfinite(x) else None
        except (ArithmeticError, ValueError, TypeError):
            return None
    if hasattr(pd, 'Timestamp') and isinstance(v, pd.Timestamp):
        return None
    if hasattr(v, 'item') and not isinstance(v, (bytes, str)):
        try:
            v = v.item()
        except (AttributeError, ValueError):
            pass
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ('nan', 'n/a', '-', ''):
            return None
        s = re.sub(r'^[\$€£]\s*', '', s)
        s = s.replace(',', '')
        s = s.strip()
        if not s:
            return None
        try:
            x = float(s)
        except ValueError:
            return None
        if pd.isna(x) or not math.isfinite(x):
            return None
        return x
    try:
        x = float(v)
        if pd.isna(x) or not math.isfinite(x):
            return None
        return x
    except (TypeError, ValueError):
        return None

def process(df: pd.DataFrame, binding: ColumnBinding, region: str=DEFAULT_REGION, service: ServiceMode='both', cpu_filter: CPUFilterMode='both') -> pd.DataFrame:
    cols = list(df.columns)
    require_unique_column_names(cols)
    _reserved = frozenset(INSERT_COLS)
    _bad = [c for c in cols if c in _reserved]
    if _bad:
        raise ValueError(f'Input contains reserved enrichment column name(s): {sorted(set(_bad))!r}. Rename in source and re-upload to preserve data integrity.')
    if not any(c == binding.instance for c in cols):
        raise ValueError('Binding columns missing from DataFrame.')
    if binding.os is not None and not any(c == binding.os for c in cols):
        raise ValueError('Binding OS column missing from DataFrame.')
    try:
        ins_idx = _first_col_index(cols, binding.instance)
    except ValueError as exc:
        raise ValueError('Instance column not in DataFrame.') from exc
    work = df.copy()
    _finops_debug = os.environ.get('FINOPS_DEBUG', '').strip().lower() in ('1', 'true', 'yes')
    cc = binding.actual_cost
    cc_idx: int | None = _first_col_index(cols, cc) if (cc and any(c == cc for c in cols)) else None
    os_idx = _first_col_index(cols, binding.os) if binding.os is not None else None
    if _finops_debug:
        logger.info(
            'FinOps enrichment (debug): rows=%s region=%s service=%s',
            len(work),
            region,
            service,
        )
    if work.empty:
        left = work.iloc[:, : ins_idx + 1].copy()
        right = work.iloc[:, ins_idx + 1 :].copy()
        mid = pd.DataFrame({c: pd.Series(index=work.index, dtype=object) for c in INSERT_COLS})
        return pd.concat([left, mid, right], axis=1)
    n = len(work)
    actual_vals: list[float | None] = []
    if cc_idx is not None:
        raw_a = work.iloc[:, cc_idx].tolist()
        actual_vals = [_to_float(x) for x in raw_a]
    else:
        actual_vals = [None] * n
    inst_series = work.iloc[:, ins_idx]
    cur_p: list = [None] * n
    a1i: list = [None] * n
    a1p: list = [None] * n
    a1s: list = [None] * n
    a2i: list = [None] * n
    a2p: list = [None] * n
    a2s: list = [None] * n
    act_out: list = [None] * n
    pricing_os_out: list[str] = [LINUX_FALLBACK_LABEL] * n
    cpu: CPUFilterMode = cpu_filter if cpu_filter in ('default', 'intel', 'graviton', 'both') else 'both'
    row_na_fallback_count = 0
    _price_region = PRICING_LOOKUP_REGION
    assert _price_region == 'eu-west-1', 'Hourly pricing must use eu-west-1 bundled SKU table'
    for i in range(n):
        raw_inst = inst_series.iloc[i]
        raw_inst_norm = normalize_instance_string(raw_inst)
        raw_os_cell = _raw_os_cell_for_row(work, i, cols, ins_idx, cc_idx, os_idx)
        act = actual_vals[i]
        if act is not None and act <= 0:
            act = None
        act_out[i] = act
        disp_inst = raw_inst_norm
        os_engine = 'linux'
        try:
            pricing_os_out[i] = normalize_pricing_os_label(raw_os_cell)
            os_engine = normalize_os_engine_key(raw_os_cell)
            canon = canonicalize_instance_api_name(raw_inst_norm)
            if canon is not None:
                disp_inst = normalize_instance_string(canon)
            if canon is None:
                a1i[i] = a2i[i] = NA
                cur_p[i] = a1p[i] = a2p[i] = None
                a1s[i] = a2s[i] = NA
                continue
            inst = disp_inst
            if not _row_matches_service(inst, service):
                cur_p[i] = a1p[i] = a2p[i] = None
                a1i[i] = a2i[i] = NA
                a1s[i] = a2s[i] = NA
                continue
            backend = _row_price_service(inst, service)
            rec = get_rds_recommendations(inst, cpu_filter=cpu) if backend == 'rds' else get_recommendations(inst, cpu_filter=cpu)
            alt1 = rec.get('alt1')
            alt2 = rec.get('alt2')
            if alt1 and alt2 and (alt1 == alt2):
                alt2 = None
            p_cur = _hourly_cur(inst, os_engine, backend)
            p_a1 = _hourly_alt(alt1, os_engine, backend)
            p_a2 = _hourly_alt(alt2, os_engine, backend)
            cur_p[i] = p_cur
            a1i[i] = alt1 if alt1 is not None else NA
            if alt2 is not None:
                a2i[i] = alt2
            elif alt1 is not None:
                a2i[i] = ALT2_NO_DISTINCT
            else:
                a2i[i] = NA
            a1p[i] = p_a1
            a2p[i] = p_a2
            a1s[i] = _savings_from_hourly(p_cur, p_a1)
            a2s[i] = _savings_from_hourly(p_cur, p_a2)
        except Exception as exc:
            row_na_fallback_count += 1
            if _finops_debug:
                logger.warning('Row %s: enrichment failed (%s) — filled N/A.', i, type(exc).__name__)
                logger.debug('Row enrichment detail', exc_info=True)
            try:
                pricing_os_out[i] = normalize_pricing_os_label(raw_os_cell)
            except Exception:
                pricing_os_out[i] = LINUX_FALLBACK_LABEL
            try:
                os_engine = normalize_os_engine_key(raw_os_cell)
            except Exception:
                os_engine = 'linux'
            cur_p[i] = a1p[i] = a2p[i] = None
            a1i[i] = a2i[i] = NA
            a1s[i] = a2s[i] = NA
    if _finops_debug:
        print(
            f'[FinOps DEBUG] hourly_lookup_region={PRICING_LOOKUP_REGION} rows={n} ui_region={region!r}',
            flush=True,
        )
        for j in range(min(5, n)):
            inst_j = normalize_instance_string(inst_series.iloc[j])
            os_j = pricing_os_out[j]
            pj = cur_p[j]
            p_txt = f'{pj:.6f}' if isinstance(pj, (int, float)) and math.isfinite(float(pj)) else 'N/A'
            print(f'[FinOps DEBUG] {inst_j} {os_j} {PRICING_LOOKUP_REGION} {p_txt}', flush=True)
    left = work.iloc[:, :ins_idx + 1].copy()
    right = work.iloc[:, ins_idx + 1:].copy()
    mid = pd.DataFrame(
        {
            INSERT_COLS[0]: pricing_os_out,
            INSERT_COLS[1]: act_out,
            INSERT_COLS[2]: cur_p,
            INSERT_COLS[3]: a1i,
            INSERT_COLS[4]: a1p,
            INSERT_COLS[5]: a1s,
            INSERT_COLS[6]: a2i,
            INSERT_COLS[7]: a2p,
            INSERT_COLS[8]: a2s,
        },
        index=work.index,
    )
    out = pd.concat([left, mid, right], axis=1)
    assert len(out) == len(work), 'Row count changed'
    assert len(out.columns) == len(work.columns) + len(INSERT_COLS), 'Column count wrong'
    _expected_cols = list(work.columns[: ins_idx + 1]) + INSERT_COLS + list(work.columns[ins_idx + 1 :])
    assert list(out.columns) == _expected_cols, 'Original columns must appear in the same order with enrichment inserted after the instance column only'
    assert list(out.columns[: ins_idx + 1]) == list(work.columns[: ins_idx + 1])
    assert list(out.columns[ins_idx + 1 + len(INSERT_COLS) :]) == list(work.columns[ins_idx + 1 :])
    for k in range(ins_idx + 1):
        assert out.iloc[:, k].tolist() == work.iloc[:, k].tolist(), 'mutated leading column'
    for k in range(ins_idx + 1, len(work.columns)):
        j = k + len(INSERT_COLS)
        assert out.iloc[:, j].tolist() == work.iloc[:, k].tolist(), 'mutated original trailing'
    if row_na_fallback_count:
        logger.info('FinOps enrichment: %s row(s) returned N/A fallback (invalid or unsupported row data).', row_na_fallback_count)
    if _finops_debug:
        logger.info('FinOps enrichment finished rows=%s', len(out))
    return out

def _na_like(x: object) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    if isinstance(x, str) and not x.strip():
        return True
    return False


def apply_na_fill(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in INSERT_COLS:
        if c not in df.columns:
            continue
        df[c] = df[c].apply(lambda x: NA if _na_like(x) else x)
    return df
