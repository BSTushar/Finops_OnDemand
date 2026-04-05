from __future__ import annotations
import logging
import math
from typing import Literal
import pandas as pd
from data_loader import ColumnBinding
from instance_api import canonicalize_instance_api_name
from pricing_engine import DEFAULT_REGION, get_price, get_rds_hourly
from recommender import CPUFilterMode, get_recommendations
from rds_recommender import get_rds_recommendations
logger = logging.getLogger(__name__)
ServiceMode = Literal['ec2', 'rds', 'both']
INSERT_COLS: list[str] = ['Actual Cost ($)', 'Alt1 Instance', 'Alt1 Cost ($)', 'Alt1 Savings %', 'Alt2 Instance', 'Alt2 Cost ($)', 'Alt2 Savings %']
NA = 'N/A'
NO_SAVINGS = 'No Savings'

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

def _hourly_cur(inst: str, region: str, os_val: str, backend: Literal['ec2', 'rds']) -> float | None:
    if backend == 'rds':
        return get_rds_hourly(inst, region=region, os=os_val)
    return get_price(inst, region=region, os=os_val)

def _hourly_alt(alt: str | None, region: str, os_val: str, backend: Literal['ec2', 'rds']) -> float | None:
    if not alt:
        return None
    return _hourly_cur(alt, region, os_val, backend)

def _project_alt_cost(actual: float | None, p_cur: float | None, p_alt: float | None) -> float | None:
    if actual is None or p_cur is None or p_alt is None:
        return None
    if not math.isfinite(actual) or not math.isfinite(p_cur) or not math.isfinite(p_alt):
        return None
    if actual <= 0 or p_cur <= 0 or p_alt < 0:
        return None
    return round(actual * (p_alt / p_cur), 4)

def _savings_display(actual: float | None, alt_cost: float | None) -> float | str | None:
    if actual is None or alt_cost is None:
        return None
    if not math.isfinite(actual) or not math.isfinite(alt_cost) or actual <= 0:
        return None
    if alt_cost >= actual:
        return NO_SAVINGS
    pct = round((actual - alt_cost) / actual * 100, 1)
    return max(0.0, pct)

def _to_float(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str) and (not v.strip() or v.strip().lower() in ('nan', 'n/a', '')):
        return None
    try:
        x = float(v)
        if pd.isna(x) or not math.isfinite(x):
            return None
        return x
    except (TypeError, ValueError):
        return None

def process(df: pd.DataFrame, binding: ColumnBinding, region: str=DEFAULT_REGION, service: ServiceMode='both', cpu_filter: CPUFilterMode='both') -> pd.DataFrame:
    if df.empty:
        raise ValueError('Cannot process an empty DataFrame.')
    cols = list(df.columns)
    if binding.instance not in cols or binding.os not in cols:
        raise ValueError('Binding columns missing from DataFrame.')
    try:
        ins_idx = cols.index(binding.instance)
    except ValueError as exc:
        raise ValueError('Instance column not in DataFrame.') from exc
    n = len(df)
    (ci, co) = (binding.instance, binding.os)
    cc = binding.actual_cost
    actual_vals: list[float | None] = []
    if cc and cc in cols:
        raw_a = df[cc].tolist()
        actual_vals = [_to_float(x) for x in raw_a]
    else:
        actual_vals = [None] * n
    inst_series = df[ci].astype(str).str.strip()
    os_series = df[co].astype(str).str.strip()
    a1i: list = [None] * n
    a1c: list = [None] * n
    a1s: list = [None] * n
    a2i: list = [None] * n
    a2c: list = [None] * n
    a2s: list = [None] * n
    act_out: list = [None] * n
    cpu: CPUFilterMode = cpu_filter if cpu_filter in ('default', 'intel', 'graviton', 'both') else 'both'
    for i in range(n):
        raw_inst = inst_series.iloc[i]
        os_val = os_series.iloc[i] or 'linux'
        act = actual_vals[i]
        act_out[i] = act
        canon = canonicalize_instance_api_name(raw_inst)
        if canon is None:
            continue
        inst = canon
        if not _row_matches_service(inst, service):
            continue
        backend = _row_price_service(inst, service)
        rec = get_rds_recommendations(inst, cpu_filter=cpu) if backend == 'rds' else get_recommendations(inst, cpu_filter=cpu)
        alt1 = rec.get('alt1')
        alt2 = rec.get('alt2')
        if alt1 and alt2 and (alt1 == alt2):
            alt2 = None
        p_cur = _hourly_cur(inst, region, os_val, backend)
        p_a1 = _hourly_alt(alt1, region, os_val, backend)
        p_a2 = _hourly_alt(alt2, region, os_val, backend)
        a1i[i] = alt1
        a2i[i] = alt2
        c1 = _project_alt_cost(act, p_cur, p_a1)
        c2 = _project_alt_cost(act, p_cur, p_a2)
        a1c[i] = c1
        a2c[i] = c2
        a1s[i] = _savings_display(act, c1)
        a2s[i] = _savings_display(act, c2)
    left = df.iloc[:, :ins_idx + 1].copy()
    right = df.iloc[:, ins_idx + 1:].copy()
    mid = pd.DataFrame({INSERT_COLS[0]: act_out, INSERT_COLS[1]: a1i, INSERT_COLS[2]: a1c, INSERT_COLS[3]: a1s, INSERT_COLS[4]: a2i, INSERT_COLS[5]: a2c, INSERT_COLS[6]: a2s}, index=df.index)
    out = pd.concat([left, mid, right], axis=1)
    assert len(out) == len(df), 'Row count changed'
    assert len(out.columns) == len(df.columns) + len(INSERT_COLS), 'Column count wrong'
    assert list(out.columns[:ins_idx + 1]) == list(df.columns[:ins_idx + 1])
    assert list(out.columns[ins_idx + 1 + len(INSERT_COLS):]) == list(df.columns[ins_idx + 1:])
    for k in range(ins_idx + 1):
        assert out.iloc[:, k].tolist() == df.iloc[:, k].tolist(), 'mutated leading column'
    for k in range(ins_idx + 1, len(df.columns)):
        j = k + len(INSERT_COLS)
        assert out.iloc[:, j].tolist() == df.iloc[:, k].tolist(), 'mutated original trailing'
    logger.info('Processed %s rows service=%s region=%s', len(out), service, region)
    return out

def apply_na_fill(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in INSERT_COLS:
        if c not in df.columns:
            continue
        df[c] = df[c].apply(lambda x: NA if x is None or (isinstance(x, float) and pd.isna(x)) else x)
    return df
