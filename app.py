from __future__ import annotations
import logging
import pandas as pd
import streamlit as st
from data_loader import LoadResult, analyze_load, dataframe_from_bytes, finalize_binding, load_file
from excel_export import build_excel, savings_numeric
from processor import apply_na_fill, process
from pricing_engine import COST_DISCLAIMER_TEXT, DECISION_SUPPORT_NOTE, DEFAULT_REGION, REGION_LABELS, SUPPORTED_REGIONS, cache_age_days, cache_is_stale, format_pricing_snapshot_line
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs
logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
st.set_page_config(page_title='FinOps Optimizer', page_icon='◆', layout='wide', initial_sidebar_state='collapsed')
APPLE_UI_CSS = """
<style>
:root {
  --apple-blue: #0071e3;
  --apple-blue-hover: #0077ed;
  --apple-text: #1d1d1f;
  --apple-text2: #6e6e73;
  --apple-bg: #f5f5f7;
  --apple-card: #ffffff;
  --apple-line: rgba(0,0,0,0.08);
  --apple-shadow: 0 4px 24px rgba(0,0,0,0.06);
  --apple-radius: 18px;
  --apple-radius-sm: 12px;
  --fin-green: #34c759;
  --fin-amber: #ff9f0a;
  --fin-red: #ff3b30;
}
@media (prefers-color-scheme: dark) {
  :root {
    --apple-text: #f5f5f7;
    --apple-text2: #a1a1a6;
    --apple-bg: #000000;
    --apple-card: #1c1c1e;
    --apple-line: rgba(255,255,255,0.12);
    --apple-shadow: 0 8px 40px rgba(0,0,0,0.45);
  }
}
html, body, .stApp, [data-testid="stAppViewContainer"] {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif !important;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
.stApp {
  background: var(--apple-bg) !important;
  color: var(--apple-text) !important;
}
.block-container {
  padding-top: 2.5rem !important;
  padding-bottom: 4rem !important;
  max-width: 1080px !important;
}
#MainMenu, footer { visibility: hidden; }
hr {
  margin: 2rem 0 !important;
  border: none !important;
  height: 1px !important;
  background: var(--apple-line) !important;
}
/* Hero */
.apple-hero {
  text-align: center;
  padding: 2.5rem 1rem 2rem;
  margin-bottom: 0.5rem;
}
.apple-eyebrow {
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--apple-text2);
  margin: 0 0 0.75rem;
}
.apple-headline {
  font-size: clamp(2rem, 5vw, 2.75rem);
  font-weight: 600;
  letter-spacing: -0.03em;
  line-height: 1.1;
  color: var(--apple-text);
  margin: 0 0 0.5rem;
}
.apple-tagline {
  font-size: 1.125rem;
  font-weight: 400;
  color: var(--apple-text2);
  margin: 0 0 1.25rem;
  line-height: 1.45;
  max-width: 36rem;
  margin-left: auto;
  margin-right: auto;
}
.apple-pill {
  display: inline-block;
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--apple-text2);
  background: var(--apple-card);
  border: 1px solid var(--apple-line);
  padding: 0.35rem 0.85rem;
  border-radius: 100px;
  box-shadow: var(--apple-shadow);
}
/* Guided steps */
.flow-step-head {
  display: flex;
  align-items: flex-start;
  gap: 1rem;
  margin: 2.25rem 0 1rem;
  padding-bottom: 0.35rem;
  border-bottom: 1px solid var(--apple-line);
}
.flow-num {
  flex-shrink: 0;
  width: 2rem;
  height: 2rem;
  border-radius: 50%;
  background: var(--apple-blue);
  color: #fff;
  font-size: 0.85rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  justify-content: center;
  line-height: 1;
}
.flow-title {
  font-size: 1.25rem;
  font-weight: 600;
  letter-spacing: -0.02em;
  color: var(--apple-text);
  margin: 0;
}
.flow-sub {
  font-size: 0.9rem;
  color: var(--apple-text2);
  margin: 0.35rem 0 0;
  line-height: 1.4;
}
/* Trust / disclaimer card */
.trust-card {
  background: var(--apple-card);
  border: 1px solid var(--apple-line);
  border-radius: var(--apple-radius);
  padding: 1.5rem 1.75rem;
  margin: 1.5rem 0 0.5rem;
  box-shadow: var(--apple-shadow);
}
.trust-card .snap { font-size: 0.9rem; font-weight: 600; color: var(--apple-text); margin: 0 0 0.65rem; }
.trust-card .legal { font-size: 0.8125rem; color: var(--apple-text2); line-height: 1.55; margin: 0.4rem 0 0; }
.trust-card code { font-size: 0.8em; padding: 0.1em 0.35em; border-radius: 4px; background: var(--apple-bg); }
/* Section label */
.sec {
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--apple-text2);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin: 0 0 0.5rem;
}
/* KPI metrics */
.metric {
  background: var(--apple-card) !important;
  border: 1px solid var(--apple-line) !important;
  border-radius: var(--apple-radius-sm) !important;
  padding: 1rem 1.15rem !important;
  box-shadow: var(--apple-shadow);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.metric:hover { transform: translateY(-1px); }
.metric-label { font-size: 0.68rem !important; font-weight: 600 !important; color: var(--apple-text2) !important; letter-spacing: 0.04em; text-transform: uppercase; }
.metric-value { font-size: 1.35rem !important; font-weight: 600 !important; color: var(--apple-text) !important; margin-top: 0.25rem !important; }
/* Alerts */
.box-ok, .box-warn, .box-err {
  padding: 0.85rem 1.1rem;
  border-radius: var(--apple-radius-sm);
  font-size: 0.875rem;
  line-height: 1.5;
  margin: 0.5rem 0;
  border: 1px solid var(--apple-line);
}
.box-ok { background: rgba(52, 199, 89, 0.1); border-color: rgba(52, 199, 89, 0.25); color: var(--apple-text); }
.box-warn { background: rgba(255, 159, 10, 0.1); border-color: rgba(255, 159, 10, 0.3); color: var(--apple-text); }
.box-err { background: rgba(255, 59, 48, 0.08); border-color: rgba(255, 59, 48, 0.25); color: var(--apple-text); }
/* Primary controls row breathing room */
[data-testid="stHorizontalBlock"] {
  gap: 0.75rem;
}
/* Buttons */
div.stButton > button {
  border-radius: 100px !important;
  font-weight: 500 !important;
  padding: 0.55rem 1.25rem !important;
  transition: background 0.2s ease, transform 0.15s ease, box-shadow 0.2s ease !important;
  border: none !important;
}
div.stButton > button[kind="primary"] {
  background: var(--apple-blue) !important;
  color: #fff !important;
  box-shadow: 0 2px 12px rgba(0, 113, 227, 0.35) !important;
}
div.stButton > button[kind="primary"]:hover {
  background: var(--apple-blue-hover) !important;
  box-shadow: 0 4px 16px rgba(0, 113, 227, 0.4) !important;
}
div.stButton > button[kind="secondary"] {
  background: var(--apple-card) !important;
  color: var(--apple-text) !important;
  border: 1px solid var(--apple-line) !important;
}
/* Inputs */
.stSelectbox [data-baseweb="select"] > div, .stTextInput input, [data-baseweb="input"] {
  border-radius: 10px !important;
}
[data-testid="stFileUploader"] section {
  border-radius: var(--apple-radius-sm) !important;
  border: 2px dashed var(--apple-line) !important;
  background: var(--apple-card) !important;
  padding: 1rem !important;
  transition: border-color 0.2s ease, background 0.2s ease !important;
}
[data-testid="stFileUploader"] section:hover {
  border-color: var(--apple-blue) !important;
}
/* Expander */
.streamlit-expanderHeader {
  font-weight: 500 !important;
  border-radius: var(--apple-radius-sm) !important;
}
[data-testid="stExpander"] {
  border: 1px solid var(--apple-line) !important;
  border-radius: var(--apple-radius) !important;
  overflow: hidden;
  background: var(--apple-card) !important;
  box-shadow: var(--apple-shadow);
}
/* Dataframe */
[data-testid="stDataFrame"] {
  max-height: 520px !important;
  overflow: auto !important;
  border-radius: var(--apple-radius-sm) !important;
  border: 1px solid var(--apple-line) !important;
}
/* Radio horizontal */
[data-testid="stHorizontalBlock"] [data-baseweb="radio"] label {
  font-size: 0.875rem !important;
}
/* Caption */
.stCaption, [data-testid="stCaptionContainer"] {
  color: var(--apple-text2) !important;
  font-size: 0.8125rem !important;
}
/* Info blocks */
.stAlert {
  border-radius: var(--apple-radius-sm) !important;
}
</style>
"""
st.markdown(APPLE_UI_CSS, unsafe_allow_html=True)


def _flow_step(num: int, title: str, subtitle: str='') -> None:
    sub = f'<p class="flow-sub">{subtitle}</p>' if subtitle else ''
    st.markdown(f'<div class="flow-step-head"><span class="flow-num">{num}</span><div><p class="flow-title">{title}</p>{sub}</div></div>', unsafe_allow_html=True)

def _savings_for_kpi(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str) and v.strip() == 'No Savings':
        return None
    return savings_numeric(v)

def kpis(df: pd.DataFrame) -> dict:
    s1 = pd.Series([_savings_for_kpi(x) for x in df.get('Alt1 Savings %', [])], dtype=float)
    s1 = s1.dropna()
    return {'total': len(df), 'avg1': s1.mean() if len(s1) else None, 'max1': s1.max() if len(s1) else None, 'act_col': 'Actual Cost ($)' in df.columns}

def render_kpis(k: dict):
    (c1, c2, c3, c4) = st.columns(4)
    with c1:
        st.markdown(f"""<div class="metric"><div class="metric-label">Rows</div><div class="metric-value">{k['total']:,}</div></div>""", unsafe_allow_html=True)
    with c2:
        v = f"{k['avg1']:.1f}%" if k['avg1'] is not None else '—'
        st.markdown(f'<div class="metric"><div class="metric-label">Avg Alt1 savings</div><div class="metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c3:
        v = f"{k['max1']:.1f}%" if k['max1'] is not None else '—'
        st.markdown(f'<div class="metric"><div class="metric-label">Max Alt1 savings</div><div class="metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric"><div class="metric-label">Actual cost column</div><div class="metric-value">{('Yes' if k['act_col'] else 'No')}</div></div>""", unsafe_allow_html=True)
for (key, default) in (('load_result', None), ('result', None), ('region_id', DEFAULT_REGION), ('service', 'both'), ('cpu_filter', 'both'), ('cost_pick', None)):
    if key not in st.session_state:
        st.session_state[key] = default
stale = cache_is_stale()
pill = f'Dataset {cache_age_days()}d old — consider refresh' if stale else 'List prices loaded locally'
st.markdown(f'''<div class="apple-hero">
<p class="apple-eyebrow">Decision support · EC2 &amp; RDS</p>
<h1 class="apple-headline">FinOps Optimizer</h1>
<p class="apple-tagline">Upload your export, map columns once, and see indicative savings from a static AWS list-price snapshot—before you commit to changes.</p>
<span class="apple-pill">{pill}</span>
</div>''', unsafe_allow_html=True)
_flow_step(1, 'Bring your file', 'CSV or Excel. Your columns stay in order; we only add enrichment after the instance column.')
(up_col, reg_col, svc_col, cpu_col, go_col) = st.columns([3, 2, 1, 1, 1])
with up_col:
    st.markdown('<div class="sec">File</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader('Drop your spreadsheet', type=['csv', 'xlsx', 'xls'], label_visibility='visible')
    st.caption('Include the column with AWS API Names (e.g. m5.large, db.r5.xlarge).')
with reg_col:
    st.markdown('<div class="sec">Pricing region</div>', unsafe_allow_html=True)
    region_opts = [f'{label}  [{rid}]' for (rid, label) in SUPPORTED_REGIONS]
    default_idx = [r for (r, _) in SUPPORTED_REGIONS].index(DEFAULT_REGION)
    sel_disp = st.selectbox('region', region_opts, index=default_idx, label_visibility='collapsed')
    sel_region = [r for (r, _) in SUPPORTED_REGIONS][region_opts.index(sel_disp)]
    st.session_state['region_id'] = sel_region
with svc_col:
    st.markdown('<div class="sec">Service</div>', unsafe_allow_html=True)
    st.session_state['service'] = st.radio('svc', ['ec2', 'rds', 'both'], format_func=lambda x: {'ec2': 'EC2', 'rds': 'RDS', 'both': 'Both'}[x], label_visibility='collapsed', horizontal=True)
with cpu_col:
    st.markdown('<div class="sec">CPU</div>', unsafe_allow_html=True)
    st.session_state['cpu_filter'] = st.selectbox('cpu', ['both', 'default', 'intel', 'graviton'], format_func=lambda x: {'both': 'Both', 'default': 'Default', 'intel': 'Intel', 'graviton': 'Graviton'}[x], label_visibility='collapsed')
with go_col:
    st.markdown('<div class="sec">&nbsp;</div>', unsafe_allow_html=True)
    run = st.button('Continue', type='primary', disabled=uploaded is None, use_container_width=True)
_snapshot_ui = format_pricing_snapshot_line(st.session_state.get('region_id', DEFAULT_REGION))
st.markdown(f'''<div class="trust-card">
<p class="snap">{_snapshot_ui}</p>
<p class="legal">{COST_DISCLAIMER_TEXT}</p>
<p class="legal">{DECISION_SUPPORT_NOTE}</p>
<p class="legal">Indicative savings only. Original columns preserved. Unknown API names or SKUs show <strong>N/A</strong>—never a guess.</p>
</div>''', unsafe_allow_html=True)
with st.expander('How it works · Limitations', expanded=False):
    st.markdown(
        """
**What this tool does**
- Enriches your uploaded spreadsheet with **Actual Cost** (from your file), **recommended instance classes** (Alt1 / Alt2), **indicative alt costs**, and **savings %** using **static AWS on-demand list prices** from a **local dataset** (no live AWS Pricing API calls).
- Supports **EC2** and **RDS** API names (`m5.large`, `db.r5.xlarge`, …) and **Service** mode (EC2-only, RDS-only, or both).

**What it does not do**
- It does **not** replace **AWS Billing**, **Cost Explorer**, or **CUR**; it does **not** apply your enterprise discounts, Reserved Instances, or Savings Plans.
- It does **not** prove performance or compatibility (especially **Graviton**); engineering must validate before production changes.

**How to use it**
1. Upload CSV or Excel (all original columns stay in order).  
2. Choose **Pricing region** (default Ireland `eu-west-1`).  
3. Pick **Service** (EC2 / RDS / Both) and **CPU** mode for enrichment.  
4. Map **instance** and **OS** columns if prompted; choose **cost** column if needed.  
5. **Run enrichment**, then filter, export **Excel** (includes disclaimer + pricing metadata) or **CSV** (table only).

**Limitations**
- Values are **indicative**; validate against **actual invoices** before decisions.  
- Many **RDS** SKUs may be missing from the local table → **N/A**.  
- Pricing snapshot **as-of** date is shown above and in the Excel **Metadata** sheet.

**Fix Your Sheet (optional)**  
Merge two uploads (e.g. inventory + cost extract) on a common ID **before** enrichment. Dataset 1 is primary: its columns and order are kept; only **new** columns from Dataset 2 are appended, with values filled where D1 cells are empty.
"""
    )
_flow_step(2, 'Fix Your Sheet (optional)', 'Two files—inventory plus cost extract? Merge on a shared ID. Primary file keeps its column order; we only append new columns from the second file.')
st.caption('Dataset 1 = primary row layout. Dataset 2 = extra fields (e.g. spend). Match on resource ID, instance ID, or similar.')
(fx1, fx2) = st.columns(2)
with fx1:
    st.markdown('<div class="sec">Dataset 1 · Primary</div>', unsafe_allow_html=True)
    fix_u1 = st.file_uploader('Primary spreadsheet', type=['csv', 'xlsx', 'xls'], key='fix_sheet_d1')
with fx2:
    st.markdown('<div class="sec">Dataset 2 · Supplement</div>', unsafe_allow_html=True)
    fix_u2 = st.file_uploader('Supplement spreadsheet', type=['csv', 'xlsx', 'xls'], key='fix_sheet_d2')
if fix_u1 and fix_u2:
    try:
        fix_d1 = dataframe_from_bytes(fix_u1.getvalue(), fix_u1.name)
        fix_d2 = dataframe_from_bytes(fix_u2.getvalue(), fix_u2.name)
    except ValueError as fve:
        st.markdown(f'<div class="box-err">❌ Fix Your Sheet: {fve}</div>', unsafe_allow_html=True)
        fix_d1 = None
        fix_d2 = None
    if fix_d1 is not None and fix_d2 is not None:
        _pairs = suggest_key_pairs(list(fix_d1.columns), list(fix_d2.columns))
        _def = _pairs[0] if _pairs else (str(fix_d1.columns[0]), str(fix_d2.columns[0]))
        _i1 = list(fix_d1.columns).index(_def[0]) if _def[0] in fix_d1.columns else 0
        _i2 = list(fix_d2.columns).index(_def[1]) if _def[1] in fix_d2.columns else 0
        fix_k1 = st.selectbox('Merge key — Dataset 1 (primary)', list(fix_d1.columns), index=min(_i1, len(fix_d1.columns) - 1), key='fix_sheet_key_d1')
        fix_k2 = st.selectbox('Merge key — Dataset 2', list(fix_d2.columns), index=min(_i2, len(fix_d2.columns) - 1), key='fix_sheet_key_d2')
        if st.button('Merge sheets', key='fix_sheet_merge'):
            try:
                (fix_merged, fix_mw) = merge_primary_with_secondary(fix_d1, fix_d2, fix_k1, fix_k2)
                st.session_state['fix_merged_df'] = fix_merged
                st.session_state['fix_merge_warnings'] = fix_mw
            except ValueError as mve:
                st.markdown(f'<div class="box-err">❌ {mve}</div>', unsafe_allow_html=True)
_fix_mdf = st.session_state.get('fix_merged_df')
if _fix_mdf is not None:
    st.markdown(f'<div class="box-ok">✅ Merged preview: **{len(_fix_mdf):,}** rows × **{len(_fix_mdf.columns)}** columns</div>', unsafe_allow_html=True)
    for _fw in st.session_state.get('fix_merge_warnings', []):
        st.markdown(f'<div class="box-warn">⚠️ {_fw}</div>', unsafe_allow_html=True)
    st.dataframe(_fix_mdf.head(40), use_container_width=True, hide_index=True, height=360)
    if st.button('Use merged data', type='primary', key='fix_sheet_apply'):
        _mw = st.session_state.get('fix_merge_warnings', [])
        _bw = ['Dataset prepared with Fix Your Sheet (two files merged on key).'] + list(_mw)
        st.session_state['load_result'] = analyze_load(_fix_mdf, _bw)
        st.session_state['result'] = None
        st.session_state['binding'] = None
        st.session_state['cost_pick'] = None
        st.session_state.pop('_enrich_svc', None)
        st.session_state.pop('_enrich_cpu', None)
        st.session_state.pop('fix_merged_df', None)
        st.session_state.pop('fix_merge_warnings', None)
        st.rerun()
st.markdown('<hr/>', unsafe_allow_html=True)
if run and uploaded:
    with st.spinner('Loading…'):
        try:
            st.session_state.pop('fix_merged_df', None)
            st.session_state.pop('fix_merge_warnings', None)
            lr: LoadResult = load_file(uploaded, uploaded.name)
            st.session_state['load_result'] = lr
            st.session_state['result'] = None
            st.session_state['cost_pick'] = None
            st.session_state['binding'] = None
            st.session_state.pop('_enrich_svc', None)
            st.session_state.pop('_enrich_cpu', None)
            for w in lr.warnings:
                st.markdown(f'<div class="box-warn">⚠️ {w}</div>', unsafe_allow_html=True)
        except ValueError as ve:
            st.session_state['load_result'] = None
            st.markdown(f'<div class="box-err">❌ {ve}</div>', unsafe_allow_html=True)
        except Exception as e:
            st.session_state['load_result'] = None
            log.error('load_file failed: %s', type(e).__name__)
            st.markdown('<div class="box-err">❌ Failed to read file.</div>', unsafe_allow_html=True)
lr: LoadResult | None = st.session_state.get('load_result')
binding_ready = False
chosen_binding = None
if st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True
if lr is not None and (not binding_ready):
    cols_all = list(lr.df.columns)
    if lr.needs_instance_pick or lr.needs_os_pick:
        _flow_step(3, 'Map columns', 'Tell us which columns hold the instance API name and OS (or engine).')
        (mc1, mc2) = st.columns(2)
        with mc1:
            di = 0
            if lr.instance_candidates:
                di = cols_all.index(lr.instance_candidates[0]) if lr.instance_candidates[0] in cols_all else 0
            inst_sel = st.selectbox('Instance / DB class (AWS API Name)', cols_all, index=min(di, len(cols_all) - 1))
        with mc2:
            do = 0
            if lr.os_candidates:
                do = cols_all.index(lr.os_candidates[0]) if lr.os_candidates[0] in cols_all else 0
            os_sel = st.selectbox('OS / engine column', cols_all, index=min(do, len(cols_all) - 1))
        cost_sel = None
        if len(lr.cost_candidates) > 1:
            cost_sel = st.selectbox('Actual cost column (required for savings)', lr.cost_candidates, key='cost_ambiguous')
        elif len(lr.cost_candidates) == 1:
            cost_sel = lr.cost_candidates[0]
        else:
            cost_sel = st.selectbox('Actual cost column (optional)', ['— None —'] + cols_all, key='cost_optional')
            if cost_sel == '— None —':
                cost_sel = None
        if st.button('Save mapping', type='primary'):
            try:
                b = finalize_binding(lr, inst_sel, os_sel, cost_sel).binding
                st.session_state['binding'] = b
                st.session_state['cost_pick'] = cost_sel
                st.rerun()
            except ValueError as e:
                st.markdown(f'<div class="box-err">❌ {e}</div>', unsafe_allow_html=True)
    elif lr.binding is not None:
        if len(lr.cost_candidates) > 1 and lr.binding.actual_cost is None:
            st.markdown('<div class="box-warn">Multiple cost columns — pick one for Actual Cost.</div>', unsafe_allow_html=True)
            cp = st.selectbox('Actual cost column', lr.cost_candidates, key='cost_pick_multi')
            if st.button('Confirm cost column', type='primary'):
                b = finalize_binding(lr, lr.binding.instance, lr.binding.os, cp).binding
                st.session_state['binding'] = b
                st.rerun()
        elif st.session_state.get('binding') is None:
            st.session_state['binding'] = lr.binding
if lr is not None and st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True
    if st.session_state.get('result') is None:
        _flow_step(4, 'Run enrichment', 'Applies list prices for the region you chose and fills Alt instance, costs, and savings.')
        if st.button('Run enrichment', type='primary', key='run_enrich'):
            try:
                svc = st.session_state['service']
                cpu = st.session_state['cpu_filter']
                reg = st.session_state.get('region_id', DEFAULT_REGION)
                out = process(lr.df, chosen_binding, region=reg, service=svc, cpu_filter=cpu)
                st.session_state['result'] = out
                st.session_state['_enrich_svc'] = svc
                st.session_state['_enrich_cpu'] = cpu
                st.success(f'Enriched {len(out):,} rows')
                st.rerun()
            except Exception as ex:
                st.markdown(f'<div class="box-err">❌ {ex}</div>', unsafe_allow_html=True)
                log.error('process failed: %s', type(ex).__name__)
df_out: pd.DataFrame | None = st.session_state.get('result')
if df_out is not None:
    if st.session_state.get('_enrich_svc') != st.session_state.get('service') or st.session_state.get('_enrich_cpu') != st.session_state.get('cpu_filter'):
        st.warning('Service or CPU mode changed since last enrichment — click **Run enrichment** to refresh.')
    st.markdown('<div class="flow-step-head" style="margin-top:1.5rem;border-bottom:none;"><span class="flow-num" style="background:#34c759;">5</span><div><p class="flow-title">Results</p><p class="flow-sub">Filter, search, then download Excel or CSV.</p></div></div>', unsafe_allow_html=True)
    (f1, f2, f3, f4) = st.columns([1, 1, 1, 3])
    with f1:
        vf_svc = st.radio('View service', ['all', 'ec2', 'rds'], format_func=lambda x: {'all': 'Both (show all)', 'ec2': 'EC2 rows only', 'rds': 'RDS rows only'}[x], horizontal=True)
    with f2:
        st.caption('CPU (enrichment)')
        st.write(str(st.session_state.get('cpu_filter', 'both')).title())
    with f3:
        vf_os = st.text_input('OS contains', placeholder='filter…')
    with f4:
        q = st.text_input('Search', placeholder='any column…')
    view = df_out.copy()
    bind = st.session_state.get('binding')
    inst_col = bind.instance if bind else None
    if vf_svc == 'ec2' and inst_col and (inst_col in view.columns):
        view = view[~view[inst_col].astype(str).str.lower().str.strip().str.startswith('db.')]
    elif vf_svc == 'rds' and inst_col and (inst_col in view.columns):
        view = view[view[inst_col].astype(str).str.lower().str.strip().str.startswith('db.')]
    os_col_name = bind.os if bind else None
    if vf_os and os_col_name and (os_col_name in view.columns):
        view = view[view[os_col_name].astype(str).str.contains(vf_os, case=False, na=False, regex=False)]
    if q:
        m = pd.Series(False, index=view.index)
        for col in view.columns:
            m |= view[col].astype(str).str.contains(q, case=False, na=False, regex=False)
        view = view[m]
    st.caption(f'Showing **{len(view):,}** of **{len(df_out):,}** rows')
    render_kpis(kpis(view))

    def _style_sav(v: str) -> str:
        n = savings_numeric(v)
        if n is None:
            return 'color: #86868b'
        if n >= 20:
            return 'color: #34c759; font-weight:600'
        if n > 0:
            return 'color: #ff9f0a; font-weight:600'
        return 'color: #ff3b30; font-weight:600'

    def _fmt_cell(cname: str):

        def _f(x):
            if 'Savings %' in cname:
                if pd.isna(x) or x is None:
                    return 'N/A'
                return str(x) if isinstance(x, str) else f'{float(x):.1f}%'
            if 'Cost ($)' in cname:
                if pd.isna(x) or x is None:
                    return 'N/A'
                if isinstance(x, (int, float)):
                    return f'${float(x):,.4f}'
                return str(x)
            return x
        return _f
    fmt_map = {c: _fmt_cell(c) for c in view.columns if 'Savings %' in c or 'Cost ($)' in c}
    style_cols = {c: _style_sav for c in view.columns if 'Savings %' in c}
    try:
        sty = view.style.format(fmt_map, na_rep='N/A')
        for (col_name, fn) in style_cols.items():
            sty = sty.map(fn, subset=[col_name])
        sty = sty.set_properties(**{'font-size': '0.8rem'})
    except Exception as ex:
        log.debug('dataframe style skipped: %s', type(ex).__name__)
        sty = view
    st.dataframe(sty, use_container_width=True, hide_index=True, height=480)
    export_df = apply_na_fill(df_out)
    reg_id = st.session_state.get('region_id', DEFAULT_REGION)
    reg_lbl = REGION_LABELS.get(reg_id, '')
    (dx1, dx2) = st.columns(2)
    with dx1:
        st.download_button('Download Excel', build_excel(export_df, reg_lbl, reg_id), 'finops_recommendations.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', use_container_width=True)
    with dx2:
        st.download_button('Download CSV', export_df.to_csv(index=False).encode(), 'finops_recommendations.csv', 'text/csv', use_container_width=True)
elif lr is None:
    st.markdown('<div class="trust-card"><p class="snap" style="margin:0;">Ready when you are</p><p class="legal" style="margin:0.5rem 0 0;">Upload a file above and tap <strong>Continue</strong>. We’ll guide you through mapping and enrichment.</p></div>', unsafe_allow_html=True)
elif not binding_ready:
    st.markdown('<div class="trust-card"><p class="snap" style="margin:0;">Almost there</p><p class="legal" style="margin:0.5rem 0 0;">Finish column mapping (and cost column if asked), then run enrichment.</p></div>', unsafe_allow_html=True)
