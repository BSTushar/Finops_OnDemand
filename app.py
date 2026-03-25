"""
app.py  ·  FinOps EC2 Optimizer  ·  v1.3
=========================================
Production Streamlit UI — Airbus-grade internal tooling.

Features
--------
✅  File upload (CSV / Excel)
✅  Auto column detection + failsafe manual mapping dropdown
✅  4-region dropdown (EU Ireland default)
✅  Sticky-style virtual table header via CSS
✅  Search + multi-filter (region, family, savings tier)
✅  Colour-coded savings (green ≥20%, amber 5–20%)
✅  Old-gen instance flagging (red badge)
✅  KPI summary tiles
✅  Savings-by-family bar chart
✅  One-click Excel export (business-ready, formatted)
✅  Docker-ready (runs on port 8501)

Run locally:  streamlit run app.py
Docker:       docker build -t finops . && docker run -p 8501:8501 finops
"""

import io
import logging
import traceback
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

from data_loader import load_file, LoadResult, ALL_EXPECTED, COLUMN_ALIASES
from processor import process, apply_na_fill, ENRICHED_COLS
from pricing_engine import (
    SUPPORTED_REGIONS, DEFAULT_REGION, REGION_LABELS,
    CACHE_METADATA, cache_is_stale, cache_age_days,
)

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ── Page ───────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FinOps EC2 Optimizer",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

:root {
  --bg:    #f4f5f7;
  --card:  #ffffff;
  --dark:  #1a1d23;
  --mid:   #6b7280;
  --light: #e2e5eb;
  --green: #00955c;
  --amber: #d97706;
  --red:   #dc2626;
  --blue:  #2563eb;
}

html, body, [class*="css"] {
  font-family: 'IBM Plex Sans', sans-serif;
  background: var(--bg); color: var(--dark);
}

/* ── Header ── */
.hdr {
  background: var(--dark); color: #f0f2f5;
  padding: 1.4rem 2rem 1.5rem; border-radius: 8px; margin-bottom: 1.1rem;
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: .5rem;
}
.hdr-left h1 { margin: 0; font-size: 1.45rem; font-weight: 600; letter-spacing: -.3px; }
.hdr-left p  { margin: .2rem 0 0; font-size: .8rem; color: #8a93a6; font-weight: 300; }
.hdr-right   { text-align: right; font-size: .75rem; color: #8a93a6; line-height: 1.6; }
.badge { display:inline-block; background:#00c27a; color:var(--dark);
         font-size:.62rem; font-weight:700; padding:.12rem .45rem;
         border-radius:3px; letter-spacing:.5px; text-transform:uppercase; margin-left:.5rem; }
.pill { display:inline-block; font-size:.68rem; font-weight:600;
        padding:.15rem .5rem; border-radius:10px; margin-left:.4rem; }
.pill-ok   { background:#d1fae5; color:#065f46; }
.pill-warn { background:#fef3c7; color:#92400e; }

/* ── Section labels ── */
.sec { font-size:.7rem; font-weight:600; color:var(--mid);
       letter-spacing:.7px; text-transform:uppercase; margin-bottom:.45rem; }

/* ── Metric boxes ── */
.metric-row { display:flex; gap:.75rem; flex-wrap:wrap; margin-bottom:1rem; }
.metric {
  flex: 1 1 140px; background:var(--card); border:1px solid var(--light);
  border-radius:8px; padding:1rem 1.1rem;
}
.metric-label { font-size:.68rem; font-weight:600; color:var(--mid);
                letter-spacing:.5px; text-transform:uppercase; }
.metric-value { font-size:1.6rem; font-weight:600; color:var(--dark);
                font-family:'IBM Plex Mono',monospace; line-height:1.2; margin-top:.15rem; }
.metric-sub   { font-size:.72rem; color:var(--mid); margin-top:.1rem; }
.metric-green .metric-value { color: var(--green); }
.metric-amber .metric-value { color: var(--amber); }

/* ── Alert boxes ── */
.box-ok   { background:#f0fdf4; border-left:3px solid #22c55e; border-radius:0 6px 6px 0; padding:.7rem 1rem; font-size:.82rem; color:#166534; margin:.5rem 0; }
.box-warn { background:#fffbeb; border-left:3px solid #f59e0b; border-radius:0 6px 6px 0; padding:.7rem 1rem; font-size:.82rem; color:#92400e; margin:.5rem 0; }
.box-err  { background:#fff1f2; border-left:3px solid #ef4444; border-radius:0 6px 6px 0; padding:.7rem 1rem; font-size:.82rem; color:#991b1b; margin:.5rem 0; }
.box-info { background:#eff6ff; border-left:3px solid #3b82f6; border-radius:0 6px 6px 0; padding:.7rem 1rem; font-size:.82rem; color:#1e40af; margin:.5rem 0; }

/* ── Table wrapper: sticky header via CSS ── */
.table-wrap {
  max-height: 520px;
  overflow-y: auto;
  border: 1px solid var(--light);
  border-radius: 8px;
}
/* Streamlit dataframe already handles scroll internally;
   the above provides an outer containment boundary */

/* ── Tag badges in table ── */
.tag-old     { background:#fee2e2; color:#991b1b; font-size:.68rem; font-weight:700;
               padding:.1rem .4rem; border-radius:3px; }
.tag-current { background:#f3f4f6; color:#374151; font-size:.68rem;
               padding:.1rem .4rem; border-radius:3px; }
.tag-latest  { background:#d1fae5; color:#065f46; font-size:.68rem; font-weight:600;
               padding:.1rem .4rem; border-radius:3px; }

/* ── Savings legend ── */
.legend { display:flex; gap:.75rem; align-items:center;
          font-size:.75rem; color:var(--mid); margin-bottom:.5rem; flex-wrap:wrap; }
.legend-dot { width:10px; height:10px; border-radius:2px; display:inline-block; margin-right:.3rem; }

/* ── Upload ── */
[data-testid="stFileUploader"] {
  border: 2px dashed #c8cdd8 !important; border-radius: 8px !important;
  background: #fafbfc !important;
}

/* ── Buttons ── */
.stButton>button, .stDownloadButton>button {
  font-family: 'IBM Plex Sans', sans-serif !important;
  font-weight: 500 !important; border-radius: 6px !important;
}
.stDownloadButton>button {
  background: var(--dark) !important; color: #fff !important; border: none !important;
}
.stDownloadButton>button:hover { background: #2d3340 !important; }

/* ── Misc ── */
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent; }
.block-container { padding-top: 1.1rem; padding-bottom: 2rem; max-width: 1480px; }
hr { border-color: var(--light); margin: 1rem 0; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# Utility functions
# ══════════════════════════════════════════════════════════════════════════

def build_excel(df: pd.DataFrame, region_label: str) -> bytes:
    """Return bytes of a formatted Excel workbook."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="EC2 Recommendations")
        wb = writer.book
        ws = writer.sheets["EC2 Recommendations"]

        thin = Side(style="thin", color="D0D5DD")
        bdr  = Border(left=thin, right=thin, top=thin, bottom=thin)

        # Header
        hdr_fill = PatternFill("solid", fgColor="1A1D23")
        hdr_font = Font(bold=True, color="FFFFFF", size=10, name="Calibri")
        for cidx in range(1, ws.max_column + 1):
            c = ws.cell(row=1, column=cidx)
            c.font = hdr_font; c.fill = hdr_fill; c.border = bdr
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Column index maps
        col_names   = df.columns.tolist()
        sav_cidx    = (col_names.index("Savings Opportunity (%)") + 1
                       if "Savings Opportunity (%)" in col_names else None)
        gen_cidx    = (col_names.index("Generation Flag") + 1
                       if "Generation Flag" in col_names else None)
        price_cidxs = {col_names.index(c) + 1
                       for c in col_names if "Price" in c}

        green_fill = PatternFill("solid", fgColor="D1FAE5")
        amber_fill = PatternFill("solid", fgColor="FEF3C7")
        red_fill   = PatternFill("solid", fgColor="FEE2E2")
        norm_font  = Font(size=10, name="Calibri")
        mono_font  = Font(size=9,  name="Courier New")

        for ridx in range(2, ws.max_row + 1):
            for cidx in range(1, ws.max_column + 1):
                cell = ws.cell(row=ridx, column=cidx)
                cell.border = bdr
                cell.alignment = Alignment(vertical="center")
                if cell.value == "N/A":
                    cell.number_format = "@"
                    cell.font = Font(size=10, name="Calibri", color="9CA3AF")
                elif cidx in price_cidxs:
                    cell.font = mono_font
                    cell.number_format = '$#,##0.0000'
                else:
                    cell.font = norm_font

            # Savings colour
            if sav_cidx:
                sc = ws.cell(row=ridx, column=sav_cidx)
                try:
                    fv = float(sc.value)
                    if fv >= 20:
                        sc.fill = green_fill
                        sc.font = Font(bold=True, color="065F46", size=10, name="Calibri")
                        sc.number_format = '0.0"%"'
                    elif fv >= 5:
                        sc.fill = amber_fill
                        sc.font = Font(bold=True, color="92400E", size=10, name="Calibri")
                        sc.number_format = '0.0"%"'
                except Exception:
                    pass

            # Old gen colour
            if gen_cidx:
                gc = ws.cell(row=ridx, column=gen_cidx)
                if gc.value == "Old Gen":
                    gc.fill = red_fill
                    gc.font = Font(bold=True, color="991B1B", size=10, name="Calibri")
                elif gc.value == "Latest":
                    gc.fill = green_fill
                    gc.font = Font(bold=True, color="065F46", size=10, name="Calibri")

        # Auto-width
        for col_cells in ws.columns:
            w = max((len(str(c.value or "")) for c in col_cells), default=8)
            ws.column_dimensions[col_cells[0].column_letter].width = min(w + 3, 48)

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        ws.row_dimensions[1].height = 36

        # Metadata sheet
        ws_m = wb.create_sheet("Metadata")
        ws_m.append(["Field", "Value"])
        ws_m.append(["Generated at",    datetime.now().strftime("%Y-%m-%d %H:%M UTC")])
        ws_m.append(["Pricing region",  region_label])
        ws_m.append(["Pricing vintage", CACHE_METADATA["last_updated"].strftime("%Y-%m-%d")])
        ws_m.append(["Source",          CACHE_METADATA["source"]])
        ws_m.append(["Total rows",      len(df)])
        n_opt = sum(1 for v in df.get("Savings Opportunity (%)", [])
                    if v not in (None, "N/A") and str(v) not in ("nan", ""))
        ws_m.append(["Optimisable rows", n_opt])
        for cell in ws_m[1]:
            cell.font = Font(bold=True, name="Calibri")
    return buf.getvalue()


def kpis(df: pd.DataFrame) -> dict:
    sav = pd.to_numeric(df.get("Savings Opportunity (%)", pd.Series(dtype=float)),
                        errors="coerce")
    old_gen_n = (df.get("Generation Flag", pd.Series()) == "Old Gen").sum()
    return dict(
        total      = len(df),
        with_sav   = int(sav.notna().sum()),
        avg_sav    = sav.dropna().mean(),
        max_sav    = sav.dropna().max(),
        old_gen_n  = int(old_gen_n),
        cost_total = df["Cost"].sum() if "Cost" in df.columns else None,
        potential  = (df["Cost"] * sav / 100).sum()
                     if "Cost" in df.columns else None,
    )


def render_kpis(k: dict):
    cols = st.columns(6)
    data = [
        ("Instances",          f"{k['total']:,}",                     "",   ""),
        ("Optimisable",        f"{k['with_sav']:,}",                  "",   ""),
        ("Old Gen ⚠️",         f"{k['old_gen_n']:,}",                 "",   "metric-amber" if k['old_gen_n'] else ""),
        ("Avg Savings",        f"{k['avg_sav']:.1f}%" if k['avg_sav'] else "—", "", "metric-green" if k['avg_sav'] else ""),
        ("Max Savings",        f"{k['max_sav']:.1f}%" if k['max_sav'] else "—", "", ""),
        ("Est. Monthly Save",  f"${k['potential']:,.0f}" if k['potential'] else "—",
                               f"of ${k['cost_total']:,.0f}" if k['cost_total'] else "", "metric-green" if k['potential'] else ""),
    ]
    for col, (label, val, sub, cls) in zip(cols, data):
        with col:
            st.markdown(f"""
            <div class="metric {cls}">
              <div class="metric-label">{label}</div>
              <div class="metric-value">{val}</div>
              {'<div class="metric-sub">'+sub+'</div>' if sub else ''}
            </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# Session state
# ══════════════════════════════════════════════════════════════════════════
for key in ("result", "load_result", "region_id"):
    if key not in st.session_state:
        st.session_state[key] = None
if "region_id" not in st.session_state or st.session_state["region_id"] is None:
    st.session_state["region_id"] = DEFAULT_REGION


# ══════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════
stale    = cache_is_stale()
pill_cls = "pill-warn" if stale else "pill-ok"
pill_txt = f"Cache {cache_age_days()}d — refresh due" if stale else f"Cache fresh"

st.markdown(f"""
<div class="hdr">
  <div class="hdr-left">
    <h1>💰 FinOps EC2 Optimizer <span class="badge">v1.3</span>
        <span class="pill {pill_cls}">{pill_txt}</span></h1>
    <p>Upload EC2 billing data · Select pricing region · Download enriched Excel with upgrade recommendations</p>
  </div>
  <div class="hdr-right">
    Pricing: AWS On-Demand, verified Mar 2025<br>
    Regions: EU Ireland · US Virginia · AP Mumbai · EU Frankfurt
  </div>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# INPUT ROW: Upload + Region + Process button
# ══════════════════════════════════════════════════════════════════════════
c_up, c_reg, c_btn = st.columns([4, 3, 1])

with c_up:
    uploaded = st.file_uploader(
        "file", type=["csv", "xlsx", "xls"],
        label_visibility="collapsed",
        help="Required: Instance Type, OS. Optional: Cost, Usage, Region, Account, Application",
    )
    st.caption("📎 CSV or Excel · Auto-detects columns · Case-insensitive matching")

with c_reg:
    st.markdown('<div class="sec">Pricing Region</div>', unsafe_allow_html=True)
    region_opts = [f"{label}  [{rid}]" for rid, label in SUPPORTED_REGIONS]
    default_idx = [r for r, _ in SUPPORTED_REGIONS].index(DEFAULT_REGION)
    sel_disp    = st.selectbox("region", region_opts, index=default_idx,
                               label_visibility="collapsed")
    sel_region  = [r for r, _ in SUPPORTED_REGIONS][region_opts.index(sel_disp)]
    st.session_state["region_id"] = sel_region

with c_btn:
    st.markdown("<br>", unsafe_allow_html=True)
    run = st.button("⚙️  Process", type="primary",
                    disabled=(uploaded is None), use_container_width=True)

st.markdown("---")

# ── Sample expander ────────────────────────────────────────────────────────
with st.expander("📄 View Sample Input  /  Download Template"):
    sample_df = pd.DataFrame({
        "Instance Type": ["m4.large","c4.xlarge","r4.2xlarge","t2.medium","m5.xlarge",
                          "c5.2xlarge","r5.large","i3.large","m6i.4xlarge","t3.micro"],
        "OS":    ["Linux","Linux","Linux","Windows","Linux","RHEL","Linux","Linux","Linux","Linux"],
        "Cost":  [73.2,145.0,389.0,35.0,140.0,250.0,92.0,114.0,584.0,8.5],
        "Usage": [720]*10,
        "Region":["eu-west-1"]*10,
        "Account":["prod-eu"]*10,
        "Application":["WebFrontend","APIGateway","DataWarehouse","DevServer","AppServer",
                       "BatchProcessor","Analytics","Storage","WebFrontend","CI-CD"],
    })
    st.dataframe(sample_df, use_container_width=True, hide_index=True)
    st.download_button("⬇ Download Sample CSV", sample_df.to_csv(index=False).encode(),
                       "sample_ec2_input.csv", "text/csv")


# ══════════════════════════════════════════════════════════════════════════
# PROCESSING STEP
# ══════════════════════════════════════════════════════════════════════════
if run and uploaded:
    with st.spinner("Reading file and detecting columns…"):
        try:
            lr: LoadResult = load_file(uploaded, uploaded.name)
            st.session_state["load_result"] = lr
            st.session_state["result"] = None   # reset previous result

            for w in lr.warnings:
                st.markdown(f'<div class="box-warn">⚠️ {w}</div>', unsafe_allow_html=True)

            if not lr.needs_manual_mapping:
                result = process(lr.df, region=sel_region)
                st.session_state["result"] = result
                label = REGION_LABELS.get(sel_region, sel_region)
                st.markdown(
                    f'<div class="box-ok">✅ Processed <strong>{len(result):,}</strong> rows '
                    f'from <code>{uploaded.name}</code>  ·  Region: <strong>{label}</strong></div>',
                    unsafe_allow_html=True)

        except ValueError as ve:
            st.session_state["load_result"] = None
            st.session_state["result"] = None
            st.markdown(f'<div class="box-err">❌ <strong>Error:</strong> {ve}</div>',
                        unsafe_allow_html=True)
        except Exception:
            st.session_state["load_result"] = None
            st.session_state["result"] = None
            tb = traceback.format_exc()
            log.error(tb)
            st.markdown(
                f'<div class="box-err">❌ Unexpected error — check file format.'
                f'<details><summary>Details</summary><pre style="font-size:.7rem">{tb[-600:]}</pre></details></div>',
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# FAILSAFE: Manual column mapping UI
# ══════════════════════════════════════════════════════════════════════════
lr: LoadResult | None = st.session_state.get("load_result")

if lr and lr.needs_manual_mapping and st.session_state.get("result") is None:
    st.markdown("---")
    st.markdown("""
    <div class="box-warn">
      ⚠️ <strong>Column detection incomplete.</strong>
      Some required columns could not be automatically identified.
      Please map them manually below.
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Manual Column Mapping</div>', unsafe_allow_html=True)
    st.caption(f"Columns found in file: **{', '.join(lr.df.columns.tolist())}**")

    available_raw = ["— not in file —"] + lr.df.columns.tolist()
    canonical_needed = sorted(lr.missing_required)
    user_mapping: dict[str, str] = {}

    mapping_cols = st.columns(min(len(canonical_needed), 4))
    for i, canon in enumerate(canonical_needed):
        with mapping_cols[i % 4]:
            # Pre-select best guess
            guess_idx = 0
            for j, raw in enumerate(available_raw[1:], start=1):
                if canon.lower().replace(" ", "") in raw.lower().replace(" ", ""):
                    guess_idx = j; break
            sel = st.selectbox(f"→ {canon}", available_raw, index=guess_idx, key=f"map_{canon}")
            if sel != "— not in file —":
                user_mapping[sel] = canon

    if st.button("✅ Apply Mapping & Process", type="primary"):
        if len(user_mapping) < len(canonical_needed):
            st.markdown(
                f'<div class="box-err">❌ Please map all required columns: '
                f'{", ".join(canonical_needed)}</div>', unsafe_allow_html=True)
        else:
            try:
                new_lr = lr.apply_mapping(user_mapping)
                if new_lr.needs_manual_mapping:
                    st.markdown(
                        f'<div class="box-err">❌ Still missing: '
                        f'{", ".join(sorted(new_lr.missing_required))}</div>',
                        unsafe_allow_html=True)
                else:
                    result = process(new_lr.df, region=st.session_state["region_id"])
                    st.session_state["result"] = result
                    st.session_state["load_result"] = new_lr
                    st.markdown(
                        f'<div class="box-ok">✅ Processed <strong>{len(result):,}</strong> rows '
                        f'with manual column mapping.</div>', unsafe_allow_html=True)
                    st.rerun()
            except Exception as ex:
                st.markdown(f'<div class="box-err">❌ {ex}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# RESULTS PANEL
# ══════════════════════════════════════════════════════════════════════════
df_out: pd.DataFrame | None = st.session_state.get("result")

if df_out is not None:
    k = kpis(df_out)

    # KPI row
    st.markdown('<div class="sec" style="margin-top:1.1rem">Summary</div>',
                unsafe_allow_html=True)
    render_kpis(k)

    st.markdown("---")

    # ── Legend ────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="legend">
      <span><span class="legend-dot" style="background:#D1FAE5"></span>Savings ≥20%</span>
      <span><span class="legend-dot" style="background:#FEF3C7"></span>Savings 5–20%</span>
      <span><span class="legend-dot" style="background:#FEE2E2"></span>Old Generation</span>
      <span><span class="legend-dot" style="background:#D1FAE5;border:1px solid #065f46"></span>Latest Gen</span>
    </div>""", unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────
    st.markdown('<div class="sec">Filter & Search</div>', unsafe_allow_html=True)
    fc1, fc2, fc3, fc4, fc5 = st.columns([3, 2, 2, 2, 2])

    with fc1:
        srch = st.text_input("Search", placeholder="instance, app, account…",
                             label_visibility="collapsed")
    with fc2:
        rgn_vals = (["All"] + sorted(df_out["Region"].dropna().unique().tolist())
                    if "Region" in df_out.columns else ["All"])
        sel_rgn = st.selectbox("Region", rgn_vals, label_visibility="collapsed")
    with fc3:
        fam_vals = ["All"] + sorted({
            str(x).split(".")[0] for x in df_out["Instance Type"].dropna()
            if "." in str(x)
        })
        sel_fam = st.selectbox("Family", fam_vals, label_visibility="collapsed")
    with fc4:
        sav_opts = ["All", "≥20% High", "5–20% Med", "0–5% Low", "No Rec"]
        sel_sav  = st.selectbox("Savings", sav_opts, label_visibility="collapsed")
    with fc5:
        gen_opts = ["All", "Old Gen", "Current", "Latest"]
        sel_gen  = st.selectbox("Generation", gen_opts, label_visibility="collapsed")

    # ── Apply filters ──────────────────────────────────────────────────────
    view = df_out.copy()

    if srch:
        mask = pd.Series(False, index=view.index)
        for col in view.select_dtypes(include="object").columns:
            mask |= view[col].astype(str).str.contains(srch, case=False, na=False)
        view = view[mask]

    if "Region" in view.columns and sel_rgn != "All":
        view = view[view["Region"] == sel_rgn]
    if sel_fam != "All":
        view = view[view["Instance Type"].astype(str).str.startswith(sel_fam + ".", na=False)]
    if sel_gen != "All" and "Generation Flag" in view.columns:
        view = view[view["Generation Flag"] == sel_gen]

    num_sav = pd.to_numeric(view.get("Savings Opportunity (%)", pd.Series()), errors="coerce")
    if sel_sav == "≥20% High":
        view = view[num_sav >= 20]
    elif sel_sav == "5–20% Med":
        view = view[(num_sav >= 5) & (num_sav < 20)]
    elif sel_sav == "0–5% Low":
        view = view[(num_sav >= 0) & (num_sav < 5)]
    elif sel_sav == "No Rec":
        view = view[num_sav.isna()]

    st.caption(f"Showing **{len(view):,}** of **{len(df_out):,}** rows")

    # ── Table with colour styling ─────────────────────────────────────────
    st.markdown('<div class="sec" style="margin-top:.3rem">Recommendations</div>',
                unsafe_allow_html=True)

    def _colour_sav(v):
        try:
            f = float(v)
            if f >= 20:  return "color:#00955c;font-weight:700"
            if f >= 5:   return "color:#d97706;font-weight:600"
            if f >= 0:   return "color:#374151"
            return "color:#dc2626;font-weight:600"
        except Exception:
            return "color:#9ca3af"

    def _colour_gen(v):
        if v == "Old Gen":  return "color:#dc2626;font-weight:700"
        if v == "Latest":   return "color:#00955c;font-weight:600"
        return "color:#6b7280"

    def _fmt(x, fmt_str="${}"):
        if pd.isna(x) or x in (None, "N/A") or str(x) in ("nan", ""):
            return "N/A"
        try:
            return fmt_str.format(f"{float(x):.4f}")
        except Exception:
            return str(x)

    fmt_map: dict = {
        "On-Demand Price ($)":     lambda x: _fmt(x, "${}"),
        "Alt 1 Price ($)":         lambda x: _fmt(x, "${}"),
        "Alt 2 Price ($)":         lambda x: _fmt(x, "${}"),
        "Savings Opportunity (%)": lambda x: (
            "N/A" if pd.isna(x) or x in (None, "N/A") else f"{float(x):.1f}%"
        ),
    }
    if "Cost"  in view.columns:
        fmt_map["Cost"]  = lambda x: f"${x:,.2f}" if pd.notna(x) else "—"
    if "Usage" in view.columns:
        fmt_map["Usage"] = lambda x: f"{x:,.0f}"  if pd.notna(x) else "—"

    style_cols = {}
    if "Savings Opportunity (%)" in view.columns:
        style_cols["Savings Opportunity (%)"] = _colour_sav
    if "Generation Flag" in view.columns:
        style_cols["Generation Flag"] = _colour_gen

    try:
        styled = view.style.format(fmt_map, na_rep="N/A")
        for col_name, fn in style_cols.items():
            if col_name in view.columns:
                styled = styled.applymap(fn, subset=[col_name])
        styled = styled.set_properties(**{"font-size": "0.79rem"})
    except Exception:
        styled = view   # fallback: plain df if styling fails

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=500,      # fixed height enables virtual scrolling (sticky header)
    )

    # ── Downloads ─────────────────────────────────────────────────────────
    st.markdown("---")
    d1, d2, _ = st.columns([2, 2, 5])
    export_df = apply_na_fill(df_out)
    region_label = REGION_LABELS.get(st.session_state.get("region_id", DEFAULT_REGION), "")

    with d1:
        xl = build_excel(export_df, region_label)
        st.download_button(
            "⬇ Download Excel (.xlsx)",
            data=xl,
            file_name="ec2_finops_recommendations.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with d2:
        csv_b = export_df.to_csv(index=False).encode()
        st.download_button(
            "⬇ Download CSV",
            data=csv_b,
            file_name="ec2_finops_recommendations.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # ── Savings chart ──────────────────────────────────────────────────────
    st.markdown("---")
    ch1, ch2 = st.columns(2)

    with ch1:
        st.markdown('<div class="sec">Avg Savings by Instance Family</div>',
                    unsafe_allow_html=True)
        chart_df = df_out.copy()
        chart_df["_s"] = pd.to_numeric(
            chart_df.get("Savings Opportunity (%)", pd.Series()), errors="coerce")
        chart_df["Fam"] = chart_df["Instance Type"].apply(
            lambda x: str(x).split(".")[0] if "." in str(x) else str(x))
        grp = (chart_df.groupby("Fam")["_s"].mean()
               .round(1).sort_values(ascending=False).dropna())
        if not grp.empty:
            st.bar_chart(grp.rename("Avg Savings (%)"), height=260,
                         use_container_width=True)
        else:
            st.info("No savings data to chart.")

    with ch2:
        st.markdown('<div class="sec">Generation Distribution</div>',
                    unsafe_allow_html=True)
        if "Generation Flag" in df_out.columns:
            gen_counts = df_out["Generation Flag"].value_counts()
            order = ["Old Gen", "Current", "Latest", "N/A"]
            gen_counts = gen_counts.reindex(
                [o for o in order if o in gen_counts.index])
            if not gen_counts.empty:
                st.bar_chart(gen_counts.rename("Count"), height=260,
                             use_container_width=True)

else:
    # ── Empty state ────────────────────────────────────────────────────────
    st.markdown("""
    <div style="text-align:center;padding:4rem 2rem;color:#9ca3af">
      <div style="font-size:2.5rem;margin-bottom:.75rem">📤</div>
      <div style="font-size:1rem;font-weight:500;color:#4b5563">
        Upload an EC2 cost export and click <strong>Process</strong>
      </div>
      <div style="font-size:.81rem;margin-top:.4rem">
        Works with AWS Cost Explorer, CUR exports, and custom billing spreadsheets<br>
        Handles 10,000+ rows · Accepts messy column names · Never guesses prices
      </div>
    </div>
    """, unsafe_allow_html=True)
