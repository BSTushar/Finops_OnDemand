from __future__ import annotations
import html
import inspect
import logging
import math
import re
import pandas as pd
import streamlit as st
from data_loader import OS_COLUMN_NONE_OPTION, LoadResult, analyze_load, dataframe_from_bytes, finalize_binding, load_file
from excel_export import build_excel, sanitize_formula_injection_dataframe, savings_numeric
try:
    from instance_api import canonicalize_instance_api_name
except Exception:
    # Defensive fallback: keep UI alive even if import resolution is broken
    # in a stale local copy/environment.
    def canonicalize_instance_api_name(value: object) -> str | None:  # type: ignore[no-redef]
        return None
from processor import apply_na_fill, process
from pricing_engine import CACHE_METADATA, DECISION_SUPPORT_NOTE, DEFAULT_REGION, PRICING_SOURCE_LABEL, RDS_PRICING_NOTE, REGION_LABELS, SUPPORTED_REGIONS, cache_age_days, cache_is_stale, cost_disclaimer_text
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs
logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


def _ui_stretch_kwargs(widget=st.dataframe) -> dict:
    """Streamlit ≥1.46 prefers width='stretch'; older versions use use_container_width."""
    if 'width' in inspect.signature(widget).parameters:
        return {'width': 'stretch'}
    return {'use_container_width': True}


def _dataframe_for_streamlit_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """PyArrow rejects object columns that mix bytes, int, str, etc. Coerce object cols to pandas string dtype."""
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype != object:
            continue

        def _scalar(v: object):
            if v is None:
                return pd.NA
            try:
                if pd.isna(v):
                    return pd.NA
            except (TypeError, ValueError):
                pass
            if isinstance(v, bytes):
                try:
                    return v.decode('utf-8', errors='replace')
                except Exception:
                    return str(v)
            return str(v)

        out[c] = out[c].map(_scalar).astype('string')
    return out


def _cell_display_generic(v: object) -> str:
    if v is None:
        return ''
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    if isinstance(v, bytes):
        try:
            return v.decode('utf-8', errors='replace')
        except Exception:
            return str(v)
    return str(v)


def _format_display_money_cell(v: object, *, hourly: bool) -> str:
    """Prefix $ for numeric hourly (4 dp) or cost (2 dp); pass through N/A."""
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t.startswith('$'):
            return t
        try:
            x = float(t.replace(',', ''))
        except ValueError:
            return t
    else:
        try:
            x = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return str(v)
    if not math.isfinite(x):
        return 'N/A'
    if hourly:
        return f'${x:.4f}'
    return f'${x:,.2f}'


def _format_display_discount_pct_cell(v: object) -> str:
    """Table display for Discount % (N/A, No Discount, or n.n%)."""
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t == 'No Discount':
            return 'No Discount'
        if t.endswith('%'):
            return t
        try:
            return f'{float(t.replace("%", "").replace(",", "").strip()):.1f}%'
        except ValueError:
            return t
    try:
        x = float(v)
        if math.isfinite(x):
            return f'{x:.1f}%'
    except (TypeError, ValueError):
        pass
    return str(v)


def _format_display_savings_cell(v: object) -> str:
    """Append % for numeric savings; keep N/A and No Savings."""
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t == 'No Savings':
            return 'No Savings'
        if t.endswith('%'):
            return t
        try:
            return f'{float(t.replace("%", "").replace(",", "").strip()):.1f}%'
        except ValueError:
            return t
    try:
        x = float(v)  # type: ignore[arg-type]
        if math.isfinite(x):
            return f'{x:.1f}%'
    except (TypeError, ValueError):
        pass
    return str(v)


def _enriched_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Display-only string view for Streamlit: $ on price/cost columns, % on savings columns."""
    if df.empty:
        return df.copy()
    parts: list[pd.Series] = []
    for j in range(df.shape[1]):
        name = df.columns[j]
        ser = df.iloc[:, j]
        cn = str(name)
        if cn == 'Actual Cost ($)':
            vals = [_format_display_money_cell(x, hourly=False) for x in ser]
        elif cn == 'Discount %':
            vals = [_format_display_discount_pct_cell(x) for x in ser]
        elif 'Price ($/hr)' in cn:
            vals = [_format_display_money_cell(x, hourly=True) for x in ser]
        elif 'Savings %' in cn:
            vals = [_format_display_savings_cell(x) for x in ser]
        else:
            vals = [_cell_display_generic(x) for x in ser]
        parts.append(pd.Series(vals, index=df.index, name=name, dtype=str))
    return pd.concat(parts, axis=1)


st.set_page_config(page_title='FinOps Optimizer', page_icon='◆', layout='wide', initial_sidebar_state='collapsed')
FINOPS_UI_CSS = """
<style>
html { color-scheme: light; }
#MainMenu { visibility: hidden; }
[data-testid="stFooter"] { visibility: hidden; }
section[data-testid="stApp"] {
  background: linear-gradient(180deg, #f8fafc 0%, #ffffff 32%, #f1f5f9 100%) !important;
}
section[data-testid="stApp"] .main .block-container {
  padding-top: 1.25rem;
  padding-bottom: 2rem;
  max-width: 1200px;
}

@keyframes finops-fade-up {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}
@keyframes finops-hero-glow {
  0%, 100% { opacity: 0.55; transform: scale(1) translateY(0); }
  50% { opacity: 0.9; transform: scale(1.03) translateY(-2px); }
}
@keyframes finops-divider-flow {
  0% { background-position: 0% 50%; }
  100% { background-position: 200% 50%; }
}
@keyframes finops-pill-pulse {
  0%, 100% { box-shadow: 0 1px 3px color-mix(in srgb, var(--st-text-color, #000) 8%, transparent); }
  50% { box-shadow: 0 2px 10px color-mix(in srgb, var(--st-primary-color, #0071e3) 22%, transparent), 0 1px 3px color-mix(in srgb, var(--st-text-color, #000) 6%, transparent); }
}

@media (prefers-reduced-motion: reduce) {
  .finops-hero::before,
  .finops-hero-inner .finops-eyebrow,
  .finops-hero-inner .finops-headline,
  .finops-hero-inner .finops-tagline,
  .finops-hero-inner .finops-pill,
  .finops-card.finops-trust-panel,
  .finops-trust-meta .finops-trust-chip,
  .finops-flow-step,
  .finops-divider,
  .finops-pipeline,
  .finops-pipeline-step--current,
  .finops-hero-inner .finops-pill--stale {
    animation: none !important;
  }
  .finops-hero::before { opacity: 0.5 !important; transform: none !important; }
}

.finops-hero {
  padding: 2.5rem 1rem 2rem;
  margin-bottom: 0.5rem;
  width: 100%;
  box-sizing: border-box;
  position: relative;
  overflow: hidden;
  border-radius: 0 0 24px 24px;
}
.finops-hero::before {
  content: "";
  position: absolute;
  left: -15%;
  right: -15%;
  top: -40%;
  height: 120%;
  pointer-events: none;
  background: radial-gradient(
    ellipse 75% 55% at 50% 0%,
    color-mix(in srgb, var(--st-primary-color, #0071e3) 22%, transparent),
    color-mix(in srgb, var(--st-primary-color, #0071e3) 6%, transparent) 42%,
    transparent 68%
  );
  animation: finops-hero-glow 14s ease-in-out infinite;
}
.finops-hero-inner {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
  max-width: 40rem;
  margin-left: auto;
  margin-right: auto;
  text-align: center;
  position: relative;
  z-index: 1;
}
.finops-hero-inner .finops-eyebrow {
  animation: finops-fade-up 0.55s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.finops-hero-inner .finops-headline {
  animation: finops-fade-up 0.62s cubic-bezier(0.22, 1, 0.36, 1) 0.06s both;
}
.finops-hero-inner .finops-subheadline {
  animation: finops-fade-up 0.64s cubic-bezier(0.22, 1, 0.36, 1) 0.08s both;
}
.finops-hero-inner .finops-tagline {
  animation: finops-fade-up 0.68s cubic-bezier(0.22, 1, 0.36, 1) 0.12s both;
}
.finops-hero-inner .finops-hero-badges {
  animation: finops-fade-up 0.6s cubic-bezier(0.22, 1, 0.36, 1) 0.14s both;
}
.finops-hero-inner .finops-pill {
  animation: finops-fade-up 0.55s cubic-bezier(0.22, 1, 0.36, 1) 0.2s both;
  transition: transform 0.22s ease, border-color 0.22s ease, box-shadow 0.22s ease;
}
.finops-hero-inner .finops-pill:hover {
  transform: translateY(-2px) scale(1.02);
}
.finops-hero-inner .finops-pill--stale {
  animation: finops-fade-up 0.55s cubic-bezier(0.22, 1, 0.36, 1) 0.2s both, finops-pill-pulse 3.2s ease-in-out infinite 0.8s;
}
@media (prefers-reduced-motion: reduce) {
  .finops-hero-inner .finops-pill:hover { transform: none; }
}
.finops-eyebrow {
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  margin: 0 0 0.75rem;
  padding: 0;
  width: 100%;
  text-align: center;
  text-wrap: balance;
  color: color-mix(in srgb, var(--st-text-color, CanvasText) 72%, var(--st-background-color, Canvas));
}
.finops-headline {
  font-size: clamp(2.35rem, 5.5vw, 3.05rem);
  font-weight: 700;
  letter-spacing: -0.035em;
  line-height: 1.08;
  margin: 0 0 0.5rem;
  padding: 0;
  width: 100%;
  text-align: center;
  text-wrap: balance;
  color: #0f172a;
}
.finops-subheadline {
  font-size: clamp(1.05rem, 2.2vw, 1.25rem);
  font-weight: 500;
  line-height: 1.45;
  margin: 0 0 1rem;
  padding: 0;
  max-width: 38rem;
  width: 100%;
  margin-left: auto;
  margin-right: auto;
  text-align: center;
  text-wrap: balance;
  color: #334155;
}
.finops-hero-badges {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 0.5rem 0.6rem;
  margin: 0 0 1rem;
  width: 100%;
  max-width: 36rem;
}
.finops-hero-badge {
  display: inline-flex;
  align-items: center;
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 0.38rem 0.75rem;
  border-radius: 100px;
  border: 1px solid #cbd5e1;
  background: #ffffff;
  color: #475569;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
}
.finops-hero-badge--accent {
  border-color: color-mix(in srgb, var(--st-primary-color, #0068c9) 35%, #cbd5e1);
  color: #0f172a;
  background: color-mix(in srgb, var(--st-primary-color, #0068c9) 8%, #ffffff);
}
.finops-tagline {
  font-size: 1rem;
  font-weight: 400;
  margin: 0 0 1.15rem;
  line-height: 1.6;
  max-width: 34rem;
  width: 100%;
  margin-left: auto;
  margin-right: auto;
  padding: 0;
  text-align: center;
  text-wrap: balance;
  color: #64748b;
}
.finops-pill {
  display: inline-block;
  font-size: 0.75rem;
  font-weight: 500;
  padding: 0.4rem 0.95rem;
  border-radius: 100px;
  border: 1px solid color-mix(in srgb, var(--st-border-color, #88888840) 100%, transparent);
  box-shadow: 0 1px 3px color-mix(in srgb, var(--st-text-color, #000) 8%, transparent);
}
.finops-pill--fresh {
  font-weight: 600;
  color: var(--st-text-color, inherit);
  background: color-mix(in srgb, var(--st-green-background-color, #34c759) 16%, var(--st-secondary-background-color, Canvas) 84%);
  border-color: color-mix(in srgb, var(--st-green-color, #22c55e) 32%, var(--st-border-color, transparent));
}
.finops-pill--stale {
  font-weight: 600;
  color: var(--st-text-color, inherit);
  background: color-mix(in srgb, var(--st-orange-background-color, #ff9f0a) 22%, var(--st-secondary-background-color, Canvas) 78%);
  border-color: color-mix(in srgb, var(--st-orange-color, #f59e0b) 38%, var(--st-border-color, transparent));
  box-shadow: 0 0 0 1px color-mix(in srgb, var(--st-orange-color, #f59e0b) 12%, transparent), 0 2px 8px color-mix(in srgb, var(--st-orange-color, #f59e0b) 8%, transparent);
}

.finops-trust-panel {
  border-left: 4px solid var(--st-primary-color, #0071e3);
  padding: 1.25rem 1.35rem 1.35rem 1.35rem;
  margin-top: 0.35rem;
  animation: finops-fade-up 0.7s cubic-bezier(0.22, 1, 0.36, 1) 0.08s both;
  transition: box-shadow 0.28s ease, border-color 0.28s ease, transform 0.28s ease;
}
.finops-card.finops-trust-panel:hover {
  transform: translateY(-2px);
  box-shadow:
    0 4px 24px color-mix(in srgb, var(--st-text-color, #000) 8%, transparent),
    0 0 0 1px color-mix(in srgb, var(--st-primary-color, #0071e3) 18%, var(--st-border-color, transparent));
}
@media (prefers-reduced-motion: reduce) {
  .finops-card.finops-trust-panel:hover { transform: none; }
}
.finops-trust-snapshot { margin-bottom: 0; }
.finops-trust-snapshot-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.45rem 0.65rem;
  margin-bottom: 0.65rem;
}
.finops-trust-snapshot-title {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: color-mix(in srgb, var(--st-primary-color, #0071e3) 65%, var(--st-text-color));
}
.finops-trust-region-code {
  font-size: 0.75rem;
  font-weight: 600;
  padding: 0.2rem 0.45rem;
  border-radius: 6px;
  background: color-mix(in srgb, var(--st-primary-color) 12%, var(--st-secondary-background-color));
  color: var(--st-text-color);
  border: 1px solid color-mix(in srgb, var(--st-border-color) 65%, transparent);
}
.finops-trust-meta {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.5rem 0.65rem;
  margin-bottom: 0.25rem;
}
@media (max-width: 720px) {
  .finops-trust-meta { grid-template-columns: 1fr; }
}
.finops-trust-chip {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  justify-content: flex-start;
  min-width: 0;
  width: 100%;
  box-sizing: border-box;
  padding: 0.45rem 0.55rem;
  border-radius: 10px;
  background: color-mix(in srgb, var(--st-secondary-background-color) 88%, var(--st-background-color));
  border: 1px solid color-mix(in srgb, var(--st-border-color) 50%, transparent);
  animation: finops-fade-up 0.5s cubic-bezier(0.22, 1, 0.36, 1) both;
  transition: transform 0.22s ease, border-color 0.22s ease, box-shadow 0.22s ease, background 0.22s ease;
}
.finops-trust-meta .finops-trust-chip:nth-child(1) { animation-delay: 0.06s; }
.finops-trust-meta .finops-trust-chip:nth-child(2) { animation-delay: 0.12s; }
.finops-trust-meta .finops-trust-chip:nth-child(3) { animation-delay: 0.18s; }
.finops-trust-chip:hover {
  transform: translateY(-2px);
  border-color: color-mix(in srgb, var(--st-primary-color, #0071e3) 35%, var(--st-border-color));
  box-shadow: 0 3px 14px color-mix(in srgb, var(--st-text-color, #000) 7%, transparent);
}
@media (prefers-reduced-motion: reduce) {
  .finops-trust-chip:hover { transform: none; }
}
.finops-trust-chip-label {
  font-size: 0.625rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  margin-bottom: 0.2rem;
  color: color-mix(in srgb, var(--st-text-color) 62%, var(--st-background-color));
}
.finops-trust-chip-val {
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--st-text-color);
  line-height: 1.45;
  word-break: break-word;
}
.finops-trust-rule {
  height: 1px;
  border: none;
  margin: 0.85rem 0 0.75rem;
  background: color-mix(in srgb, var(--st-border-color) 85%, var(--st-primary-color));
  opacity: 0.85;
}
.finops-trust-panel .finops-trust-emph {
  font-size: 0.8125rem;
  font-weight: 600;
  line-height: 1.55;
  margin: 0 0 0.4rem;
  color: var(--st-text-color);
}
.finops-trust-panel .finops-card-body {
  font-size: 0.8125rem;
  line-height: 1.55;
  margin: 0.35rem 0 0;
}
.finops-trust-foot { margin-top: 0.45rem !important; }

#finops-fix-sheet-anchor { display: none; }
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) {
  margin-top: 0.75rem;
  margin-bottom: 0.25rem;
  padding: 1.15rem 1.25rem 1.35rem;
  border-radius: 16px;
  border: 1px solid color-mix(in srgb, var(--st-border-color) 82%, var(--st-primary-color));
  background: color-mix(in srgb, var(--st-secondary-background-color) 94%, var(--st-background-color));
  box-shadow:
    0 1px 0 color-mix(in srgb, var(--st-text-color) 4%, transparent),
    0 8px 28px color-mix(in srgb, var(--st-text-color) 6%, transparent);
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) [data-testid="stMarkdownContainer"] {
  text-align: left !important;
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) [data-testid="stHorizontalBlock"] {
  align-items: stretch !important;
  gap: 1.5rem !important;
  margin-top: 0.35rem !important;
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) [data-testid="column"] {
  min-width: 0;
  flex: 1 1 0 !important;
  padding: 1rem 1.1rem 1.15rem !important;
  border-radius: 14px;
  border: 1px solid color-mix(in srgb, var(--st-border-color) 75%, transparent);
  background: color-mix(in srgb, var(--st-background-color) 82%, var(--st-secondary-background-color));
  box-shadow: inset 0 1px 0 color-mix(in srgb, var(--st-text-color) 5%, transparent);
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) [data-testid="stFileUploader"] {
  margin-top: 0.35rem;
  padding: 0.65rem 0.7rem !important;
  border-radius: 12px;
  border: 1px dashed color-mix(in srgb, var(--st-border-color) 65%, var(--st-primary-color));
  background: color-mix(in srgb, var(--st-secondary-background-color) 55%, var(--st-background-color));
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) [data-testid="stFileUploader"] section {
  gap: 0.5rem !important;
}
div[data-testid="stVerticalBlock"]:has(#finops-fix-sheet-anchor) .finops-sec {
  font-size: 0.74rem;
  letter-spacing: 0.09em;
  margin: 0 0 0.45rem;
  color: color-mix(in srgb, var(--st-text-color) 80%, var(--st-background-color));
}
.finops-fix-hint-block {
  text-align: left;
  width: 100%;
  max-width: none;
  margin: 0 0 1rem;
  padding: 0.65rem 0 0.85rem;
  border-bottom: 1px solid color-mix(in srgb, var(--st-border-color) 88%, transparent);
}
.finops-fix-hint-kicker {
  display: block;
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.09em;
  text-transform: uppercase;
  margin: 0 0 0.4rem;
  color: color-mix(in srgb, var(--st-primary-color) 50%, var(--st-text-color));
}
.finops-fix-hint {
  font-size: 0.8125rem;
  line-height: 1.55;
  margin: 0;
  padding: 0;
  max-width: none;
  text-align: left;
  color: color-mix(in srgb, var(--st-text-color) 82%, var(--st-background-color));
}

/* Step 1: control row — top-align columns so labels + widgets line up (scoped via marker). */
#finops-home-toolbar-anchor { display: none; }
div[data-testid="stVerticalBlock"]:has(#finops-home-toolbar-anchor) [data-testid="stHorizontalBlock"] {
  align-items: flex-start !important;
}
div[data-testid="stVerticalBlock"]:has(#finops-home-toolbar-anchor) [data-testid="column"] {
  min-width: 0;
}
div[data-testid="stVerticalBlock"]:has(#finops-home-toolbar-anchor) [data-testid="column"] [role="radiogroup"] {
  flex-wrap: nowrap !important;
}

.finops-page-footer {
  margin-top: 2.75rem;
  padding: 1.15rem 1rem 2rem;
  text-align: center;
  font-size: 0.8125rem;
  color: color-mix(in srgb, var(--st-text-color) 58%, var(--st-background-color));
  border-top: 1px solid color-mix(in srgb, var(--st-border-color) 75%, transparent);
}
.finops-page-footer-brand {
  font-weight: 600;
  letter-spacing: -0.02em;
  color: var(--st-text-color);
}
.finops-page-footer-sep { margin: 0 0.45rem; opacity: 0.4; }
.finops-page-footer-team { font-weight: 500; color: color-mix(in srgb, var(--st-text-color) 85%, var(--st-background-color)); }

.finops-pipeline {
  margin: 0 auto 1.75rem;
  padding: 1rem 1.25rem 1.1rem;
  max-width: 52rem;
  border-radius: 16px;
  border: 1px solid #e2e8f0;
  background: #ffffff;
  box-shadow: 0 4px 24px rgba(15, 23, 42, 0.06);
}
.finops-pipeline-label {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.11em;
  text-transform: uppercase;
  color: #94a3b8;
  margin: 0 0 0.65rem;
  text-align: center;
}
.finops-pipeline-track {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-wrap: wrap;
  gap: 0.25rem 0.15rem;
}
.finops-pipeline-step {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 2.35rem;
  padding: 0.35rem 0.85rem;
  border-radius: 10px;
  font-size: 0.8125rem;
  font-weight: 600;
  color: #94a3b8;
  background: #f1f5f9;
  border: 1px solid transparent;
  transition: background 0.2s ease, color 0.2s ease, border-color 0.2s ease;
}
.finops-pipeline-step--todo {
  color: #94a3b8;
  background: #f8fafc;
  border-color: #e2e8f0;
}
.finops-pipeline-step--current {
  color: #0f172a;
  background: color-mix(in srgb, var(--st-primary-color, #0068c9) 12%, #ffffff);
  border-color: color-mix(in srgb, var(--st-primary-color, #0068c9) 45%, #e2e8f0);
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--st-primary-color, #0068c9) 15%, transparent);
  transition: background-color 0.35s ease, border-color 0.35s ease, box-shadow 0.35s ease, transform 0.35s ease;
  animation: finops-pipeline-current-pulse 2.5s ease-in-out infinite;
}
@keyframes finops-pipeline-current-pulse {
  0%, 100% { box-shadow: 0 0 0 2px color-mix(in srgb, var(--st-primary-color, #0068c9) 18%, transparent); }
  50% { box-shadow: 0 0 0 4px color-mix(in srgb, var(--st-primary-color, #0068c9) 8%, transparent), 0 4px 14px color-mix(in srgb, var(--st-primary-color, #0068c9) 12%, transparent); }
}
.finops-pipeline-step--done,
.finops-pipeline-step--todo {
  transition: background-color 0.35s ease, border-color 0.35s ease, color 0.35s ease;
}
.finops-pipeline-status {
  display: block;
  margin: 0 0 0.5rem;
  font-size: 0.8125rem;
  font-weight: 600;
  text-align: center;
  color: #475569;
}
.finops-pipeline-status strong {
  color: color-mix(in srgb, var(--st-primary-color, #0068c9) 55%, #0f172a);
}
.finops-pipeline-step--done {
  color: #166534;
  background: #ecfdf5;
  border-color: #bbf7d0;
}
.finops-pipeline-connector {
  width: 1.25rem;
  height: 2px;
  background: #e2e8f0;
  flex-shrink: 0;
  border-radius: 1px;
}
.finops-pipeline-connector--active {
  background: linear-gradient(90deg, #86efac, #22c55e);
}
.finops-pipeline-note {
  display: block;
  margin-top: 0.65rem;
  font-size: 0.75rem;
  line-height: 1.45;
  text-align: center;
  color: #64748b;
}
section[data-testid="stApp"] div[data-testid="stVerticalBlockBorderWrapper"] {
  border-radius: 16px !important;
  border: 1px solid #e2e8f0 !important;
  background: #ffffff !important;
  box-shadow: 0 2px 16px rgba(15, 23, 42, 0.05) !important;
  padding-top: 0.75rem !important;
  padding-bottom: 1rem !important;
  margin-bottom: 1.35rem !important;
}
.finops-trust-preface {
  font-size: 0.75rem;
  line-height: 1.45;
  margin: 0 0 0.75rem;
  padding: 0.45rem 0.55rem;
  border-radius: 8px;
  background: color-mix(in srgb, var(--st-primary-color) 8%, var(--st-secondary-background-color));
  color: color-mix(in srgb, var(--st-text-color) 88%, var(--st-background-color));
}
.finops-flow-step {
  display: flex;
  align-items: flex-start;
  gap: 1rem;
  margin: 2.65rem 0 1.15rem;
  padding-bottom: 0.45rem;
  border-bottom: 1px solid color-mix(in srgb, var(--st-border-color, #88888835) 100%, transparent);
  animation: finops-fade-up 0.55s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.finops-flow-step--optional {
  border-bottom-style: dashed;
  opacity: 0.97;
}
.finops-flow-badge-opt {
  flex-shrink: 0;
  font-size: 0.6rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 0.4rem 0.55rem;
  border-radius: 8px;
  line-height: 1.2;
  margin-top: 0.12rem;
  background: color-mix(in srgb, var(--st-secondary-background-color) 82%, var(--st-primary-color));
  color: var(--st-text-color);
  border: 1px solid color-mix(in srgb, var(--st-border-color) 65%, transparent);
}
.finops-flow-num {
  flex-shrink: 0;
  width: 2rem;
  height: 2rem;
  border-radius: 50%;
  background: var(--st-primary-color, #0071e3);
  color: #fff;
  font-size: 0.85rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  justify-content: center;
  line-height: 1;
  transition: transform 0.25s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.25s ease;
  box-shadow: 0 2px 8px color-mix(in srgb, var(--st-primary-color, #0071e3) 35%, transparent);
}
.finops-flow-step:hover .finops-flow-num {
  transform: scale(1.08);
  box-shadow: 0 4px 16px color-mix(in srgb, var(--st-primary-color, #0071e3) 45%, transparent);
}
@media (prefers-reduced-motion: reduce) {
  .finops-flow-step:hover .finops-flow-num { transform: none; }
}
.finops-flow-title {
  font-size: 1.4rem;
  font-weight: 700;
  letter-spacing: -0.025em;
  margin: 0;
  color: #0f172a;
}
.finops-flow-sub {
  font-size: 0.9375rem;
  margin: 0.4rem 0 0;
  line-height: 1.5;
  color: #64748b;
}

.finops-card {
  background: var(--st-secondary-background-color, rgba(128, 128, 128, 0.06));
  border: 1px solid color-mix(in srgb, var(--st-border-color, #88888840) 100%, transparent);
  border-radius: 18px;
  padding: 1.5rem 1.75rem;
  margin: 1.5rem 0 0.5rem;
  box-shadow: 0 2px 12px color-mix(in srgb, var(--st-text-color, #000) 6%, transparent);
  color: var(--st-text-color, inherit);
  transition: box-shadow 0.28s ease, border-color 0.28s ease, transform 0.28s ease;
}
.finops-card:not(.finops-trust-panel):hover {
  border-color: color-mix(in srgb, var(--st-border-color, #88888840) 70%, var(--st-primary-color, #0071e3));
  box-shadow: 0 6px 20px color-mix(in srgb, var(--st-text-color, #000) 9%, transparent);
}
.finops-card .finops-card-title { font-size: 0.9rem; font-weight: 600; margin: 0 0 0.65rem; color: var(--st-text-color, inherit); }
.finops-card .finops-card-body { font-size: 0.8125rem; line-height: 1.55; margin: 0.4rem 0 0; color: color-mix(in srgb, var(--st-text-color, CanvasText) 76%, var(--st-background-color, Canvas)); }
.finops-card code { font-size: 0.8em; padding: 0.1em 0.35em; border-radius: 4px; background: color-mix(in srgb, var(--st-background-color, #f5f5f5) 94%, var(--st-text-color, #000) 6%); color: var(--st-text-color, inherit); }

.finops-sec {
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin: 0 0 0.5rem;
  color: color-mix(in srgb, var(--st-text-color, CanvasText) 68%, var(--st-background-color, Canvas));
}

.finops-metric {
  border-radius: 14px;
  padding: 1.05rem 1.2rem;
  transition: transform 0.25s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.25s ease, border-color 0.25s ease;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
  color: #0f172a;
}
.finops-metric:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(15, 23, 42, 0.08);
  border-color: #cbd5e1;
}
@media (prefers-reduced-motion: reduce) {
  .finops-metric:hover { transform: translateY(-1px); }
}
.finops-metric-label {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #64748b;
}
.finops-metric-value {
  font-size: 1.4rem;
  font-weight: 700;
  margin-top: 0.3rem;
  color: #0f172a;
  letter-spacing: -0.02em;
}
.finops-metric--savings {
  background: linear-gradient(145deg, #ecfdf5 0%, #f0fdf4 100%);
  border-color: #bbf7d0;
}
.finops-metric--savings .finops-metric-value { color: #166534; }
.finops-metric--savings .finops-metric-label { color: #15803d; }
.finops-metric--risk {
  background: linear-gradient(145deg, #fef2f2 0%, #fff1f2 100%);
  border-color: #fecaca;
}
.finops-metric--risk .finops-metric-value { color: #b91c1c; }
.finops-metric--risk .finops-metric-label { color: #dc2626; }
.finops-metric--neutral {
  background: #f8fafc;
  border-color: #e2e8f0;
}
.finops-kpi-strip-title {
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: #94a3b8;
  margin: 0.5rem 0 0.65rem;
}

.finops-alert {
  padding: 0.85rem 1.1rem;
  border-radius: 12px;
  font-size: 0.875rem;
  line-height: 1.5;
  margin: 0.5rem 0;
  border: 1px solid color-mix(in srgb, var(--st-border-color, #88888835) 100%, transparent);
  color: var(--st-text-color, inherit);
  animation: finops-fade-up 0.45s cubic-bezier(0.22, 1, 0.36, 1) both;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.finops-alert:hover {
  transform: translateX(2px);
  box-shadow: 0 2px 12px color-mix(in srgb, var(--st-text-color, #000) 6%, transparent);
}
@media (prefers-reduced-motion: reduce) {
  .finops-alert { animation: none; }
  .finops-alert:hover { transform: none; }
}
.finops-alert--ok {
  background: color-mix(in srgb, var(--st-green-background-color, #34c759) 14%, var(--st-secondary-background-color, transparent));
  border-color: color-mix(in srgb, var(--st-green-color, #34c759) 28%, var(--st-border-color, transparent));
}
.finops-alert--warn {
  background: color-mix(in srgb, var(--st-orange-background-color, #ff9f0a) 14%, var(--st-secondary-background-color, transparent));
  border-color: color-mix(in srgb, var(--st-orange-color, #ff9f0a) 30%, var(--st-border-color, transparent));
}
.finops-alert--err {
  background: color-mix(in srgb, var(--st-red-background-color, #ff3b30) 12%, var(--st-secondary-background-color, transparent));
  border-color: color-mix(in srgb, var(--st-red-color, #ff3b30) 26%, var(--st-border-color, transparent));
}

.finops-divider--section {
  margin: 1.5rem 0 1.35rem;
}
.finops-divider {
  margin: 2rem 0;
  border: none;
  height: 2px;
  border-radius: 2px;
  background: linear-gradient(
    90deg,
    transparent,
    color-mix(in srgb, var(--st-primary-color, #0071e3) 45%, var(--st-border-color, #88888840)),
    color-mix(in srgb, var(--st-border-color, #88888840) 100%, transparent),
    color-mix(in srgb, var(--st-primary-color, #0071e3) 35%, var(--st-border-color, #88888840)),
    transparent
  );
  background-size: 200% 100%;
  animation: finops-divider-flow 10s linear infinite;
  opacity: 0.85;
}

div[data-testid="stVerticalBlock"]:has(#finops-enriched-df-anchor) [data-testid="stDataFrame"] {
  max-height: 500px;
  overflow: auto;
  border-radius: 12px;
  border: 1px solid var(--st-dataframe-border-color, var(--st-border-color, rgba(128, 128, 128, 0.25)));
  background: var(--st-secondary-background-color, rgba(255, 255, 255, 0.98));
  color: var(--st-text-color, inherit);
}
div[data-testid="stVerticalBlock"]:has(#finops-enriched-df-anchor) [data-testid="stDataFrame"] [class*="gdg-c1tqibwd"] {
  position: sticky !important;
  top: 0 !important;
  z-index: 6 !important;
  background: var(--st-dataframe-header-background-color, var(--st-secondary-background-color, rgba(240, 240, 240, 0.98))) !important;
}
</style>
"""
st.markdown(FINOPS_UI_CSS, unsafe_allow_html=True)


def _flow_step(num: int, title: str, subtitle: str='') -> None:
    sub = f'<p class="finops-flow-sub">{subtitle}</p>' if subtitle else ''
    st.markdown(f'<div class="finops-flow-step"><span class="finops-flow-num">{num}</span><div><p class="finops-flow-title">{title}</p>{sub}</div></div>', unsafe_allow_html=True)


def _flow_optional(title: str, subtitle: str='') -> None:
    sub = f'<p class="finops-flow-sub">{subtitle}</p>' if subtitle else ''
    st.markdown(
        f'<div class="finops-flow-step finops-flow-step--optional"><span class="finops-flow-badge-opt">Optional</span><div><p class="finops-flow-title">{title}</p>{sub}</div></div>',
        unsafe_allow_html=True,
    )


def _sync_auto_binding(lr: LoadResult | None) -> None:
    if lr is None or st.session_state.get('binding') is not None:
        return
    if lr.needs_instance_pick or lr.needs_os_pick:
        return
    if lr.binding is None:
        return
    if len(lr.cost_candidates) > 1 and lr.binding.actual_cost is None:
        return
    st.session_state['binding'] = lr.binding


def _pipeline_step_index(lr: LoadResult | None) -> int:
    if st.session_state.get('result') is not None:
        return 3
    if lr is None:
        return 0
    if st.session_state.get('binding') is None:
        return 1
    return 2


def _pipeline_bar_html(active: int) -> str:
    labels = ['Upload', 'Map', 'Analyze', 'Export']
    here = html.escape(labels[active])
    parts = [
        '<div class="finops-pipeline" role="navigation" aria-label="Workflow progress" aria-live="polite">',
        '<p class="finops-pipeline-label">Workflow</p>',
        f'<span class="finops-pipeline-status">You are here: <strong>{here}</strong></span>',
        '<div class="finops-pipeline-track">',
    ]
    for i, lab in enumerate(labels):
        if i > 0:
            conn = 'finops-pipeline-connector finops-pipeline-connector--active' if active >= i else 'finops-pipeline-connector'
            parts.append(f'<span class="{conn}" aria-hidden="true"></span>')
        if active > i:
            cls = 'finops-pipeline-step finops-pipeline-step--done'
        elif active == i:
            cls = 'finops-pipeline-step finops-pipeline-step--current'
        else:
            cls = 'finops-pipeline-step finops-pipeline-step--todo'
        parts.append(f'<span class="{cls}">{html.escape(lab)}</span>')
    parts.append(
        '</div><span class="finops-pipeline-note">Updates automatically after Continue, mapping, enrichment, and export. Optional merge is always above when you need two files.</span></div>'
    )
    return ''.join(parts)


_OLD_GEN_FAM_RE = re.compile(r'^(m[3-5]|c[3-5]|r[3-5]|t[1-3])([a-z][a-z0-9]*)?$', re.I)
_GRAV_ALT_RE = re.compile(r'[mcrtir]\d+g\.', re.I)


def _instance_family_token(cell: object) -> str:
    s = str(cell).strip().lower()
    if not s or s in ('nan', 'none', 'n/a'):
        return ''
    if s.startswith('db.') and s.count('.') >= 2:
        return s.split('.')[1]
    if '.' in s:
        return s.split('.')[0]
    return ''


def _resolve_instance_column_for_view(df: pd.DataFrame, bound_instance_col: str | None) -> str | None:
    """
    Prefer mapped instance column; fallback by scanning for AWS API-like values.
    This keeps EC2/RDS filters functional even when binding state is stale.
    """
    if bound_instance_col and bound_instance_col in df.columns:
        return bound_instance_col
    if df.empty:
        return None
    best_col: str | None = None
    best_ratio = 0.0
    sample_n = min(len(df), 2000)
    for c in df.columns:
        ser = df[c].iloc[:sample_n]
        non_empty = 0
        valid = 0
        for v in ser:
            if v is None:
                continue
            try:
                if pd.isna(v):
                    continue
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            if not s or s.lower() in ('nan', 'none', 'n/a'):
                continue
            non_empty += 1
            if canonicalize_instance_api_name(v) is not None:
                valid += 1
        if non_empty == 0:
            continue
        ratio = valid / non_empty
        if ratio > best_ratio:
            best_ratio = ratio
            best_col = str(c)
    return best_col if best_ratio >= 0.2 else None


def _is_rds_instance_cell(cell: object) -> bool:
    canon = canonicalize_instance_api_name(cell)
    return bool(canon and canon.startswith('db.'))


def _is_old_gen_instance_cell(cell: object) -> bool:
    fam = _instance_family_token(cell)
    if not fam:
        return False
    if len(fam) >= 2 and fam[-1] == 'g' and fam[-2].isdigit():
        return False
    return _OLD_GEN_FAM_RE.match(fam) is not None


def _row_graviton_alt(a1: object, a2: object) -> bool:
    for x in (a1, a2):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            continue
        s = str(x).strip().lower()
        if _GRAV_ALT_RE.search(s):
            return True
    return False


def _dashboard_strip_metrics(df: pd.DataFrame, inst_col: str | None) -> dict[str, float | int | None]:
    total_cost: float | None = None
    if 'Actual Cost ($)' in df.columns:
        ser = pd.to_numeric(df['Actual Cost ($)'], errors='coerce')
        s = float(ser.sum())
        if pd.notna(s) and s > 0:
            total_cost = s
        elif pd.notna(s):
            total_cost = 0.0
    avg_save: float | None = None
    if 'Alt1 Savings %' in df.columns:
        vals: list[float] = []
        for x in df['Alt1 Savings %']:
            v = _savings_for_kpi(x)
            if v is not None and v > 0:
                vals.append(v)
        if vals:
            avg_save = sum(vals) / len(vals)
    old_gen = 0
    grav = 0
    if inst_col and inst_col in df.columns:
        a1c = 'Alt1 Instance' if 'Alt1 Instance' in df.columns else None
        a2c = 'Alt2 Instance' if 'Alt2 Instance' in df.columns else None
        for _i, row in df.iterrows():
            if _is_old_gen_instance_cell(row.get(inst_col)):
                old_gen += 1
            a1 = row.get(a1c) if a1c else None
            a2 = row.get(a2c) if a2c else None
            if _row_graviton_alt(a1, a2):
                grav += 1
    return {'total_cost': total_cost, 'avg_save': avg_save, 'old_gen': old_gen, 'grav': grav}


def _render_dashboard_kpi_strip(m: dict[str, float | int | None]) -> None:
    st.markdown('<p class="finops-kpi-strip-title">Portfolio view (current filters)</p>', unsafe_allow_html=True)
    (k1, k2, k3, k4) = st.columns(4)
    tc = m['total_cost']
    tc_s = f'${tc:,.2f}' if tc is not None else '—'
    av = m['avg_save']
    av_s = f'{av:.1f}%' if av is not None else '—'
    with k1:
        st.markdown(
            f'<div class="finops-metric finops-metric--neutral"><div class="finops-metric-label">Total actual cost</div><div class="finops-metric-value">{tc_s}</div></div>',
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f'<div class="finops-metric finops-metric--savings"><div class="finops-metric-label">Avg Alt1 savings (where &gt; 0)</div><div class="finops-metric-value">{av_s}</div></div>',
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f'<div class="finops-metric finops-metric--risk"><div class="finops-metric-label">Older-gen families (heuristic)</div><div class="finops-metric-value">{int(m["old_gen"]):,}</div></div>',
            unsafe_allow_html=True,
        )
    with k4:
        st.markdown(
            f'<div class="finops-metric finops-metric--savings"><div class="finops-metric-label">Graviton in Alt1 / Alt2</div><div class="finops-metric-value">{int(m["grav"]):,}</div></div>',
            unsafe_allow_html=True,
        )


def _savings_for_kpi(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str) and v.strip() == 'No Savings':
        return None
    return savings_numeric(v)


def _series_savings_pct(col_name: str, df: pd.DataFrame) -> pd.Series:
    col = df.get(col_name)
    if col is None or len(col) == 0:
        return pd.Series(dtype=float)
    raw = [_savings_for_kpi(x) for x in col]
    return pd.to_numeric(pd.Series(raw), errors='coerce').dropna()


def _old_generation_detail_table(df: pd.DataFrame, inst_col: str | None) -> pd.DataFrame:
    """Rows in the current view whose instance cell matches the older-gen family heuristic (same as red KPI)."""
    if not inst_col or inst_col not in df.columns or df.empty:
        return pd.DataFrame()
    extra = [
        c
        for c in (
            'Pricing OS',
            'Discount %',
            'Current Price ($/hr)',
            'Alt1 Instance',
            'Alt1 Savings %',
            'Alt2 Instance',
            'Alt2 Savings %',
        )
        if c in df.columns
    ]
    cols = [inst_col] + [c for c in extra if c != inst_col]
    sub = df.loc[:, cols].copy()
    mask = sub[inst_col].map(_is_old_gen_instance_cell)
    out = sub.loc[mask].copy()
    out.reset_index(drop=True, inplace=True)
    return out


def kpis(df: pd.DataFrame) -> dict:
    s1 = _series_savings_pct('Alt1 Savings %', df)
    s2 = _series_savings_pct('Alt2 Savings %', df)
    return {
        'total': len(df),
        'avg1': float(s1.mean()) if len(s1) else None,
        'max1': float(s1.max()) if len(s1) else None,
        'max2': float(s2.max()) if len(s2) else None,
        'act_col': 'Actual Cost ($)' in df.columns,
    }


def render_kpis(k: dict):
    (c1, c2, c3, c4, c5) = st.columns(5)
    with c1:
        st.markdown(f"""<div class="finops-metric finops-metric--neutral"><div class="finops-metric-label">Rows in view</div><div class="finops-metric-value">{k['total']:,}</div></div>""", unsafe_allow_html=True)
    with c2:
        v = f"{k['avg1']:.1f}%" if k['avg1'] is not None else '—'
        st.markdown(f'<div class="finops-metric finops-metric--savings"><div class="finops-metric-label">Avg Alt1 savings</div><div class="finops-metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c3:
        v = f"{k['max1']:.1f}%" if k['max1'] is not None else '—'
        st.markdown(f'<div class="finops-metric finops-metric--savings"><div class="finops-metric-label">Max Alt1 savings</div><div class="finops-metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c4:
        v = f"{k['max2']:.1f}%" if k['max2'] is not None else '—'
        st.markdown(f'<div class="finops-metric finops-metric--savings"><div class="finops-metric-label">Max Alt2 savings</div><div class="finops-metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="finops-metric finops-metric--neutral"><div class="finops-metric-label">Actual cost column</div><div class="finops-metric-value">{('Yes' if k['act_col'] else 'No')}</div></div>""", unsafe_allow_html=True)
for (key, default) in (('load_result', None), ('result', None), ('region_id', DEFAULT_REGION), ('service', 'both'), ('cpu_filter', 'both'), ('cost_pick', None)):
    if key not in st.session_state:
        st.session_state[key] = default
lr: LoadResult | None = st.session_state.get('load_result')
_sync_auto_binding(lr)
stale = cache_is_stale()
pill = f'Dataset {cache_age_days()}d old — consider refresh' if stale else 'List prices loaded locally'
_pill_class = 'finops-pill finops-pill--stale' if stale else 'finops-pill finops-pill--fresh'
st.markdown(f'''<div class="finops-hero"><div class="finops-hero-inner">
<p class="finops-eyebrow">Airbus internal · FinOps decision support</p>
<h1 class="finops-headline">FinOps Optimizer</h1>
<p class="finops-subheadline">List-price intelligence for EC2 &amp; RDS — map once, enrich, export with full audit context.</p>
<div class="finops-hero-badges">
<span class="finops-hero-badge">Fully local · no external APIs</span>
<span class="finops-hero-badge finops-hero-badge--accent">EC2 &amp; RDS</span>
<span class="finops-hero-badge">Internal use</span>
</div>
<p class="finops-tagline">Upload or merge files, confirm columns, run enrichment, then download Excel or CSV. Figures are indicative from on-demand list prices for your selected region — not invoices.</p>
<span class="{_pill_class}">{html.escape(pill)}</span>
</div></div>''', unsafe_allow_html=True)
finops_pipeline_slot = st.empty()
_flow_optional(
    'Merge two spreadsheets first',
    'Skip this if you already have one file. Merge on a shared ID, then use <strong>Use merged data</strong> or upload the result in step 1.',
)
with st.container(border=True):
    st.markdown('<div id="finops-fix-sheet-anchor"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="finops-fix-hint-block"><span class="finops-fix-hint-kicker">How matching works</span>'
        '<p class="finops-fix-hint">Dataset 1 = primary row layout. Dataset 2 = extra fields (e.g. spend). Merge tries '
        '<strong>exact</strong> normalized keys first (lowercase, trimmed); if needed, the same '
        '<strong>core id</strong> token (one letter + ≥3 digits, no partial digit tail — e.g. '
        '<code>a1011</code> inside a longer string). One primary row out per row; duplicate secondary keys use the first row. Check merge warnings.</p></div>',
        unsafe_allow_html=True,
    )
    (fx1, fx2) = st.columns(2)
    with fx1:
        st.markdown('<div class="finops-sec">Dataset 1 · Primary</div>', unsafe_allow_html=True)
        fix_u1 = st.file_uploader('Primary spreadsheet', type=['csv', 'xlsx', 'xls'], key='fix_sheet_d1')
    with fx2:
        st.markdown('<div class="finops-sec">Dataset 2 · Supplement</div>', unsafe_allow_html=True)
        fix_u2 = st.file_uploader('Supplement spreadsheet', type=['csv', 'xlsx', 'xls'], key='fix_sheet_d2')
if fix_u1 and fix_u2:
    try:
        fix_d1 = dataframe_from_bytes(fix_u1.getvalue(), fix_u1.name)
        fix_d2 = dataframe_from_bytes(fix_u2.getvalue(), fix_u2.name)
    except ValueError as fve:
        st.markdown(f'<div class="finops-alert finops-alert--err">❌ Fix Your Sheet: {fve}</div>', unsafe_allow_html=True)
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
                st.markdown(f'<div class="finops-alert finops-alert--err">❌ {mve}</div>', unsafe_allow_html=True)
_fix_mdf = st.session_state.get('fix_merged_df')
if _fix_mdf is not None:
    st.markdown(f'<div class="finops-alert finops-alert--ok">✅ Merged preview: **{len(_fix_mdf):,}** rows × **{len(_fix_mdf.columns)}** columns</div>', unsafe_allow_html=True)
    for _fw in st.session_state.get('fix_merge_warnings', []):
        st.markdown(f'<div class="finops-alert finops-alert--warn">⚠️ {_fw}</div>', unsafe_allow_html=True)
    st.dataframe(_dataframe_for_streamlit_arrow(_fix_mdf.head(40)), **_ui_stretch_kwargs(), hide_index=True, height=360)
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
st.markdown('<div class="finops-divider finops-divider--section" role="separator" aria-hidden="true"></div>', unsafe_allow_html=True)
with st.container(border=True):
    _flow_step(1, 'Upload and set pricing', 'Add your file, choose region / service / CPU, then Continue. Your original column order is kept; we only append enrichment after the instance column.')
    st.markdown('<div class="finops-sec">Spreadsheet</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader('Drop your spreadsheet', type=['csv', 'xlsx', 'xls'], label_visibility='visible')
    st.caption('Needs a column with AWS API-style names (e.g. m5.large, db.r5.xlarge).')
    st.markdown('<div id="finops-home-toolbar-anchor"></div>', unsafe_allow_html=True)
    (reg_col, svc_col, cpu_col, go_col) = st.columns([2.5, 3.2, 2.0, 1.8], gap='medium')
    with reg_col:
        st.markdown('<div class="finops-sec">Pricing region</div>', unsafe_allow_html=True)
        region_opts = [f'{label}  [{rid}]' for (rid, label) in SUPPORTED_REGIONS]
        default_idx = [r for (r, _) in SUPPORTED_REGIONS].index(DEFAULT_REGION)
        sel_disp = st.selectbox('region', region_opts, index=default_idx, label_visibility='collapsed')
        sel_region = [r for (r, _) in SUPPORTED_REGIONS][region_opts.index(sel_disp)]
        st.session_state['region_id'] = sel_region
    with svc_col:
        st.markdown('<div class="finops-sec">Service</div>', unsafe_allow_html=True)
        st.session_state['service'] = st.radio(
            'svc',
            ['both', 'ec2', 'rds'],
            format_func=lambda x: {'ec2': 'EC2', 'rds': 'RDS', 'both': 'Both'}[x],
            label_visibility='collapsed',
            horizontal=True,
        )
    with cpu_col:
        st.markdown('<div class="finops-sec">CPU</div>', unsafe_allow_html=True)
        st.session_state['cpu_filter'] = st.selectbox('cpu', ['both', 'default', 'intel', 'graviton'], format_func=lambda x: {'both': 'Both', 'default': 'Default', 'intel': 'Intel', 'graviton': 'Graviton'}[x], label_visibility='collapsed')
    with go_col:
        st.markdown('<div class="finops-sec">Next</div>', unsafe_allow_html=True)
        run = st.button('Continue', type='primary', disabled=uploaded is None, **_ui_stretch_kwargs(st.button))
_reg = st.session_state.get('region_id', DEFAULT_REGION)
_rid_s = html.escape(_reg.strip().lower())
_rlabel_s = html.escape(REGION_LABELS.get(_reg.strip().lower(), _reg))
_asof_s = html.escape(CACHE_METADATA['last_updated'].strftime('%Y-%m-%d'))
_src_s = html.escape(PRICING_SOURCE_LABEL)
_disc_s = html.escape(cost_disclaimer_text(_reg))
_dec_s = html.escape(DECISION_SUPPORT_NOTE)
_rds_s = html.escape(RDS_PRICING_NOTE)
with st.container(border=True):
    st.markdown(f'''<div class="finops-card finops-trust-panel">
<p class="finops-trust-preface">The region and snapshot below are what we use when you <strong>Run enrichment</strong> (step 3) and in Excel exports—not a live bill.</p>
<div class="finops-trust-snapshot" role="status">
<div class="finops-trust-snapshot-head">
<span class="finops-trust-snapshot-title">Pricing snapshot</span>
<code class="finops-trust-region-code">{_rid_s}</code>
</div>
<div class="finops-trust-meta">
<span class="finops-trust-chip"><span class="finops-trust-chip-label">Region</span><span class="finops-trust-chip-val">{_rlabel_s}</span></span>
<span class="finops-trust-chip"><span class="finops-trust-chip-label">Source</span><span class="finops-trust-chip-val">{_src_s}</span></span>
<span class="finops-trust-chip"><span class="finops-trust-chip-label">Dataset as of</span><span class="finops-trust-chip-val">{_asof_s}</span></span>
</div>
</div>
<div class="finops-trust-rule" role="presentation"></div>
<p class="finops-trust-emph">{_disc_s}</p>
<p class="finops-card-body finops-trust-foot">{_rds_s}</p>
<p class="finops-card-body">{_dec_s}</p>
<p class="finops-card-body finops-trust-foot">Indicative savings only. Original columns preserved. Unknown API names or SKUs show <strong>N/A</strong>—never a guess.</p>
</div>''', unsafe_allow_html=True)
    with st.expander('Quick guide (same order as this page)', expanded=False):
        st.markdown(
            """
**Step 1 — Upload and set pricing**  
Pick **Pricing region** (list prices for that region), **Service** (**Both** is the default for mixed CURs — `db.*` rows always use RDS list SKUs), **CPU** (usually Both). Upload CSV or Excel, then **Continue**.

**Optional — Merge two files first** (at the top of the page, before step 1)  
Skip if you already have one table. Merge on a shared ID, then **Use merged data** or upload the merged file in step 1.

**Step 2 — Map columns** (only if the tool asks)  
Choose **instance / DB class**, **OS or engine**, and **actual cost** when prompted. Cost drives meaningful savings %.

**Step 3 — Run enrichment**  
Uses your region + service + CPU. If you change those after enriching, click **Run enrichment** again.

**Step 4 — Results and download**  
Filter the table, then **Excel** (disclaimers + metadata) or **CSV** (table only).

_More detail and caveats: **How it works · Limitations** below._
"""
        )
    with st.expander('How it works · Limitations', expanded=False):
        st.markdown(
            """
**Same story as the page**  
The workflow bar under the title tracks **Upload → Map → Analyze → Export**. The **Optional** merge block comes **first** on the page; skip it if you do not need two files combined.

**What this tool does**  
Adds **Alt instances**, **indicative costs**, and **savings %** using **static on-demand list prices** from a **local dataset** (no live AWS Pricing API). Keeps your columns in order; appends enrichment after the instance column.

**What it does not do**  
Does not replace **Billing**, **Cost Explorer**, or **CUR**; does not apply **RI**, **SP**, or **enterprise discounts**. Does not prove **performance** or **Graviton** fit—validate in engineering before production.

**Limitations**  
Indicative values only—invoices win. Many **RDS** SKUs may be missing → **N/A**. If there is no second distinct recommendation, **Alt2** shows **N/A (No distinct alternative)**. Snapshot **as-of** appears in the card above and in Excel **Metadata**.

**Optional merge**  
Dataset 1 is primary (column order kept). Dataset 2 adds columns; values fill where primary cells are empty. This block is at the **top** of the page, before step 1.
"""
        )
st.markdown('<div class="finops-divider" role="separator" aria-hidden="true"></div>', unsafe_allow_html=True)
if run and uploaded:
    with st.spinner('Loading…'):
        try:
            st.session_state.pop('fix_merged_df', None)
            st.session_state.pop('fix_merge_warnings', None)
            _lr_new = load_file(uploaded, uploaded.name)
            st.session_state['load_result'] = _lr_new
            st.session_state['result'] = None
            st.session_state['cost_pick'] = None
            st.session_state['binding'] = None
            st.session_state.pop('_enrich_svc', None)
            st.session_state.pop('_enrich_cpu', None)
            for w in _lr_new.warnings:
                st.markdown(f'<div class="finops-alert finops-alert--warn">⚠️ {w}</div>', unsafe_allow_html=True)
        except ValueError as ve:
            st.session_state['load_result'] = None
            st.markdown(f'<div class="finops-alert finops-alert--err">❌ {ve}</div>', unsafe_allow_html=True)
        except Exception as e:
            st.session_state['load_result'] = None
            log.error('load_file failed: %s', type(e).__name__)
            st.markdown('<div class="finops-alert finops-alert--err">❌ Failed to read file.</div>', unsafe_allow_html=True)
lr = st.session_state.get('load_result')
_sync_auto_binding(lr)
finops_pipeline_slot.markdown(_pipeline_bar_html(_pipeline_step_index(lr)), unsafe_allow_html=True)
binding_ready = False
chosen_binding = None
if st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True
if lr is not None and (not binding_ready):
    cols_all = list(lr.df.columns)
    if lr.needs_instance_pick or lr.needs_os_pick:
        with st.container(border=True):
            _flow_step(2, 'Map columns', 'Point to the instance / DB class column, OS or engine, and actual cost when asked.')
            (mc1, mc2) = st.columns(2)
            with mc1:
                di = 0
                if lr.instance_candidates:
                    di = cols_all.index(lr.instance_candidates[0]) if lr.instance_candidates[0] in cols_all else 0
                inst_sel = st.selectbox('Instance / DB class (AWS API Name)', cols_all, index=min(di, len(cols_all) - 1))
            with mc2:
                if lr.needs_os_pick:
                    os_opts = list(lr.os_candidates)
                    os_sel = st.selectbox('OS / engine column (multiple matches — pick one)', os_opts, index=0)
                else:
                    os_opts = [OS_COLUMN_NONE_OPTION] + cols_all
                    default_os = OS_COLUMN_NONE_OPTION
                    if lr.os_candidates and lr.os_candidates[0] in cols_all:
                        default_os = lr.os_candidates[0]
                    oi = os_opts.index(default_os) if default_os in os_opts else 0
                    os_sel = st.selectbox('OS / engine column (optional — detected from cell values)', os_opts, index=min(oi, len(os_opts) - 1))
            cost_sel = None
            if len(lr.cost_candidates) >= 1:
                cost_sel = lr.cost_candidates[0]
            else:
                cost_sel = st.selectbox('Actual cost column (optional)', ['— None —'] + cols_all, key='cost_optional')
                if cost_sel == '— None —':
                    cost_sel = None
            if st.button('Save mapping', type='primary'):
                try:
                    os_final = None if os_sel == OS_COLUMN_NONE_OPTION else os_sel
                    b = finalize_binding(lr, inst_sel, os_final, cost_sel).binding
                    st.session_state['binding'] = b
                    st.session_state['cost_pick'] = cost_sel
                    st.rerun()
                except ValueError as e:
                    st.markdown(f'<div class="finops-alert finops-alert--err">❌ {e}</div>', unsafe_allow_html=True)
    elif lr.binding is not None:
        # Cost binding is auto-selected in analyze_load when candidates exist.
        pass
if lr is not None and st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True
    st.markdown(
        '<p class="finops-pipeline-note" style="text-align:center;margin:0.35rem 0 0.75rem;font-weight:600;">'
        'Pricing baseline: <strong>Linux</strong> (fallback applied where OS column is absent, blank, or not recognized).'
        '</p>',
        unsafe_allow_html=True,
    )
    if st.session_state.get('result') is None:
        with st.container(border=True):
            _flow_step(3, 'Run enrichment', 'Applies list prices for your region and fills Alt instances, indicative costs, and savings.')
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
                    st.markdown(f'<div class="finops-alert finops-alert--err">❌ {ex}</div>', unsafe_allow_html=True)
                    log.error('process failed: %s', type(ex).__name__)
df_out: pd.DataFrame | None = st.session_state.get('result')
if df_out is not None:
    with st.container(border=True):
        if st.session_state.get('_enrich_svc') != st.session_state.get('service') or st.session_state.get('_enrich_cpu') != st.session_state.get('cpu_filter'):
            st.warning('Service or CPU mode changed since last enrichment — click **Run enrichment** to refresh.')
        st.markdown('<div class="finops-flow-step" style="margin-top:0.5rem;border-bottom:none;"><span class="finops-flow-num" style="background:var(--st-green-color,#34c759);">4</span><div><p class="finops-flow-title">Results</p><p class="finops-flow-sub">Filter and search the table, then download Excel or CSV.</p></div></div>', unsafe_allow_html=True)
        (f1, f2, f3, f4) = st.columns([1, 1, 1, 3])
        with f1:
            vf_svc = st.radio('View service', ['all', 'ec2', 'rds'], format_func=lambda x: {'all': 'Both (show all)', 'ec2': 'EC2 rows only', 'rds': 'RDS rows only'}[x], horizontal=True, key='vf_svc')
        with f2:
            st.caption('CPU (enrichment)')
            st.write(str(st.session_state.get('cpu_filter', 'both')).title())
        with f3:
            vf_os = st.text_input('OS contains', placeholder='filter…', key='vf_os')
        with f4:
            q = st.text_input('Search', placeholder='any column…', key='vf_search')
        view = df_out.copy()
        bind = st.session_state.get('binding')
        bound_inst_col = bind.instance if bind else None
        inst_col = _resolve_instance_column_for_view(view, bound_inst_col)

        def _first_col_pos(frame: pd.DataFrame, name: str | None) -> int | None:
            if not name:
                return None
            cols_l = list(frame.columns)
            try:
                return cols_l.index(name)
            except ValueError:
                return None

        ii = _first_col_pos(view, inst_col)
        if ii is not None:
            inst_ser = view.iloc[:, ii].map(_is_rds_instance_cell)
            if vf_svc == 'ec2':
                view = view[~inst_ser]
            elif vf_svc == 'rds':
                view = view[inst_ser]
        os_col_name = bind.os if bind else None
        if vf_os:
            oi = _first_col_pos(view, os_col_name)
            pi = _first_col_pos(view, 'Pricing OS')
            oidx = oi if oi is not None else pi
            if oidx is not None:
                view = view[view.iloc[:, oidx].astype(str).str.contains(vf_os, case=False, na=False, regex=False)]
        if q:
            m = pd.Series(False, index=view.index)
            for j in range(view.shape[1]):
                m |= view.iloc[:, j].astype(str).str.contains(q, case=False, na=False, regex=False)
            view = view[m]
        st.caption(f'Showing **{len(view):,}** of **{len(df_out):,}** rows')
        if view.empty and len(df_out) > 0:
            st.warning(
                'No rows match your current filters. Clear **Search** and **OS contains**, or set **View service** to **Both (show all)**.'
            )
        try:
            _strip_m = _dashboard_strip_metrics(view, inst_col)
            _render_dashboard_kpi_strip(_strip_m)
            _og_n = int(_strip_m.get('old_gen') or 0)
            if _og_n > 0 and inst_col:
                _og_detail = _old_generation_detail_table(view, inst_col)
                with st.expander(f'Which resources count as older-gen? ({_og_n} in current view)', expanded=False):
                    st.caption(
                        'Same heuristic as the red KPI: instance **family** matches older patterns '
                        '(**m3–m5**, **c3–c5**, **r3–r5**, **t1–t3** variants), excluding Graviton (**…g**). '
                        'RDS **`db.`** classes use the segment after **`db.`** as the family.'
                    )
                    if _og_detail.empty:
                        st.warning('Could not list rows — check that the instance column is mapped.')
                    else:
                        st.dataframe(
                            _dataframe_for_streamlit_arrow(_og_detail),
                            **_ui_stretch_kwargs(),
                            hide_index=True,
                            height=min(420, 36 + 28 * len(_og_detail)),
                        )
            st.markdown('<p class="finops-kpi-strip-title">Row statistics</p>', unsafe_allow_html=True)
            render_kpis(kpis(view))
        except Exception as ex:
            log.warning('Results KPI strip skipped: %s', type(ex).__name__)

        df_display = _enriched_table_for_display(view)
        st.markdown('<div id="finops-enriched-df-anchor"></div>', unsafe_allow_html=True)
        st.caption('Table shows **$** for cost/hourly columns and **%** for savings; exports stay numeric-friendly.')
        st.dataframe(df_display, **_ui_stretch_kwargs(), hide_index=True, height=520)
        export_df = apply_na_fill(df_out)
        reg_id = st.session_state.get('region_id', DEFAULT_REGION)
        reg_lbl = REGION_LABELS.get(reg_id, '')
        (dx1, dx2) = st.columns(2)
        with dx1:
            st.download_button('Download Excel', build_excel(export_df, reg_lbl, reg_id), 'finops_recommendations.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', **_ui_stretch_kwargs(st.download_button))
        with dx2:
            _csv_df = sanitize_formula_injection_dataframe(export_df.copy())
            st.download_button('Download CSV', _csv_df.to_csv(index=False).encode(), 'finops_recommendations.csv', 'text/csv', **_ui_stretch_kwargs(st.download_button))
elif lr is None:
    st.markdown('<div class="finops-card"><p class="finops-card-title" style="margin:0;">Start at step 1</p><p class="finops-card-body" style="margin:0.5rem 0 0;">Upload a spreadsheet and click <strong>Continue</strong>. Steps 2–4 appear after that.</p></div>', unsafe_allow_html=True)
elif not binding_ready:
    st.markdown('<div class="finops-card"><p class="finops-card-title" style="margin:0;">Finish step 2</p><p class="finops-card-body" style="margin:0.5rem 0 0;">Complete column mapping (and pick a cost column if asked), then go to step 3.</p></div>', unsafe_allow_html=True)
st.markdown(
    '<footer class="finops-page-footer" role="contentinfo">'
    '<span class="finops-page-footer-brand">FinOps Optimizer</span>'
    '<span class="finops-page-footer-sep" aria-hidden="true">·</span>'
    '<span class="finops-page-footer-team">FinOps Team</span>'
    '</footer>',
    unsafe_allow_html=True,
)
