from __future__ import annotations
import html
import logging
import pandas as pd
import streamlit as st
from data_loader import LoadResult, analyze_load, dataframe_from_bytes, finalize_binding, load_file
from excel_export import build_excel, savings_numeric
from processor import apply_na_fill, process
from pricing_engine import CACHE_METADATA, DECISION_SUPPORT_NOTE, DEFAULT_REGION, PRICING_SOURCE_LABEL, REGION_LABELS, SUPPORTED_REGIONS, cache_age_days, cache_is_stale, cost_disclaimer_text
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs
logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
st.set_page_config(page_title='FinOps Optimizer', page_icon='◆', layout='wide', initial_sidebar_state='collapsed')
FINOPS_UI_CSS = """
<style>
#MainMenu { visibility: hidden; }
[data-testid="stFooter"] { visibility: hidden; }

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
.finops-hero-inner .finops-tagline {
  animation: finops-fade-up 0.68s cubic-bezier(0.22, 1, 0.36, 1) 0.12s both;
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
  font-size: clamp(2rem, 5vw, 2.75rem);
  font-weight: 600;
  letter-spacing: -0.03em;
  line-height: 1.1;
  margin: 0 0 0.65rem;
  padding: 0;
  width: 100%;
  text-align: center;
  text-wrap: balance;
  color: var(--st-text-color, inherit);
}
.finops-tagline {
  font-size: 1.0625rem;
  font-weight: 400;
  margin: 0 0 1.1rem;
  line-height: 1.5;
  max-width: 36rem;
  width: 100%;
  margin-left: auto;
  margin-right: auto;
  padding: 0;
  text-align: center;
  text-wrap: balance;
  color: color-mix(in srgb, var(--st-text-color, CanvasText) 78%, var(--st-background-color, Canvas));
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

.finops-journey {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: center;
  gap: 0.35rem 0.65rem;
  margin: 0 auto 1.25rem;
  padding: 0.65rem 1rem;
  max-width: 44rem;
  font-size: 0.8125rem;
  line-height: 1.45;
  text-align: center;
  color: color-mix(in srgb, var(--st-text-color) 88%, var(--st-background-color));
  border-radius: 12px;
  border: 1px solid color-mix(in srgb, var(--st-border-color) 65%, transparent);
  background: color-mix(in srgb, var(--st-secondary-background-color) 88%, var(--st-background-color));
}
.finops-journey strong {
  color: var(--st-text-color);
  font-weight: 700;
}
.finops-journey-sep {
  opacity: 0.45;
  user-select: none;
}
.finops-journey-note {
  display: block;
  width: 100%;
  margin-top: 0.35rem;
  font-size: 0.75rem;
  opacity: 0.85;
  color: color-mix(in srgb, var(--st-text-color) 72%, var(--st-background-color));
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
  margin: 2.25rem 0 1rem;
  padding-bottom: 0.35rem;
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
  font-size: 1.25rem;
  font-weight: 600;
  letter-spacing: -0.02em;
  margin: 0;
  color: var(--st-text-color, inherit);
}
.finops-flow-sub {
  font-size: 0.9rem;
  margin: 0.35rem 0 0;
  line-height: 1.4;
  color: color-mix(in srgb, var(--st-text-color, CanvasText) 72%, var(--st-background-color, Canvas));
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
  border-radius: 12px;
  padding: 1rem 1.15rem;
  transition: transform 0.25s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.25s ease, border-color 0.25s ease;
  background: color-mix(
    in srgb,
    var(--st-secondary-background-color, rgba(128, 128, 128, 0.08)) 88%,
    var(--st-text-color, #000) 6%
  );
  border: 1px solid color-mix(in srgb, var(--st-border-color, #88888830) 100%, transparent);
  box-shadow: 0 1px 2px color-mix(in srgb, var(--st-text-color, #000) 5%, transparent);
  color: var(--st-text-color, inherit);
}
.finops-metric:hover {
  transform: translateY(-3px);
  box-shadow: 0 8px 22px color-mix(in srgb, var(--st-text-color, #000) 10%, transparent);
  border-color: color-mix(in srgb, var(--st-primary-color, #0071e3) 22%, var(--st-border-color));
}
@media (prefers-reduced-motion: reduce) {
  .finops-metric:hover { transform: translateY(-1px); }
}
.finops-metric-label {
  font-size: 0.68rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: color-mix(in srgb, var(--st-text-color, CanvasText) 65%, var(--st-background-color, Canvas));
}
.finops-metric-value {
  font-size: 1.35rem;
  font-weight: 600;
  margin-top: 0.25rem;
  color: var(--st-text-color, inherit);
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
        st.markdown(f"""<div class="finops-metric"><div class="finops-metric-label">Rows</div><div class="finops-metric-value">{k['total']:,}</div></div>""", unsafe_allow_html=True)
    with c2:
        v = f"{k['avg1']:.1f}%" if k['avg1'] is not None else '—'
        st.markdown(f'<div class="finops-metric"><div class="finops-metric-label">Avg Alt1 savings</div><div class="finops-metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c3:
        v = f"{k['max1']:.1f}%" if k['max1'] is not None else '—'
        st.markdown(f'<div class="finops-metric"><div class="finops-metric-label">Max Alt1 savings</div><div class="finops-metric-value">{v}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="finops-metric"><div class="finops-metric-label">Actual cost column</div><div class="finops-metric-value">{('Yes' if k['act_col'] else 'No')}</div></div>""", unsafe_allow_html=True)
for (key, default) in (('load_result', None), ('result', None), ('region_id', DEFAULT_REGION), ('service', 'both'), ('cpu_filter', 'both'), ('cost_pick', None)):
    if key not in st.session_state:
        st.session_state[key] = default
stale = cache_is_stale()
pill = f'Dataset {cache_age_days()}d old — consider refresh' if stale else 'List prices loaded locally'
_pill_class = 'finops-pill finops-pill--stale' if stale else 'finops-pill finops-pill--fresh'
st.markdown(f'''<div class="finops-hero"><div class="finops-hero-inner">
<p class="finops-eyebrow">Decision support · EC2 &amp; RDS</p>
<h1 class="finops-headline">FinOps Optimizer</h1>
<p class="finops-tagline">Follow the steps down this page: upload one spreadsheet (or merge two first), map columns if asked, run enrichment, then export. Savings are indicative from on-demand list prices for the region you pick—not invoices.</p>
<span class="{_pill_class}">{html.escape(pill)}</span>
</div></div>''', unsafe_allow_html=True)
st.markdown(
    '<div class="finops-journey" role="navigation" aria-label="Main steps on this page">'
    '<span><strong>1</strong> Upload &amp; pricing settings</span><span class="finops-journey-sep" aria-hidden="true">→</span>'
    '<span><strong>2</strong> Map columns</span><span class="finops-journey-sep" aria-hidden="true">→</span>'
    '<span><strong>3</strong> Run enrichment</span><span class="finops-journey-sep" aria-hidden="true">→</span>'
    '<span><strong>4</strong> Results &amp; download</span>'
    '<span class="finops-journey-note">Optional block is <em>first</em> below—merge two files only if you need one combined table before step 1.</span>'
    '</div>',
    unsafe_allow_html=True,
)
_flow_optional(
    'Merge two spreadsheets first',
    'Skip this if you already have one file. Merge on a shared ID, then use <strong>Use merged data</strong> or upload the result in step 1.',
)
with st.container():
    st.markdown('<div id="finops-fix-sheet-anchor"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="finops-fix-hint-block"><span class="finops-fix-hint-kicker">How matching works</span>'
        '<p class="finops-fix-hint">Dataset 1 = primary row layout. Dataset 2 = extra fields (e.g. spend). Match on resource ID, instance ID, or similar.</p></div>',
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
st.markdown('<div class="finops-divider finops-divider--section" role="separator" aria-hidden="true"></div>', unsafe_allow_html=True)
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
    st.session_state['service'] = st.radio('svc', ['ec2', 'rds', 'both'], format_func=lambda x: {'ec2': 'EC2', 'rds': 'RDS', 'both': 'Both'}[x], label_visibility='collapsed', horizontal=True)
with cpu_col:
    st.markdown('<div class="finops-sec">CPU</div>', unsafe_allow_html=True)
    st.session_state['cpu_filter'] = st.selectbox('cpu', ['both', 'default', 'intel', 'graviton'], format_func=lambda x: {'both': 'Both', 'default': 'Default', 'intel': 'Intel', 'graviton': 'Graviton'}[x], label_visibility='collapsed')
with go_col:
    st.markdown('<div class="finops-sec">Next</div>', unsafe_allow_html=True)
    run = st.button('Continue', type='primary', disabled=uploaded is None, use_container_width=True)
_reg = st.session_state.get('region_id', DEFAULT_REGION)
_rid_s = html.escape(_reg.strip().lower())
_rlabel_s = html.escape(REGION_LABELS.get(_reg.strip().lower(), _reg))
_asof_s = html.escape(CACHE_METADATA['last_updated'].strftime('%Y-%m-%d'))
_src_s = html.escape(PRICING_SOURCE_LABEL)
_disc_s = html.escape(cost_disclaimer_text(_reg))
_dec_s = html.escape(DECISION_SUPPORT_NOTE)
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
<p class="finops-card-body">{_dec_s}</p>
<p class="finops-card-body finops-trust-foot">Indicative savings only. Original columns preserved. Unknown API names or SKUs show <strong>N/A</strong>—never a guess.</p>
</div>''', unsafe_allow_html=True)
with st.expander('Quick guide (same order as this page)', expanded=False):
    st.markdown(
        """
**Step 1 — Upload and set pricing**  
Pick **Pricing region** (list prices for that region), **Service** (EC2 / RDS / Both), and **CPU** (usually Both). Upload CSV or Excel, then **Continue**.

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
The strip under the title is the main path: **1 → 2 → 3 → 4**. The **Optional** merge block comes **first** on the page; skip it if you do not need two files combined.

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
            lr: LoadResult = load_file(uploaded, uploaded.name)
            st.session_state['load_result'] = lr
            st.session_state['result'] = None
            st.session_state['cost_pick'] = None
            st.session_state['binding'] = None
            st.session_state.pop('_enrich_svc', None)
            st.session_state.pop('_enrich_cpu', None)
            for w in lr.warnings:
                st.markdown(f'<div class="finops-alert finops-alert--warn">⚠️ {w}</div>', unsafe_allow_html=True)
        except ValueError as ve:
            st.session_state['load_result'] = None
            st.markdown(f'<div class="finops-alert finops-alert--err">❌ {ve}</div>', unsafe_allow_html=True)
        except Exception as e:
            st.session_state['load_result'] = None
            log.error('load_file failed: %s', type(e).__name__)
            st.markdown('<div class="finops-alert finops-alert--err">❌ Failed to read file.</div>', unsafe_allow_html=True)
lr: LoadResult | None = st.session_state.get('load_result')
binding_ready = False
chosen_binding = None
if st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True
if lr is not None and (not binding_ready):
    cols_all = list(lr.df.columns)
    if lr.needs_instance_pick or lr.needs_os_pick:
        _flow_step(2, 'Map columns', 'Point to the instance / DB class column, OS or engine, and actual cost when asked.')
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
                st.markdown(f'<div class="finops-alert finops-alert--err">❌ {e}</div>', unsafe_allow_html=True)
    elif lr.binding is not None:
        if len(lr.cost_candidates) > 1 and lr.binding.actual_cost is None:
            st.markdown('<div class="finops-alert finops-alert--warn">Multiple cost columns — pick one for Actual Cost.</div>', unsafe_allow_html=True)
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
    if st.session_state.get('_enrich_svc') != st.session_state.get('service') or st.session_state.get('_enrich_cpu') != st.session_state.get('cpu_filter'):
        st.warning('Service or CPU mode changed since last enrichment — click **Run enrichment** to refresh.')
    st.markdown('<div class="finops-flow-step" style="margin-top:1.5rem;border-bottom:none;"><span class="finops-flow-num" style="background:var(--st-green-color,#34c759);">4</span><div><p class="finops-flow-title">Results</p><p class="finops-flow-sub">Filter and search the table, then download Excel or CSV.</p></div></div>', unsafe_allow_html=True)
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
            return 'opacity: 0.62'
        if n >= 20:
            return 'color: var(--st-green-text-color, #22c55e); font-weight:600'
        if n > 0:
            return 'color: var(--st-orange-text-color, #f59e0b); font-weight:600'
        return 'color: var(--st-red-text-color, #ef4444); font-weight:600'

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
    st.markdown('<div id="finops-enriched-df-anchor"></div>', unsafe_allow_html=True)
    st.dataframe(sty, use_container_width=True, hide_index=True, height=500)
    export_df = apply_na_fill(df_out)
    reg_id = st.session_state.get('region_id', DEFAULT_REGION)
    reg_lbl = REGION_LABELS.get(reg_id, '')
    (dx1, dx2) = st.columns(2)
    with dx1:
        st.download_button('Download Excel', build_excel(export_df, reg_lbl, reg_id), 'finops_recommendations.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', use_container_width=True)
    with dx2:
        st.download_button('Download CSV', export_df.to_csv(index=False).encode(), 'finops_recommendations.csv', 'text/csv', use_container_width=True)
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
