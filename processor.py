"""
processor.py  ·  FinOps EC2 Optimizer  ·  v1.3
===============================================
Enrichment pipeline — strict column order, old-gen flagging, vectorised.

Output column layout (guaranteed):
  Col 0  Instance Type
  Col 1  OS
  Col 2  On-Demand Price ($)
  Col 3  Alt 1 Instance
  Col 4  Alt 1 Price ($)
  Col 5  Alt 2 Instance
  Col 6  Alt 2 Price ($)
  Col 7  Size
  Col 8  Savings Opportunity (%)
  Col 9  Generation Flag         ← NEW: "Old Gen" / "Current" / "Latest"
  Col 10+ <all original columns, order preserved>

Pricing: O(1) dict lookup — zero network calls, no guessing.
"""

import logging
import pandas as pd

from pricing_engine import get_price, DEFAULT_REGION
from recommender import get_recommendations, parse_instance

logger = logging.getLogger(__name__)

# ── Column specs ───────────────────────────────────────────────────────────
ENRICHED_COLS: list[str] = [
    "On-Demand Price ($)",
    "Alt 1 Instance",
    "Alt 1 Price ($)",
    "Alt 2 Instance",
    "Alt 2 Price ($)",
    "Size",
    "Savings Opportunity (%)",
    "Generation Flag",
]

NA_FILL = "N/A"

# Old-generation families (for visual flagging)
OLD_GEN_FAMILIES: frozenset[str] = frozenset({
    "t1", "t2", "m1", "m2", "m3", "m4",
    "c1", "c3", "c4",
    "r3", "r4",
    "i2", "i3",
    "p2", "p3",
    "g3", "g4dn", "g4ad",
    "x1", "x1e",
    "d2", "h1",
})

CURRENT_GEN_FAMILIES: frozenset[str] = frozenset({
    "t3", "t3a", "m5", "m5a", "m6i", "m6g", "m6a",
    "c5", "c5a", "c6i", "c6g", "c6a",
    "r5", "r5a", "r5b", "r6i", "r6g", "r6a",
    "i3en", "i4i",
    "g5",
})

LATEST_GEN_FAMILIES: frozenset[str] = frozenset({
    "t4g", "m7i", "m7g", "c7i", "c7g", "r7i", "r7g",
    "inf2", "trn1",
})


def _flag_generation(family: str | None) -> str:
    if not family:
        return NA_FILL
    f = family.lower()
    if f in OLD_GEN_FAMILIES:
        return "Old Gen"
    if f in LATEST_GEN_FAMILIES:
        return "Latest"
    if f in CURRENT_GEN_FAMILIES:
        return "Current"
    return "Current"   # unknown → don't flag negatively


def _safe_price(instance: str | None, region: str, os_val: str) -> float | None:
    if not instance:
        return None
    try:
        return get_price(instance, region, os_val)
    except Exception:
        return None


def _safe_rec(instance: str) -> dict:
    try:
        return get_recommendations(instance)
    except Exception:
        return {"family": None, "size": None, "alt1": None, "alt2": None}


def _savings_pct(orig: float | None, alt: float | None) -> float | None:
    if orig and alt and orig > 0:
        return round((orig - alt) / orig * 100, 1)
    return None


def process(df: pd.DataFrame, region: str = DEFAULT_REGION) -> pd.DataFrame:
    """
    Enrich the input DataFrame with AWS pricing + recommendations.
    Guarantees ENRICHED_COLS immediately follow OS.
    All original columns appended after, in original order, values unchanged.
    """
    if df.empty:
        raise ValueError("Cannot process an empty DataFrame.")
    for col in ("Instance Type", "OS"):
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' is missing from the dataset.")

    df = df.copy()

    # ── Vectorised enrichment ─────────────────────────────────────────────
    od_prices, a1_insts, a1_prices, a2_insts, a2_prices = [], [], [], [], []
    sizes, savings, gen_flags = [], [], []

    for _, row in df.iterrows():
        inst   = str(row.get("Instance Type") or "").strip()
        os_val = str(row.get("OS") or "linux").strip()

        rec    = _safe_rec(inst)
        family = rec.get("family")
        size   = rec.get("size") or (parse_instance(inst) or (None, None))[1]

        od  = _safe_price(inst, region, os_val)

        a1  = rec.get("alt1")
        a1p = _safe_price(a1, region, os_val) if a1 else None

        a2  = rec.get("alt2")
        a2p = _safe_price(a2, region, os_val) if a2 else None

        # Prefer cheapest alt first
        if od and a1p and a2p and a1p >= od and a2p < od:
            a1, a1p, a2, a2p = a2, a2p, a1, a1p

        sav   = _savings_pct(od, a1p)
        g_flag = _flag_generation(family)

        od_prices.append(round(od, 6)  if od  is not None else None)
        a1_insts.append(a1)
        a1_prices.append(round(a1p, 6) if a1p is not None else None)
        a2_insts.append(a2)
        a2_prices.append(round(a2p, 6) if a2p is not None else None)
        sizes.append(size)
        savings.append(sav)
        gen_flags.append(g_flag)

    # ── Build output with guaranteed column order ─────────────────────────
    core     = ["Instance Type", "OS"]
    trailing = [c for c in df.columns if c not in core]

    out = df[core].copy()
    out["On-Demand Price ($)"]     = od_prices
    out["Alt 1 Instance"]          = a1_insts
    out["Alt 1 Price ($)"]         = a1_prices
    out["Alt 2 Instance"]          = a2_insts
    out["Alt 2 Price ($)"]         = a2_prices
    out["Size"]                    = sizes
    out["Savings Opportunity (%)"] = savings
    out["Generation Flag"]         = gen_flags

    for col in trailing:
        out[col] = df[col].values

    # ── Integrity guards ──────────────────────────────────────────────────
    assert list(out.columns[:2])  == ["Instance Type", "OS"],  "Core col order broken"
    assert list(out.columns[2:10]) == ENRICHED_COLS,            "Enriched col order broken"
    assert len(out.columns) == len(set(out.columns)),           "Duplicate columns"
    assert len(out) == len(df),                                 "Row count changed"

    logger.info(f"Processed {len(out)} rows × {len(out.columns)} cols | region={region}")
    return out


def apply_na_fill(df: pd.DataFrame) -> pd.DataFrame:
    """Replace None/NaN in computed columns with 'N/A'. For export only."""
    df = df.copy()
    for col in ENRICHED_COLS:
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), other=NA_FILL)
    return df
