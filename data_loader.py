"""
data_loader.py  ·  FinOps EC2 Optimizer  ·  v1.3
=================================================
File ingestion with:
  - Auto column detection (case-insensitive alias map, 50+ aliases)
  - FAILSAFE: returns partial df + unmapped columns when auto-detection fails
  - Manual column mapping support (called by UI with user selections)
  - Encoding detection (UTF-8 → latin-1 fallback)
  - Coerces bad numeric values, warns instead of crashing
  - Strips whitespace, empty rows, empty columns
"""

import io
import logging
import re
from typing import BinaryIO

import pandas as pd

logger = logging.getLogger(__name__)

# ── Canonical column names ─────────────────────────────────────────────────
REQUIRED_COLS  = {"Instance Type", "OS"}          # Absolute minimum to process
OPTIONAL_COLS  = {"Cost", "Usage", "Region", "Account", "Application"}
ALL_EXPECTED   = REQUIRED_COLS | OPTIONAL_COLS

# ── Alias map (raw_lower → canonical) ─────────────────────────────────────
COLUMN_ALIASES: dict[str, str] = {
    # ── Instance Type ──────────────────────────────────────────────────
    "instance type":        "Instance Type",
    "instancetype":         "Instance Type",
    "instance_type":        "Instance Type",
    "instance":             "Instance Type",
    "ec2 type":             "Instance Type",
    "ec2type":              "Instance Type",
    "ec2_type":             "Instance Type",
    "type":                 "Instance Type",
    "instance size":        "Instance Type",
    "resource type":        "Instance Type",
    "vm type":              "Instance Type",
    # ── OS ────────────────────────────────────────────────────────────
    "os":                   "OS",
    "o/s":                  "OS",
    "operating system":     "OS",
    "operating_system":     "OS",
    "platform":             "OS",
    "os type":              "OS",
    "os_type":              "OS",
    "system":               "OS",
    # ── Cost ──────────────────────────────────────────────────────────
    "cost":                 "Cost",
    "monthly cost":         "Cost",
    "monthly_cost":         "Cost",
    "total cost":           "Cost",
    "total_cost":           "Cost",
    "charge":               "Cost",
    "charges":              "Cost",
    "cost ($)":             "Cost",
    "cost(usd)":            "Cost",
    "cost (usd)":           "Cost",
    "cost_usd":             "Cost",
    "billed cost":          "Cost",
    "blended cost":         "Cost",
    "unblended cost":       "Cost",
    "amortized cost":       "Cost",
    "spend":                "Cost",
    # ── Usage ─────────────────────────────────────────────────────────
    "usage":                "Usage",
    "usage hours":          "Usage",
    "usage_hours":          "Usage",
    "hours":                "Usage",
    "usage (hrs)":          "Usage",
    "usage(hrs)":           "Usage",
    "running hours":        "Usage",
    "running_hours":        "Usage",
    "uptime":               "Usage",
    "hours used":           "Usage",
    # ── Region ────────────────────────────────────────────────────────
    "region":               "Region",
    "aws region":           "Region",
    "aws_region":           "Region",
    "location":             "Region",
    "availability zone":    "Region",
    "az":                   "Region",
    "zone":                 "Region",
    # ── Account ───────────────────────────────────────────────────────
    "account":              "Account",
    "account id":           "Account",
    "account_id":           "Account",
    "aws account":          "Account",
    "aws_account":          "Account",
    "account name":         "Account",
    "accountname":          "Account",
    "payer account":        "Account",
    "linked account":       "Account",
    # ── Application ───────────────────────────────────────────────────
    "application":          "Application",
    "app":                  "Application",
    "service":              "Application",
    "workload":             "Application",
    "project":              "Application",
    "team":                 "Application",
    "tag:application":      "Application",
    "tag:project":          "Application",
    "tag:service":          "Application",
    "tag: application":     "Application",
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename df columns using alias map (case-insensitive, strip whitespace)."""
    rename: dict[str, str] = {}
    seen_targets: set[str] = set()
    for col in df.columns:
        key = col.strip().lower()
        canonical = COLUMN_ALIASES.get(key)
        if canonical and canonical not in seen_targets:
            rename[col] = canonical
            seen_targets.add(canonical)
    return df.rename(columns=rename)


def _apply_manual_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """
    Apply user-supplied column mapping {raw_col → canonical_col}.
    Only renames; does not drop unmapped columns.
    """
    valid = {k: v for k, v in mapping.items()
             if k in df.columns and v in ALL_EXPECTED}
    return df.rename(columns=valid)


def _coerce_numeric(df: pd.DataFrame, col: str) -> list[str]:
    """Coerce a column to numeric. Returns list of warnings (empty if clean)."""
    warns: list[str] = []
    before = df[col].isna().sum()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    after  = df[col].isna().sum()
    bad    = after - before
    if bad:
        warns.append(f"{bad} row(s) had non-numeric '{col}' values — set to 0.")
        df[col] = df[col].fillna(0.0)
    return warns


def _parse_dataframe(raw_bytes: bytes, ext: str) -> pd.DataFrame:
    """Parse raw bytes into DataFrame based on file extension."""
    if ext in ("xlsx", "xls", "xlsm"):
        return pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl",
                             dtype=str, keep_default_na=False)
    elif ext == "csv":
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return pd.read_csv(io.BytesIO(raw_bytes), encoding=enc,
                                   dtype=str, keep_default_na=False)
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError("Could not decode CSV file (tried utf-8, latin-1, cp1252).")
    else:
        raise ValueError(
            f"Unsupported format '.{ext}'. Upload CSV (.csv) or Excel (.xlsx / .xls)."
        )


class LoadResult:
    """
    Container returned by load_file().
    Always contains a DataFrame.  Check .needs_manual_mapping first.
    """
    def __init__(
        self,
        df: pd.DataFrame,
        warnings: list[str],
        missing_required: set[str],
        unmapped_cols: list[str],
    ):
        self.df               = df
        self.warnings         = warnings
        self.missing_required = missing_required   # canonical names not yet found
        self.unmapped_cols    = unmapped_cols       # raw column names not yet mapped
        self.needs_manual_mapping = bool(missing_required)

    def apply_mapping(self, mapping: dict[str, str]) -> "LoadResult":
        """
        Apply user-supplied {raw_col → canonical_col} mapping and
        re-run validation. Returns a new LoadResult.
        """
        df2 = _apply_manual_mapping(self.df.copy(), mapping)
        return _validate_and_coerce(df2, self.warnings.copy())


def load_file(file_obj: BinaryIO, filename: str) -> LoadResult:
    """
    Parse and auto-map columns.
    Always returns a LoadResult — never raises on missing columns.
    Caller checks .needs_manual_mapping.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    try:
        raw = file_obj.read() if hasattr(file_obj, "read") else bytes(file_obj)
        df  = _parse_dataframe(raw, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc

    if df.empty:
        raise ValueError("The uploaded file contains no data rows.")

    # Strip truly blank rows and columns
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("All rows are empty after stripping blank lines.")

    df = _normalise_columns(df)
    return _validate_and_coerce(df, [])


def _validate_and_coerce(df: pd.DataFrame, base_warnings: list[str]) -> LoadResult:
    warnings = base_warnings[:]

    # Track which required columns are still missing
    missing = REQUIRED_COLS - set(df.columns)
    unmapped = [c for c in df.columns if c not in ALL_EXPECTED]

    # Type-coerce numeric columns that exist
    for col in ("Cost", "Usage"):
        if col in df.columns:
            warnings.extend(_coerce_numeric(df, col))
        else:
            warnings.append(f"Optional column '{col}' not found — will be blank in output.")

    # Strip whitespace from string columns
    for col in ALL_EXPECTED:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().replace("nan", "")

    logger.info(f"Loaded {len(df)} rows. Missing: {missing}. Unmapped: {len(unmapped)} cols.")
    return LoadResult(df=df, warnings=warnings,
                      missing_required=missing, unmapped_cols=unmapped)
