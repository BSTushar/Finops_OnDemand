from __future__ import annotations

"""Value-based OS detection labels and pricing-engine OS keys (no column-name coupling)."""

import re
from typing import Literal
import pandas as pd
from pricing_engine import OS_ALIASES, OS_SURCHARGE_MAP

PRICING_OS_METADATA_NOTE: str = (
    'Pricing OS shows Linux or Windows baseline used per row. '
    'Missing OS column, blank cells, or unrecognized values use Linux for pricing (see Pricing OS column).'
)

CellOsKind = Literal['linux', 'windows'] | None

# Linux-type tokens (substring / phrase); longer phrases first; include unix for Product/SKU-style cols
_LINUX_PHRASES: tuple[str, ...] = ('amazon linux', 'rhel', 'ubuntu', 'debian', 'unix', 'linux')
# Windows: avoid matching unrelated words containing "win"
_WIN_RE = re.compile('\\b(?:windows|win)\\b', re.I)
_WIN_PREFIX_RE = re.compile('^win\\d{2,4}', re.I)
_WIN_DIGITS_RE = re.compile('^win\\d+$', re.I)


def _cell_str(cell: object) -> str:
    if cell is None:
        return ''
    try:
        if pd.isna(cell):
            return ''
    except (TypeError, ValueError):
        pass
    s = str(cell).strip().lower()
    if not s or s in ('nan', 'none', 'n/a'):
        return ''
    return s


def cell_matches_valid_os_pattern(cell: object) -> bool:
    """True if cell value matches allowed Linux- or Windows-type patterns (for column detection)."""
    s = _cell_str(cell)
    if not s:
        return False
    if _WIN_RE.search(s) or _WIN_PREFIX_RE.match(s) or _WIN_DIGITS_RE.match(s):
        return True
    for ph in _LINUX_PHRASES:
        if ph in s:
            return True
    return False


def classify_os_kind(cell: object) -> CellOsKind:
    """Classify cell as linux-family or windows-family for detection; invalid → None."""
    s = _cell_str(cell)
    if not s:
        return None
    if _WIN_RE.search(s) or _WIN_PREFIX_RE.match(s) or _WIN_DIGITS_RE.match(s):
        return 'windows'
    for ph in _LINUX_PHRASES:
        if ph in s:
            return 'linux'
    return None


def normalize_pricing_os_display(cell: object) -> str:
    """User-facing bucket: Linux or Windows. Missing/invalid → Linux (fallback)."""
    return 'Windows' if classify_os_kind(cell) == 'windows' else 'Linux'


def engine_os_for_pricing(cell: object) -> str:
    """OS string for get_price / get_rds_hourly; always defined; default linux. Does not change surcharge rules."""
    s = _cell_str(cell)
    if not s:
        return 'linux'
    if _WIN_RE.search(s) or _WIN_PREFIX_RE.match(s) or _WIN_DIGITS_RE.match(s):
        return 'windows'
    for alias in sorted(OS_ALIASES.keys(), key=len, reverse=True):
        if s == alias or alias in s:
            mapped = OS_ALIASES[alias]
            if mapped in OS_SURCHARGE_MAP:
                return mapped
    if any((p in s for p in _LINUX_PHRASES)):
        return 'linux'
    return 'linux'
