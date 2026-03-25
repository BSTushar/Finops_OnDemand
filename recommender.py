"""
recommender.py
--------------
Maps EC2 instance families to recommended upgrade paths.
Follows AWS generation progression and Graviton preference.
"""

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Family upgrade map: family -> (alt1_family, alt2_family)
# alt1 = next-gen incremental upgrade
# alt2 = latest / Graviton preferred
# ---------------------------------------------------------------------------

FAMILY_UPGRADE_MAP: dict[str, tuple[str, str]] = {
    # General Purpose
    "t2":  ("t3",   "t4g"),
    "t3":  ("t3a",  "t4g"),
    "t3a": ("t4g",  "t4g"),
    "t4g": ("t4g",  "t4g"),   # already latest Graviton
    "m1":  ("m4",   "m7g"),
    "m2":  ("m4",   "m7g"),
    "m3":  ("m4",   "m6g"),
    "m4":  ("m5",   "m6g"),
    "m5":  ("m6i",  "m7g"),
    "m5a": ("m6i",  "m7g"),
    "m5n": ("m6i",  "m7i"),
    "m5zn":("m6i",  "m7i"),
    "m6i": ("m7i",  "m7g"),
    "m6g": ("m7g",  "m7g"),
    "m6a": ("m7i",  "m7g"),
    "m7i": ("m7i",  "m7g"),   # already latest x86
    "m7g": ("m7g",  "m7g"),   # already latest Graviton

    # Compute Optimised
    "c1":  ("c4",   "c7g"),
    "c3":  ("c4",   "c7g"),
    "c4":  ("c5",   "c6g"),
    "c5":  ("c6i",  "c7g"),
    "c5a": ("c6i",  "c7g"),
    "c5n": ("c6i",  "c7i"),
    "c6i": ("c7i",  "c7g"),
    "c6g": ("c7g",  "c7g"),
    "c6a": ("c7i",  "c7g"),
    "c7i": ("c7i",  "c7g"),
    "c7g": ("c7g",  "c7g"),

    # Memory Optimised
    "r3":  ("r4",   "r7g"),
    "r4":  ("r5",   "r6g"),
    "r5":  ("r6i",  "r7g"),
    "r5a": ("r6i",  "r7g"),
    "r5b": ("r6i",  "r7i"),
    "r5n": ("r6i",  "r7i"),
    "r6i": ("r7i",  "r7g"),
    "r6g": ("r7g",  "r7g"),
    "r6a": ("r7i",  "r7g"),
    "r7i": ("r7i",  "r7g"),
    "r7g": ("r7g",  "r7g"),

    # High memory
    "x1":  ("x2idn","x2iedn"),
    "x1e": ("x2idn","x2iedn"),
    "x2idn":("x2iedn","x2iedn"),

    # Storage Optimised
    "i2":  ("i3",   "i4i"),
    "i3":  ("i3en", "i4i"),
    "i3en":("i4i",  "i4i"),
    "i4i": ("i4i",  "i4i"),
    "d2":  ("d3",   "d3"),
    "d3":  ("d3",   "d3"),
    "h1":  ("i3en", "i4i"),

    # GPU / Accelerated
    "p2":  ("p3",   "p4d"),
    "p3":  ("p4d",  "p4d"),
    "p4d": ("p4d",  "p4d"),
    "g3":  ("g4dn", "g5"),
    "g4dn":("g5",   "g5"),
    "g4ad":("g5",   "g5"),
    "g5":  ("g5",   "g5"),
    "inf1":("inf2", "inf2"),
    "inf2":("inf2", "inf2"),
    "trn1":("trn1", "trn1"),

    # Network Intensive
    "c5n": ("c6in", "c7gn"),
    "c6in":("c7i",  "c7gn"),
    "c7gn":("c7gn", "c7gn"),
    "r5n": ("r6in", "r6in"),
    "r6in":("r7i",  "r7i"),
}

# Sizes that map 1-to-1 across families
VALID_SIZES = {
    "nano", "micro", "small", "medium", "large",
    "xlarge", "2xlarge", "3xlarge", "4xlarge", "6xlarge",
    "8xlarge", "9xlarge", "10xlarge", "12xlarge", "16xlarge",
    "18xlarge", "24xlarge", "32xlarge", "48xlarge",
    "metal",
}

# Some families don't support small sizes; fallback mapping
SIZE_FALLBACK: dict[tuple[str, str], str] = {
    # (target_family, requested_size): nearest_available_size
    ("m6g", "nano"): "medium",
    ("m7g", "nano"): "medium",
    ("c6g", "nano"): "medium",
    ("c7g", "nano"): "medium",
    ("r6g", "nano"): "medium",
    ("r7g", "nano"): "medium",
    ("m6g", "micro"): "medium",
    ("m7g", "micro"): "medium",
    ("c6g", "micro"): "medium",
    ("c7g", "micro"): "medium",
    ("m6g", "small"): "medium",
    ("m7g", "small"): "medium",
    ("c6g", "small"): "medium",
    ("c7g", "small"): "medium",
}


def parse_instance(instance_type: str) -> tuple[str, str] | None:
    """
    Split an instance type into (family, size).
    e.g. 'm5.xlarge' -> ('m5', 'xlarge')
    Returns None if parsing fails.
    """
    if not instance_type or not isinstance(instance_type, str):
        return None
    parts = instance_type.strip().lower().split(".")
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def build_alt(target_family: str, size: str) -> str | None:
    """
    Build a candidate instance string, applying size fallbacks where needed.
    """
    resolved_size = SIZE_FALLBACK.get((target_family, size), size)
    candidate = f"{target_family}.{resolved_size}"
    return candidate


def get_recommendations(instance_type: str) -> dict[str, str | None]:
    """
    Returns:
        {
          "family": str,
          "size":   str,
          "alt1":   str | None,
          "alt2":   str | None,
        }
    """
    result: dict[str, str | None] = {
        "family": None, "size": None, "alt1": None, "alt2": None
    }

    parsed = parse_instance(instance_type)
    if parsed is None:
        logger.warning(f"Cannot parse instance type: {instance_type!r}")
        return result

    family, size = parsed
    result["family"] = family
    result["size"] = size

    upgrade = FAMILY_UPGRADE_MAP.get(family)
    if upgrade is None:
        # Try a prefix match (e.g. unknown sub-variant)
        for key in FAMILY_UPGRADE_MAP:
            if family.startswith(key):
                upgrade = FAMILY_UPGRADE_MAP[key]
                break

    if upgrade is None:
        logger.debug(f"No upgrade path for family: {family}")
        # Default: same instance = no recommendation
        return result

    alt1_family, alt2_family = upgrade

    # Build alt1 only if it's a different family
    if alt1_family != family:
        result["alt1"] = build_alt(alt1_family, size)
    # Build alt2 only if it's a different family (and different from alt1)
    if alt2_family != family and alt2_family != alt1_family:
        result["alt2"] = build_alt(alt2_family, size)
    elif alt2_family != family and alt2_family == alt1_family:
        result["alt2"] = None  # same suggestion, skip duplicate
    elif alt2_family == family:
        result["alt2"] = None

    return result
