"""
rds_recommender.py
------------------
RDS instance class recommendations (db.*) using same-size AWS mappings.
Delegates family logic to EC2 recommender on the class body (strip db. prefix).
"""

from __future__ import annotations

import logging

from recommender import CPUFilterMode, get_recommendations as get_ec2_recommendations

logger = logging.getLogger(__name__)


def parse_rds_class(db_class: str) -> str | None:
    """Return EC2-style 'family.size' or None."""
    if not db_class or not isinstance(db_class, str):
        return None
    s = db_class.strip().lower()
    if not s.startswith("db."):
        return None
    body = s[3:].strip()
    return body if body else None


def get_rds_recommendations(db_class: str, cpu_filter: CPUFilterMode = "both") -> dict[str, str | None]:
    body = parse_rds_class(db_class)
    out: dict[str, str | None] = {
        "family": None, "size": None, "alt1": None, "alt2": None,
    }
    if not body:
        return out

    rec = get_ec2_recommendations(body, cpu_filter=cpu_filter)

    def to_db(x: str | None) -> str | None:
        if not x:
            return None
        if x.lower().startswith("db."):
            return x
        return f"db.{x}"

    out["family"] = rec.get("family")
    out["size"] = rec.get("size")
    out["alt1"] = to_db(rec.get("alt1"))
    out["alt2"] = to_db(rec.get("alt2"))
    return out
