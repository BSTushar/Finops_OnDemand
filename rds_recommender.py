from __future__ import annotations
import logging
from instance_api import canonicalize_instance_api_name
from recommender import CPUFilterMode, get_recommendations as get_ec2_recommendations
logger = logging.getLogger(__name__)


def get_rds_recommendations(db_class: str, cpu_filter: CPUFilterMode='both') -> dict[str, str | None]:
    out: dict[str, str | None] = {'family': None, 'size': None, 'alt1': None, 'alt2': None}
    canon = canonicalize_instance_api_name(db_class)
    if not canon or not canon.startswith('db.'):
        if db_class and str(db_class).strip():
            logger.warning('Invalid RDS API Name (value omitted for security)')
        return out
    body = canon[3:]
    rec = get_ec2_recommendations(body, cpu_filter=cpu_filter)

    def to_db(x: str | None) -> str | None:
        if not x:
            return None
        if x.lower().startswith('db.'):
            return x
        return f'db.{x}'
    out['family'] = rec.get('family')
    out['size'] = rec.get('size')
    out['alt1'] = to_db(rec.get('alt1'))
    out['alt2'] = to_db(rec.get('alt2'))
    return out
