from __future__ import annotations
import re
from pricing_normalize import normalize_instance_string

VALID_SIZES: frozenset[str] = frozenset({'nano', 'micro', 'small', 'medium', 'large', 'xlarge', '2xlarge', '3xlarge', '4xlarge', '6xlarge', '8xlarge', '9xlarge', '10xlarge', '12xlarge', '16xlarge', '18xlarge', '24xlarge', '32xlarge', '48xlarge', 'metal'})
_FAMILY_RE = re.compile('^[a-z][a-z0-9]*$')


def canonicalize_instance_api_name(value: object) -> str | None:
    s = normalize_instance_string(value)
    if not s or s in ('nan', 'none', 'n/a'):
        return None
    if ' ' in s or '\t' in s or '\n' in s:
        return None
    if s.startswith('db.'):
        body = s[3:]
        if not body or body.count('.') != 1:
            return None
        parts = body.split('.', 1)
        if len(parts) != 2:
            return None
        (fam, size) = (parts[0], parts[1])
        if size not in VALID_SIZES:
            return None
        if len(fam) < 2 or len(fam) > 28:
            return None
        if not _FAMILY_RE.fullmatch(fam):
            return None
        return f'db.{fam}.{size}'
    parts = s.split('.', 1)
    if len(parts) != 2:
        return None
    (fam, size) = (parts[0], parts[1])
    if size not in VALID_SIZES:
        return None
    if len(fam) < 2 or len(fam) > 28:
        return None
    if not _FAMILY_RE.fullmatch(fam):
        return None
    return f'{fam}.{size}'
