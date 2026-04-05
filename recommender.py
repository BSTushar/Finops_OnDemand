from __future__ import annotations
import logging
import re
from typing import Literal
logger = logging.getLogger(__name__)
CPUFilterMode = Literal['default', 'intel', 'graviton', 'both']
_NON_GRAVITON_G: frozenset[str] = frozenset({'g3', 'g4dn', 'g4ad', 'g5', 'g6', 'g6e', 'g5g'})

def is_graviton_family(family: str) -> bool:
    fam = family.lower().strip()
    if fam in _NON_GRAVITON_G:
        return False
    return fam.endswith('g')
FAMILY_UPGRADE_MAP: dict[str, tuple[str, str]] = {'t2': ('t3', 't4g'), 't3': ('t3a', 't4g'), 't3a': ('t4g', 't4g'), 't4g': ('t4g', 't4g'), 'm1': ('m4', 'm7g'), 'm2': ('m4', 'm7g'), 'm3': ('m4', 'm6g'), 'm4': ('m5', 'm6g'), 'm5': ('m6i', 'm7g'), 'm5a': ('m6i', 'm7g'), 'm5n': ('m6i', 'm7i'), 'm5zn': ('m6i', 'm7i'), 'm6i': ('m7i', 'm7g'), 'm6g': ('m7g', 'm7g'), 'm6a': ('m7i', 'm7g'), 'm7i': ('m7i', 'm7g'), 'm7g': ('m7g', 'm7g'), 'c1': ('c4', 'c7g'), 'c3': ('c4', 'c7g'), 'c4': ('c5', 'c6g'), 'c5': ('c6i', 'c7g'), 'c5a': ('c6i', 'c7g'), 'c5n': ('c6i', 'c7i'), 'c6i': ('c7i', 'c7g'), 'c6g': ('c7g', 'c7g'), 'c6a': ('c7i', 'c7g'), 'c7i': ('c7i', 'c7g'), 'c7g': ('c7g', 'c7g'), 'r3': ('r4', 'r7g'), 'r4': ('r5', 'r6g'), 'r5': ('r6i', 'r7g'), 'r5a': ('r6i', 'r7g'), 'r5b': ('r6i', 'r7i'), 'r5n': ('r6i', 'r7i'), 'r6i': ('r7i', 'r7g'), 'r6g': ('r7g', 'r7g'), 'r6a': ('r7i', 'r7g'), 'r7i': ('r7i', 'r7g'), 'r7g': ('r7g', 'r7g'), 'x1': ('x2idn', 'x2iedn'), 'x1e': ('x2idn', 'x2iedn'), 'x2idn': ('x2iedn', 'x2iedn'), 'i2': ('i3', 'i4i'), 'i3': ('i3en', 'i4i'), 'i3en': ('i4i', 'i4i'), 'i4i': ('i4i', 'i4i'), 'd2': ('d3', 'd3'), 'd3': ('d3', 'd3'), 'h1': ('i3en', 'i4i'), 'p2': ('p3', 'p4d'), 'p3': ('p4d', 'p4d'), 'p4d': ('p4d', 'p4d'), 'g3': ('g4dn', 'g5'), 'g4dn': ('g5', 'g5'), 'g4ad': ('g5', 'g5'), 'g5': ('g5', 'g5'), 'inf1': ('inf2', 'inf2'), 'inf2': ('inf2', 'inf2'), 'trn1': ('trn1', 'trn1'), 'c5n': ('c6in', 'c7gn'), 'c6in': ('c7i', 'c7gn'), 'c7gn': ('c7gn', 'c7gn'), 'r5n': ('r6in', 'r6in'), 'r6in': ('r7i', 'r7i')}
INTEL_UPGRADE_MAP: dict[str, tuple[str, str]] = {'t2': ('t3', 't3a'), 't3': ('t3a', 't3a'), 't3a': ('t3a', 't3a'), 'm1': ('m4', 'm7i'), 'm2': ('m4', 'm7i'), 'm3': ('m4', 'm6i'), 'm4': ('m5', 'm7i'), 'm5': ('m6i', 'm7i'), 'm5a': ('m6i', 'm7i'), 'm5n': ('m6i', 'm7i'), 'm5zn': ('m6i', 'm7i'), 'm6i': ('m7i', 'm7i'), 'm6a': ('m7i', 'm7i'), 'm6g': ('m7i', 'm7i'), 'm7i': ('m7i', 'm7i'), 'm7g': ('m7i', 'm7i'), 'c1': ('c4', 'c7i'), 'c3': ('c4', 'c7i'), 'c4': ('c5', 'c7i'), 'c5': ('c6i', 'c7i'), 'c5a': ('c6i', 'c7i'), 'c5n': ('c6i', 'c7i'), 'c6i': ('c7i', 'c7i'), 'c6a': ('c7i', 'c7i'), 'c6g': ('c7i', 'c7i'), 'c7i': ('c7i', 'c7i'), 'c7g': ('c7i', 'c7i'), 'r3': ('r4', 'r7i'), 'r4': ('r5', 'r7i'), 'r5': ('r6i', 'r7i'), 'r5a': ('r6i', 'r7i'), 'r5b': ('r6i', 'r7i'), 'r5n': ('r6i', 'r7i'), 'r6i': ('r7i', 'r7i'), 'r6a': ('r7i', 'r7i'), 'r6g': ('r7i', 'r7i'), 'r7i': ('r7i', 'r7i'), 'r7g': ('r7i', 'r7i'), 'x1': ('x2idn', 'x2iedn'), 'x1e': ('x2idn', 'x2iedn'), 'x2idn': ('x2iedn', 'x2iedn'), 'i2': ('i3', 'i4i'), 'i3': ('i3en', 'i4i'), 'i3en': ('i4i', 'i4i'), 'i4i': ('i4i', 'i4i'), 'd2': ('d3', 'd3'), 'd3': ('d3', 'd3'), 'h1': ('i3en', 'i4i'), 'p2': ('p3', 'p4d'), 'p3': ('p4d', 'p4d'), 'p4d': ('p4d', 'p4d'), 'g3': ('g4dn', 'g5'), 'g4dn': ('g5', 'g5'), 'g4ad': ('g5', 'g5'), 'g5': ('g5', 'g5'), 'inf1': ('inf2', 'inf2'), 'inf2': ('inf2', 'inf2'), 'trn1': ('trn1', 'trn1'), 'c6in': ('c7i', 'c7gn'), 'c7gn': ('c7gn', 'c7gn')}
GRAV_UPGRADE_MAP: dict[str, tuple[str, str]] = {'t2': ('t3a', 't4g'), 't3': ('t4g', 't4g'), 't3a': ('t4g', 't4g'), 't4g': ('t4g', 't4g'), 'm1': ('m4', 'm7g'), 'm2': ('m4', 'm7g'), 'm3': ('m4', 'm6g'), 'm4': ('m5', 'm7g'), 'm5': ('m6g', 'm7g'), 'm5a': ('m6g', 'm7g'), 'm5n': ('m6g', 'm7g'), 'm5zn': ('m6g', 'm7g'), 'm6i': ('m6g', 'm7g'), 'm6g': ('m7g', 'm7g'), 'm6a': ('m6g', 'm7g'), 'm7i': ('m7g', 'm7g'), 'm7g': ('m7g', 'm7g'), 'c1': ('c4', 'c7g'), 'c3': ('c4', 'c7g'), 'c4': ('c5', 'c7g'), 'c5': ('c6g', 'c7g'), 'c5a': ('c6g', 'c7g'), 'c5n': ('c6g', 'c7g'), 'c6i': ('c6g', 'c7g'), 'c6g': ('c7g', 'c7g'), 'c6a': ('c6g', 'c7g'), 'c7i': ('c7g', 'c7g'), 'c7g': ('c7g', 'c7g'), 'r3': ('r4', 'r7g'), 'r4': ('r5', 'r7g'), 'r5': ('r6g', 'r7g'), 'r5a': ('r6g', 'r7g'), 'r5b': ('r6g', 'r7g'), 'r5n': ('r6g', 'r7g'), 'r6i': ('r6g', 'r7g'), 'r6g': ('r7g', 'r7g'), 'r6a': ('r6g', 'r7g'), 'r7i': ('r7g', 'r7g'), 'r7g': ('r7g', 'r7g'), 'x1': ('x2idn', 'x2iedn'), 'x1e': ('x2idn', 'x2iedn'), 'x2idn': ('x2iedn', 'x2iedn'), 'i2': ('i3', 'i4i'), 'i3': ('i3en', 'i4i'), 'i3en': ('i4i', 'i4i'), 'i4i': ('i4i', 'i4i'), 'd2': ('d3', 'd3'), 'd3': ('d3', 'd3'), 'h1': ('i3en', 'i4i'), 'p2': ('p3', 'p4d'), 'p3': ('p4d', 'p4d'), 'p4d': ('p4d', 'p4d'), 'g3': ('g4dn', 'g5'), 'g4dn': ('g5', 'g5'), 'g4ad': ('g5', 'g5'), 'g5': ('g5', 'g5'), 'inf1': ('inf2', 'inf2'), 'inf2': ('inf2', 'inf2'), 'trn1': ('trn1', 'trn1'), 'c6in': ('c7gn', 'c7gn'), 'c7gn': ('c7gn', 'c7gn')}
VALID_SIZES = {'nano', 'micro', 'small', 'medium', 'large', 'xlarge', '2xlarge', '3xlarge', '4xlarge', '6xlarge', '8xlarge', '9xlarge', '10xlarge', '12xlarge', '16xlarge', '18xlarge', '24xlarge', '32xlarge', '48xlarge', 'metal'}
SIZE_FALLBACK: dict[tuple[str, str], str] = {('m6g', 'nano'): 'medium', ('m7g', 'nano'): 'medium', ('c6g', 'nano'): 'medium', ('c7g', 'nano'): 'medium', ('r6g', 'nano'): 'medium', ('r7g', 'nano'): 'medium', ('m6g', 'micro'): 'medium', ('m7g', 'micro'): 'medium', ('c6g', 'micro'): 'medium', ('c7g', 'micro'): 'medium', ('m6g', 'small'): 'medium', ('m7g', 'small'): 'medium', ('c6g', 'small'): 'medium', ('c7g', 'small'): 'medium'}

def parse_instance(instance_type: str) -> tuple[str, str] | None:
    if not instance_type or not isinstance(instance_type, str):
        return None
    parts = instance_type.strip().lower().split('.')
    if len(parts) != 2:
        return None
    return (parts[0], parts[1])

def build_alt(target_family: str, size: str) -> str | None:
    resolved_size = SIZE_FALLBACK.get((target_family, size), size)
    candidate = f'{target_family}.{resolved_size}'
    return candidate

def _lookup_upgrade(family: str, mode: CPUFilterMode) -> tuple[str, str] | None:
    if mode == 'both':
        u = FAMILY_UPGRADE_MAP.get(family)
        if u is None:
            for key in FAMILY_UPGRADE_MAP:
                if family.startswith(key):
                    return FAMILY_UPGRADE_MAP[key]
        return u
    if mode == 'intel':
        u = INTEL_UPGRADE_MAP.get(family)
        if u is None:
            for key in INTEL_UPGRADE_MAP:
                if family.startswith(key):
                    return INTEL_UPGRADE_MAP[key]
        return u
    if mode == 'graviton':
        u = GRAV_UPGRADE_MAP.get(family)
        if u is None:
            for key in GRAV_UPGRADE_MAP:
                if family.startswith(key):
                    return GRAV_UPGRADE_MAP[key]
        return u
    if is_graviton_family(family):
        u = GRAV_UPGRADE_MAP.get(family)
        if u is None:
            for key in GRAV_UPGRADE_MAP:
                if family.startswith(key):
                    return GRAV_UPGRADE_MAP[key]
        return u
    u = INTEL_UPGRADE_MAP.get(family)
    if u is None:
        for key in INTEL_UPGRADE_MAP:
            if family.startswith(key):
                return INTEL_UPGRADE_MAP[key]
    return u

def get_recommendations(instance_type: str, cpu_filter: CPUFilterMode='both') -> dict[str, str | None]:
    result: dict[str, str | None] = {'family': None, 'size': None, 'alt1': None, 'alt2': None}
    parsed = parse_instance(instance_type)
    if parsed is None:
        logger.warning('Cannot parse instance type (value omitted for security)')
        return result
    (family, size) = parsed
    result['family'] = family
    result['size'] = size
    mode: CPUFilterMode = cpu_filter if cpu_filter in ('default', 'intel', 'graviton', 'both') else 'both'
    upgrade = _lookup_upgrade(family, mode)
    if upgrade is None:
        logger.debug('No upgrade path for family (details omitted)')
        return result
    (alt1_family, alt2_family) = upgrade
    if alt1_family != family:
        result['alt1'] = build_alt(alt1_family, size)
    if alt2_family != family and alt2_family != alt1_family:
        result['alt2'] = build_alt(alt2_family, size)
    elif alt2_family != family and alt2_family == alt1_family:
        result['alt2'] = None
    elif alt2_family == family:
        result['alt2'] = None
    return result
