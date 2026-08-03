from __future__ import annotations

import re


def _candidate_keys(value):
    for candidate in (getattr(value, 'slug', ''), getattr(value, 'name', '')):
        key = re.sub(r'[^A-Z0-9]', '', str(candidate or '').upper())
        if key:
            yield key


def resolve_site_key(device) -> str:
    """Resolve a monitoring site from the NetBox site-group hierarchy first.

    Physical sites are commonly named by address, while their Site Group carries
    the stable monitoring code such as TARAZ, TUSHINO, or NOVOMOSKOVSK.
    """
    from nbxsync.services.autofill import AutofillError, SERVER_SITE_MAP

    site = getattr(device, 'site', None)
    group = getattr(site, 'group', None)
    seen = set()

    while group is not None:
        identity = getattr(group, 'pk', None) or id(group)
        if identity in seen:
            break
        seen.add(identity)

        for key in _candidate_keys(group):
            if key in SERVER_SITE_MAP:
                return key
        group = getattr(group, 'parent', None)

    for key in _candidate_keys(site):
        if key in SERVER_SITE_MAP:
            return key

    raise AutofillError(f'Unsupported site or site group: {site or "not set"}')
