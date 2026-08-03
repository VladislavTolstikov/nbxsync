from . import autofill as _autofill
from .site_resolution import resolve_site_key

_autofill._site_key = resolve_site_key

AutofillError = _autofill.AutofillError
AutofillResult = _autofill.AutofillResult
fill_nbxsync_device = _autofill.fill_nbxsync_device

__all__ = ('AutofillError', 'AutofillResult', 'fill_nbxsync_device')
