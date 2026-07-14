import logging

from .hostinterfacesync import HostInterfaceSync
from .hostsync import HostSync

logger = logging.getLogger(__name__)

_original_host_sync = HostSync.sync
_original_interface_sync = HostInterfaceSync.sync


def _expected_host_key(sync_obj):
    name = str(sync_obj.obj.assigned_object)
    return sync_obj.sanitize_string(name)[:64]


def guarded_host_sync(self, obj_id=None):
    object_id = obj_id