from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.settings import get_plugin_settings


DEVICE_SYNC_STATUSES = frozenset({'active', 'staged', 'planned'})


def status_slug(instance) -> str:
    status = getattr(instance, 'status', None)
    return str(getattr(status, 'slug', None) or status or '').strip().lower()


def desired_host_status(instance):
    """Return the authoritative desired Zabbix state for a NetBox host object.

    Device lifecycle is a hard policy and deliberately does not depend on a
    deployment override of statusmapping:
      active -> enabled
      staged/planned -> disabled
      every other Device status -> deleted

    Non-Device objects retain the plugin's configurable status mapping.
    """
    object_type = instance._meta.model_name
    slug = status_slug(instance)

    if object_type == 'device':
        if slug == 'active':
            return ZabbixHostStatus.ENABLED
        if slug in {'staged', 'planned'}:
            return ZabbixHostStatus.DISABLED
        return ZabbixHostStatus.DELETED

    pluginsettings = get_plugin_settings()
    status_mapping = getattr(pluginsettings.statusmapping, object_type, {})
    return status_mapping.get(getattr(instance, 'status', None))
