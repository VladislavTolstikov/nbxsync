import logging

from django.contrib.contenttypes.models import ContentType

from dcim.models import Device

from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.models import (
    ZabbixHostInterface,
    ZabbixProxy,
    ZabbixServer,
    ZabbixServerAssignment,
    ZabbixTemplate,
)
from nbxsync.services import autofill
from nbxsync.settings import get_plugin_settings
from nbxsync.utils import ZabbixConnection


logger = logging.getLogger(__name__)

OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'
DEVICE_SYNC_STATUSES = frozenset({'active', 'staged', 'planned'})
HOST_OBJECT_MODELS = frozenset({'device', 'virtualmachine', 'virtualdevicecontext'})


def status_slug(instance) -> str:
    status = getattr(instance, 'status', None)
    return str(getattr(status, 'slug', None) or status or '').strip().lower()


def desired_host_status(instance):
    """Return the desired Zabbix host state for a NetBox object.

    Device policy is intentionally strict: active is enabled, staged/planned are
    disabled, and every other (including custom) status means delete.
    Non-device objects continue to use the configured status mapping.
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


def _target_for_server(device, server_id):
    site_key = autofill._site_key(device)
    target_key = autofill.SERVER_SITE_MAP[site_key]
    for target in autofill.TARGETS_BY_SITE[target_key]:
        if target.server_id == server_id:
            return site_key, target
    return site_key, None


def device_is_auto_managed_on_server(device, server_id) -> bool:
    """Return whether current autofill rules place this Device on this server."""
    role = autofill._role(device)
    if role in autofill.PASSIVE_ROLES or role in autofill.MANUAL_ROLES:
        return False
    if autofill.select_rule(device) is None:
        return False
    try:
        _, target = _target_for_server(device, server_id)
    except Exception:
        return False
    return target is not None


def ensure_device_assignment_for_server(device, zabbixserver) -> bool:
    """Create local NbxSync config for one auto-managed Device/server pair.

    This is the background/full-sync counterpart of the UI autofill action. It
    accepts active, staged and planned devices, while the UI button remains
    active-only.
    """
    if status_slug(device) not in DEVICE_SYNC_STATUSES:
        return False
    if getattr(device, 'site', None) is None:
        return False

    primary_ip = getattr(device, 'primary_ip4', None) or getattr(device, 'primary_ip6', None)
    if primary_ip is None:
        return False

    if not device_is_auto_managed_on_server(device, zabbixserver.pk):
        return False

    rule = autofill.select_rule(device)
    site_key, target = _target_for_server(device, zabbixserver.pk)

    templates = {
        row.name: row
        for row in ZabbixTemplate.objects.filter(
            zabbixserver=zabbixserver,
            name__in=rule.templates,
        )
    }
    missing_templates = [name for name in rule.templates if name not in templates]
    if missing_templates:
        raise autofill.AutofillError(
            f'Missing templates on {zabbixserver.name}: {", ".join(missing_templates)}'
        )

    proxy = None
    if target.proxy_id is not None:
        proxy = ZabbixProxy.objects.filter(pk=target.proxy_id, zabbixserver=zabbixserver).first()
        if proxy is None:
            raise autofill.AutofillError(
                f'Missing proxy {target.proxy_id} on {zabbixserver.name}'
            )

    ct = ContentType.objects.get_for_model(device, for_concrete_model=False)
    result = autofill.AutofillResult()
    group_names = autofill._group_names(device, site_key, result)

    autofill._ensure_server(device, ct, target, proxy, result)
    autofill._ensure_groups(device, ct, zabbixserver, group_names, result)
    autofill._ensure_interface(device, ct, zabbixserver, rule, primary_ip, result)

    template_map = {(zabbixserver.pk, name): row for name, row in templates.items()}
    autofill._ensure_templates(device, ct, zabbixserver, template_map, rule, result)
    autofill._ensure_inventory(device, ct, result)
    return True


def prepare_server_assignments(zabbixserver) -> int:
    """Ensure assignments for all auto-managed active/staged/planned Devices."""
    prepared = 0
    devices = (
        Device.objects.filter(status__in=DEVICE_SYNC_STATUSES)
        .select_related(
            'role',
            'site',
            'device_type__manufacturer',
            'primary_ip4',
            'primary_ip6',
        )
    )

    for device in devices.iterator():
        try:
            if ensure_device_assignment_for_server(device, zabbixserver):
                prepared += 1
        except autofill.AutofillError as error:
            logger.warning(
                'SyncAll autofill skipped device=%s server=%s: %s',
                device,
                zabbixserver.pk,
                error,
            )
        except Exception:
            logger.exception(
                'SyncAll autofill failed device=%s server=%s',
                device,
                zabbixserver.pk,
            )

    return prepared


def _tag_map(host):
    return {
        str(tag.get('tag')): str(tag.get('value', ''))
        for tag in host.get('tags', [])
        if tag.get('tag')
    }


def _parse_owner_ids(owner_type_value, owner_id_value):
    try:
        owner_type_id = int(owner_type_value)
        owner_id = int(owner_id_value)
    except (TypeError, ValueError):
        return None

    if owner_type_id <= 0 or owner_id <= 0:
        return None
    return owner_type_id, owner_id


def _resolve_owner(content_type_id, object_id):
    try:
        content_type = ContentType.objects.get(pk=content_type_id)
    except ContentType.DoesNotExist:
        return None, False

    model = content_type.model_class()
    if model is None:
        return None, False

    return model._default_manager.filter(pk=object_id).first(), True


def _clear_interface_ids(zabbixserver, owner_type_id, owner_id):
    ZabbixHostInterface.objects.filter(
        zabbixserver=zabbixserver,
        assigned_object_type_id=owner_type_id,
        assigned_object_id=owner_id,
    ).update(interfaceid=None)


def reconcile_managed_hosts(server_id: int) -> None:
    """Clean Zabbix hosts owned by NbxSync even when their assignment is gone.

    Ownership is accepted only when both identity tags contain valid numeric IDs.
    Hosts without valid ownership tags are never touched. Hosts with a current
    assignment are left to the normal per-host job so HostSync.delete can clean
    local state as well.
    """
    try:
        zabbixserver = ZabbixServer.objects.get(pk=server_id)
    except ZabbixServer.DoesNotExist:
        logger.warning('Reconcile skipped missing Zabbix server id=%s', server_id)
        return

    deleted = 0
    with ZabbixConnection(zabbixserver) as api:
        hosts = api.host.get(
            output=['hostid', 'host', 'name', 'status'],
            selectTags=['tag', 'value'],
        )

        for host in hosts:
            tags = _tag_map(host)
            owner_type_value = tags.get(OWNER_TYPE_TAG)
            owner_id_value = tags.get(OWNER_ID_TAG)

            if owner_type_value is None and owner_id_value is None:
                continue
            if owner_type_value is None or owner_id_value is None:
                logger.warning(
                    'Reconcile ignored hostid=%s with incomplete NbxSync owner tags',
                    host.get('hostid'),
                )
                continue

            owner_ids = _parse_owner_ids(owner_type_value, owner_id_value)
            if owner_ids is None:
                logger.warning(
                    'Reconcile ignored hostid=%s with invalid NbxSync owner tags type=%r id=%r',
                    host.get('hostid'),
                    owner_type_value,
                    owner_id_value,
                )
                continue
            owner_type_id, owner_id = owner_ids

            assignment = ZabbixServerAssignment.objects.filter(
                zabbixserver=zabbixserver,
                assigned_object_type_id=owner_type_id,
                assigned_object_id=owner_id,
            ).first()
            if assignment is not None:
                continue

            owner, owner_type_valid = _resolve_owner(owner_type_id, owner_id)
            if not owner_type_valid:
                logger.warning(
                    'Reconcile ignored hostid=%s: ContentType id=%s cannot be resolved',
                    host.get('hostid'),
                    owner_type_id,
                )
                continue

            should_delete = owner is None
            if owner is not None:
                model_name = owner._meta.model_name
                if model_name == 'device':
                    should_delete = (
                        desired_host_status(owner) == ZabbixHostStatus.DELETED
                        or not device_is_auto_managed_on_server(owner, server_id)
                    )
                elif model_name in HOST_OBJECT_MODELS:
                    # VMs/VDCs have no autofill recovery path. Without an
                    # assignment the tagged Zabbix host is an orphan.
                    should_delete = True
                else:
                    logger.warning(
                        'Reconcile ignored hostid=%s: tagged owner type %s is not a host object',
                        host.get('hostid'),
                        model_name,
                    )
                    continue

            if not should_delete:
                logger.warning(
                    'Reconcile kept managed hostid=%s (%s): owner exists but no server assignment',
                    host.get('hostid'),
                    host.get('host'),
                )
                continue

            hostid = host.get('hostid')
            if not hostid:
                continue

            api.host.delete([hostid])
            _clear_interface_ids(zabbixserver, owner_type_id, owner_id)
            deleted += 1
            logger.info(
                'Reconcile deleted NbxSync hostid=%s host=%s owner_type_id=%s owner_id=%s',
                hostid,
                host.get('host'),
                owner_type_id,
                owner_id,
            )

    logger.info('Reconcile finished server=%s deleted=%s', server_id, deleted)
