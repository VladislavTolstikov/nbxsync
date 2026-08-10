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
from nbxsync.services.host_policy import (
    DEVICE_SYNC_STATUSES,
    desired_host_status,
    status_slug,
)
from nbxsync.utils import ZabbixConnection


logger = logging.getLogger(__name__)

OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'
HOST_OBJECT_MODELS = frozenset({'device', 'virtualmachine', 'virtualdevicecontext'})


def _target_for_server(device, server_id):
    site_key = autofill._site_key(device)
    target_key = autofill.SERVER_SITE_MAP[site_key]
    for target in autofill.TARGETS_BY_SITE[target_key]:
        if target.server_id == server_id:
            return site_key, target
    return site_key, None


def device_is_auto_managed_on_server(device, server_id):
    """Return True/False when management scope is known, otherwise None.

    Reconciliation must fail safe. A transient or unsupported site mapping must
    never be interpreted as proof that an existing managed host should be
    deleted.
    """
    role = autofill._role(device)
    if role in autofill.PASSIVE_ROLES or role in autofill.MANUAL_ROLES:
        return False
    if autofill.select_rule(device) is None:
        return False
    try:
        _, target = _target_for_server(device, server_id)
    except Exception as error:
        logger.warning(
            'Unable to evaluate autofill target for device=%s server=%s: %s',
            device,
            server_id,
            error,
        )
        return None
    return target is not None


def ensure_device_assignment_for_server(device, zabbixserver) -> bool:
    """Create local NbxSync config for one auto-managed Device/server pair.

    This is used by the periodic background preparation. It accepts active,
    staged and planned devices, while the UI Fill NbxSync action remains
    active-only.
    """
    if status_slug(device) not in DEVICE_SYNC_STATUSES:
        return False
    if getattr(device, 'site', None) is None:
        return False

    primary_ip = getattr(device, 'primary_ip4', None) or getattr(device, 'primary_ip6', None)
    if primary_ip is None:
        return False

    if device_is_auto_managed_on_server(device, zabbixserver.pk) is not True:
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


def ensure_device_assignments(device) -> int:
    """Ensure all approved autofill targets for one Device.

    Sync All fans a selected NetBox object out to every assigned Zabbix server.
    Therefore a missing assignment on a secondary target must be restored before
    SyncHostJob enumerates the object's assignments.
    """
    if status_slug(device) not in DEVICE_SYNC_STATUSES:
        return 0
    if getattr(device, 'site', None) is None:
        return 0

    primary_ip = getattr(device, 'primary_ip4', None) or getattr(device, 'primary_ip6', None)
    if primary_ip is None:
        return 0

    role = autofill._role(device)
    if role in autofill.PASSIVE_ROLES or role in autofill.MANUAL_ROLES:
        return 0

    rule = autofill.select_rule(device)
    if rule is None:
        return 0

    site_key, targets, servers, templates, proxies = autofill._preflight(device, rule)
    ct = ContentType.objects.get_for_model(device, for_concrete_model=False)
    result = autofill.AutofillResult()
    group_names = autofill._group_names(device, site_key, result)

    for target in targets:
        proxy = proxies.get(target.proxy_id) if target.proxy_id is not None else None
        server = servers[target.server_id]
        autofill._ensure_server(device, ct, target, proxy, result)
        autofill._ensure_groups(device, ct, server, group_names, result)
        autofill._ensure_interface(device, ct, server, rule, primary_ip, result)
        autofill._ensure_templates(device, ct, server, templates, rule, result)

    autofill._ensure_inventory(device, ct, result)
    return len(targets)


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


def _owner_tag_values(host):
    type_values = []
    id_values = []

    for tag in host.get('tags', []):
        tag_name = str(tag.get('tag', ''))
        value = str(tag.get('value', ''))
        if tag_name == OWNER_TYPE_TAG:
            type_values.append(value)
        elif tag_name == OWNER_ID_TAG:
            id_values.append(value)

    return type_values, id_values


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
        return None, None, False

    model = content_type.model_class()
    if model is None:
        return None, content_type.model, False

    return model._default_manager.filter(pk=object_id).first(), model._meta.model_name, True


def _clear_interface_ids(zabbixserver, owner_type_id, owner_id):
    ZabbixHostInterface.objects.filter(
        zabbixserver=zabbixserver,
        assigned_object_type_id=owner_type_id,
        assigned_object_id=owner_id,
    ).update(interfaceid=None)


def _clear_matching_assignment_hostid(assignment, hostid):
    if assignment is None or assignment.hostid is None:
        return
    if str(assignment.hostid) != str(hostid):
        return
    assignment.hostid = None
    assignment.save(update_fields=['hostid'])


def _enforce_lifecycle_status(api, host, desired):
    wanted = None
    if desired == ZabbixHostStatus.ENABLED:
        wanted = 0
    elif desired == ZabbixHostStatus.DISABLED:
        wanted = 1

    if wanted is None:
        return

    hostid = host.get('hostid')
    if not hostid:
        return
    if str(host.get('status')) == str(wanted):
        return

    api.host.update(hostid=hostid, status=wanted)
    logger.info(
        'Reconcile enforced host status hostid=%s status=%s',
        hostid,
        wanted,
    )


def _reconcile_one_host(api, zabbixserver, server_id, host) -> bool:
    type_values, id_values = _owner_tag_values(host)

    if not type_values and not id_values:
        return False
    if len(type_values) != 1 or len(id_values) != 1:
        logger.warning(
            'Reconcile ignored hostid=%s with ambiguous NbxSync owner tags '
            'type_values=%r id_values=%r',
            host.get('hostid'),
            type_values,
            id_values,
        )
        return False

    owner_ids = _parse_owner_ids(type_values[0], id_values[0])
    if owner_ids is None:
        logger.warning(
            'Reconcile ignored hostid=%s with invalid NbxSync owner tags type=%r id=%r',
            host.get('hostid'),
            type_values[0],
            id_values[0],
        )
        return False
    owner_type_id, owner_id = owner_ids

    hostid = host.get('hostid')
    if not hostid:
        return False

    owner, owner_model_name, owner_type_valid = _resolve_owner(
        owner_type_id,
        owner_id,
    )
    if not owner_type_valid:
        logger.warning(
            'Reconcile ignored hostid=%s: ContentType id=%s cannot be safely resolved',
            hostid,
            owner_type_id,
        )
        return False
    if owner_model_name not in HOST_OBJECT_MODELS:
        logger.warning(
            'Reconcile ignored hostid=%s: tagged owner type %s is not a host object',
            hostid,
            owner_model_name,
        )
        return False

    assignment = ZabbixServerAssignment.objects.filter(
        zabbixserver=zabbixserver,
        assigned_object_type_id=owner_type_id,
        assigned_object_id=owner_id,
    ).first()
    assignment_matches_host = bool(
        assignment is not None
        and assignment.hostid is not None
        and str(assignment.hostid) == str(hostid)
    )

    desired = desired_host_status(owner) if owner is not None else None

    if owner is not None and assignment_matches_host:
        # Even if full configuration sync later fails, lifecycle state itself is
        # cheap and unambiguous to enforce on the tagged host.
        _enforce_lifecycle_status(api, host, desired)
        # DELETED hosts are left to the normal assignment-aware delete path so
        # local maintenance/interface state is cleaned consistently.
        return False

    should_delete = owner is None

    if owner is not None:
        if desired == ZabbixHostStatus.DELETED:
            # If hostid is missing/stale on the assignment, the normal delete
            # path cannot reach this tagged host, so delete it here.
            should_delete = True
        elif assignment is not None:
            # The tagged host is definitely ours but the assignment hostid is
            # missing/stale. Preserve the host, enforce enabled/disabled state,
            # and let identity guard repair the local ID on regular sync.
            _enforce_lifecycle_status(api, host, desired)
            logger.warning(
                'Reconcile kept hostid=%s (%s): assignment=%s has hostid=%s',
                hostid,
                host.get('host'),
                assignment.pk,
                assignment.hostid,
            )
            return False
        elif owner_model_name == 'device':
            # Only explicit proof that the Device is outside the approved
            # autofill scope may delete a live orphan. Unknown/evaluation
            # failures are kept for safety.
            managed = device_is_auto_managed_on_server(owner, server_id)
            should_delete = managed is False
            if not should_delete:
                _enforce_lifecycle_status(api, host, desired)
        else:
            # VMs/VDCs have no autofill recovery path. Without an assignment the
            # tagged Zabbix host is an orphan.
            should_delete = True

    if not should_delete:
        logger.warning(
            'Reconcile kept managed hostid=%s (%s): owner exists but no server assignment',
            hostid,
            host.get('host'),
        )
        return False

    api.host.delete([hostid])
    _clear_interface_ids(zabbixserver, owner_type_id, owner_id)
    _clear_matching_assignment_hostid(assignment, hostid)
    logger.info(
        'Reconcile deleted NbxSync hostid=%s host=%s owner_type_id=%s owner_id=%s',
        hostid,
        host.get('host'),
        owner_type_id,
        owner_id,
    )
    return True


def reconcile_managed_hosts(server_id: int) -> None:
    """Reconcile Zabbix hosts carrying unambiguous NbxSync ownership tags.

    Invalid/ambiguous ownership is always non-destructive. Each host is processed
    independently so one Zabbix API failure does not prevent reconciliation of
    the remaining managed hosts.
    """
    try:
        zabbixserver = ZabbixServer.objects.get(pk=server_id)
    except ZabbixServer.DoesNotExist:
        logger.warning('Reconcile skipped missing Zabbix server id=%s', server_id)
        return

    deleted = 0
    errors = []

    with ZabbixConnection(zabbixserver) as api:
        hosts = api.host.get(
            output=['hostid', 'host', 'name', 'status'],
            selectTags=['tag', 'value'],
        )

        for host in hosts:
            try:
                if _reconcile_one_host(api, zabbixserver, server_id, host):
                    deleted += 1
            except Exception as error:
                hostid = host.get('hostid')
                errors.append(f'hostid={hostid}: {error}')
                logger.exception(
                    'Reconcile failed hostid=%s server=%s',
                    hostid,
                    server_id,
                )

    logger.info(
        'Reconcile finished server=%s deleted=%s errors=%s',
        server_id,
        deleted,
        len(errors),
    )

    if errors:
        raise RuntimeError(
            'Reconciliation completed with errors on one or more hosts: '
            + ' | '.join(errors)
        )
