import logging
from typing import Iterable

from django.db.models import QuerySet

from dcim.models import Device
from virtualization.models import VirtualMachine

from nbxsync.jobs import (
    SyncTemplatesJob,
    SyncHostJob,
    SyncProxyGroupJob,
    SyncProxyJob,
)
from nbxsync.models import (
    ZabbixServer,
    ZabbixServerAssignment,
    ZabbixHostgroup,
    ZabbixHostgroupAssignment,
)
from nbxsync.services import autofill
from nbxsync.services.reconcile import ensure_device_assignments

logger = logging.getLogger(__name__)


def _get_server(server_id: int):
    try:
        return ZabbixServer.objects.get(pk=server_id)
    except ZabbixServer.DoesNotExist:
        logger.warning("Server %s does not exist", server_id)
        return None

# Хосты, которые считаем "живыми" для синка
ACTIVE_DEVICE_STATUSES = ("active", "staged")


def _eligible_devices_for_server(zabbixserver: ZabbixServer) -> QuerySet[Device]:
    """
    Устройства, привязанные к этому ZabbixServer'у, с нужным статусом и primary IP.
    Пока только Device, без VM.
    """
    qs = (
        Device.objects.filter(
            zabbixserverassignment__zabbixserver=zabbixserver,
            status__slug__in=ACTIVE_DEVICE_STATUSES,
        )
        .filter(primary_ip4__isnull=False)
        .distinct()
    )
    return qs


def sync_templates_from_zabbix(server_id: int) -> None:
    server = _get_server(server_id)
    if not server:
        return
    worker = SyncTemplatesJob(instance=server)
    worker.run()



def ensure_hostgroup_assignments(server_id: int) -> None:
    server = _get_server(server_id)
    if not server:
        return
    logger.info("ensure_hostgroup_assignments(%s)", server_id)



def sync_hostgroups_to_zabbix(server_id: int) -> None:
    server = _get_server(server_id)
    if not server:
        return
    logger.info("sync_hostgroups_to_zabbix(%s)", server_id)



def sync_proxy_groups(server_id: int) -> None:
    server = _get_server(server_id)
    if not server:
        return
    logger.info("sync_proxy_groups(%s)", server_id)


def sync_proxies(server_id: int) -> None:
    server = _get_server(server_id)
    if not server:
        return
    logger.info("sync_proxies(%s)", server_id)


def synchost_assignment(assignment_id: int) -> None:
    try:
        assignment = ZabbixServerAssignment.objects.get(pk=assignment_id)
    except ZabbixServerAssignment.DoesNotExist:
        logger.warning("assignment %s missing", assignment_id)
        return

    obj = assignment.assigned_object
    if not obj:
        return

    logger.info(
        "HostSync START assignment %s (trigger server=%s) device=%s; syncing all assigned Zabbix servers",
        assignment_id,
        assignment.zabbixserver_id,
        obj.name,
    )

    # Sync All uses one assignment only as the trigger. Before enumerating the
    # object's assignments, restore every approved autofill target for Devices so
    # a missing secondary-server assignment cannot silently prevent fan-out.
    if obj._meta.model_name == 'device':
        try:
            target_count = ensure_device_assignments(obj)
            if target_count:
                logger.info(
                    'HostSync prepared %s autofill targets for device=%s',
                    target_count,
                    obj.name,
                )
        except autofill.AutofillError as error:
            logger.warning(
                'HostSync could not prepare all autofill targets for device=%s: %s; '
                'continuing with existing assignments',
                obj.name,
                error,
            )
        except Exception:
            logger.exception(
                'HostSync unexpected autofill preparation failure for device=%s; '
                'continuing with existing assignments',
                obj.name,
            )

    try:
        # Once selected, SyncHostJob processes every ZabbixServerAssignment
        # belonging to this NetBox object.
        worker = SyncHostJob(instance=obj)
        worker.run()
    except Exception as e:
        logger.error(
            "HostSync FAILED assignment %s (trigger server=%s) device=%s error=%s",
            assignment_id,
            assignment.zabbixserver_id,
            obj.name,
            e,
        )
        raise
