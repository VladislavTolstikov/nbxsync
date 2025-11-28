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

logger = logging.getLogger(__name__)

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


def sync_templates_from_zabbix(zabbixserver: ZabbixServer) -> None:
    """
    Глобальный шаг 1: синк шаблонов для сервера.
    Просто обёртка над штатным SyncTemplatesJob.
    """
    worker = SyncTemplatesJob(instance=zabbixserver)
    worker.run()


def ensure_hostgroup_assignments(zabbixserver: ZabbixServer) -> None:
    """
    Глобальный шаг 2: подготовка/создание хостгрупп и назначений.
    Пока заглушка — позже сюда завезём логику TYPE/LOCATION и MFR/MODEL.
    """
    logger.info("ensure_hostgroup_assignments(%s) called", zabbixserver.pk)
    # Здесь позже:
    #  - создание ZabbixHostgroup-объектов в NetBox (TYPE/LOC + MFR/MODEL)
    #  - синк этих групп в сам Zabbix
    #  - (опционально) массовое создание ZabbixHostgroupAssignment для устройств
    return


def sync_hostgroups_to_zabbix(zabbixserver: ZabbixServer) -> None:
    """
    Глобальный шаг 2b: синк объектов ZabbixHostgroup в сам Zabbix.
    Пока заглушка, чтобы удовлетворить импорт в nbxsync.worker.__init__.
    """
    logger.info("sync_hostgroups_to_zabbix(%s) called", zabbixserver.pk)
    return


def sync_proxy_groups(zabbixserver: ZabbixServer) -> None:
    """
    Глобальный шаг 3: синк групп прокси (ProxyGroup).
    Пока заглушка.
    """
    logger.info("sync_proxy_groups(%s) called", zabbixserver.pk)
    return


def sync_proxies(zabbixserver: ZabbixServer) -> None:
    """
    Глобальный шаг 4: синк прокси (Proxy).
    Пока заглушка.
    """
    logger.info("sync_proxies(%s) called", zabbixserver.pk)
    return


def synchost_assignment(assignment: ZabbixServerAssignment) -> None:
    """
    Шаг 5: синк конкретного assignment'а (host) в Zabbix.
    Вызывается из RQ-джобы с instance=ZabbixServerAssignment.
    """
    obj = assignment.assigned_object
    if obj is None:
        logger.warning("synchost_assignment(%s): no assigned_object", assignment.pk)
        return

    # Device: нужен статус и primary IP
    if isinstance(obj, Device):
        if obj.status.slug not in ACTIVE_DEVICE_STATUSES:
            logger.info(
                "synchost_assignment(%s): device %s status=%s -> skip",
                assignment.pk,
                obj.pk,
                obj.status.slug,
            )
            return
        if not (obj.primary_ip or obj.primary_ip4 or obj.primary_ip6):
            logger.info(
                "synchost_assignment(%s): device %s has no primary IP -> skip",
                assignment.pk,
                obj.pk,
            )
            return

    # VM: пока только по статусу
    if isinstance(obj, VirtualMachine):
        if obj.status.slug not in ACTIVE_DEVICE_STATUSES:
            logger.info(
                "synchost_assignment(%s): VM %s status=%s -> skip",
                assignment.pk,
                obj.pk,
                obj.status.slug,
            )
            return

    worker = SyncHostJob(instance=obj)
    worker.run()