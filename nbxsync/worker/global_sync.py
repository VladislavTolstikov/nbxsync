import logging

from django.db import IntegrityError, transaction
from django.contrib.contenttypes.models import ContentType

from dcim.models import Device, Site

from nbxsync.jobs import SyncTemplatesJob
from nbxsync.models import (
    ZabbixServer,
    ZabbixServerAssignment,
    ZabbixHostGroup,
    ZabbixHostGroupAssignment,
    ZabbixProxyGroup,
    ZabbixProxy,
)
from nbxsync.utils import get_assigned_zabbixobjects
from nbxsync.utils.sync import (
    HostSync,
    HostInterfaceSync,
    HostGroupSync,
    ProxyGroupSync,
    ProxySync,
)
from nbxsync.utils.sync.safe_sync import safe_sync

logger = logging.getLogger("worker")


# ---------------------------------------------------------------------------
# Маппинг TYPE/LOCATION для ensure_hostgroup_assignments
# ---------------------------------------------------------------------------

SITEGROUP_MAP = {
    "MIUSSI": "MK",
    "NOVOMOSKOVSK": "NI",
    "STUDGORODOK": "SG",
    "TIRHTU": "TZ",
    "TUSHINO": "TK",
    "MK": "MK",
    "NI": "NI",
    "SG": "SG",
    "TZ": "TZ",
    "TK": "TK",
}

ROLE_MAP = {
    "aggregation-switchboard": "NET",
    "c2000_controllers": "ENG",
    "cn": "SRV",
    "hpcn": "SRV",
    "csw": "NET",
    "rtr": "NET",
    "sw-fc": "NET",
    "iot": "NET",
    "end-device": "NET",
    "wifi-bridge": "NET",
    "cam": "HID",
    "safety": "ENG",
    "skud": "ENG",
    "nas": "SRV",
    "rtp": "SRV",
    "sw-l2-zk": "NET",
    "td": "SRV",
    "srv-dvr": "SRV",
    "netping": "SRV",
    "ip-ph": "NET",
    "ap": "NET",
    "sw-l2": "NET",
    "backup": "SRV",
    "pdu": "ENG",
    "fw": "NET",
    "psw": None,  # пропуск
    "engineering-systems": "ENG",
}


# ---------------------------------------------------------------------------
# Глобальные шаги Sync All
# ---------------------------------------------------------------------------

def sync_templates_from_zabbix(zabbixserver_id: int) -> None:
    server = ZabbixServer.objects.get(pk=zabbixserver_id)
    logger.info("Sync templates from Zabbix started (server=%s)", zabbixserver_id)
    worker = SyncTemplatesJob(instance=server)
    worker.run()
    logger.info("Sync templates from Zabbix finished (server=%s)", zabbixserver_id)


def ensure_hostgroup_assignments(zabbixserver_id: int) -> None:
    """
    Встроенный аналог скрипта назначения хостгрупп.

      - TYPE/LOCATION  (ROLE_MAP + SITEGROUP_MAP)
      - MANUFACTURER/MODEL  ("<Vendor>/<Model>")

    Только создаёт недостающие ZabbixHostGroupAssignment, ничего не удаляет.
    """
    server = ZabbixServer.objects.get(pk=zabbixserver_id)

    status_qs = Status.objects.filter(slug__in=["active", "staged"])
    devices = (
        Device.objects.filter(status__in=["active", "staged"])
        .exclude(primary_ip4__isnull=True, primary_ip__isnull=True)
        .select_related(
            "role",
            "site",
            "site__group",
            "device_type",
            "device_type__manufacturer",
        )
    )

    logger.info(
        "ensure_hostgroup_assignments: devices candidate count=%s (server=%s)",
        devices.count(),
        zabbixserver_id,
    )

    # site_id -> LOC
    site_loc: dict[int, str] = {}
    for s in Site.objects.select_related("group"):
        gslug = (getattr(s.group, "slug", "") or "").upper().strip()
        sslug = (s.slug or "").upper().strip()
        loc = SITEGROUP_MAP.get(gslug) or SITEGROUP_MAP.get(sslug)
        if loc:
            site_loc[s.id] = loc

    # name -> ZabbixHostGroup
    hg_by_name: dict[str, ZabbixHostGroup] = {
        hg.name: hg
        for hg in ZabbixHostGroup.objects.filter(zabbixserver=server)
    }

    ct_device = ContentType.objects.get_for_model(Device)

    created = exists = errors = 0

    for dev in devices.iterator():
        site_id = getattr(dev.site, "id", None)
        loc = site_loc.get(site_id)
        if not loc:
            continue

        pairs: list[tuple[int, int]] = []  # (device_id, zabbixhostgroup_id)

        # TYPE/LOCATION
        role_slug = (getattr(dev.role, "slug", "") or "").strip().lower()
        if role_slug:
            typ = ROLE_MAP.get(role_slug)
            if typ:
                gname = f"{typ}/{loc}"
                hg = hg_by_name.get(gname)
                if hg:
                    pairs.append((dev.pk, hg.pk))

        # MANUFACTURER/MODEL
        dt = dev.device_type
        mfr = (getattr(getattr(dt, "manufacturer", None), "name", "") or "").strip()
        model = (getattr(dt, "model", "") or "").strip()
        if mfr and model:
            gname2 = f"{mfr}/{model}"
            hg2 = hg_by_name.get(gname2)
            if hg2:
                pairs.append((dev.pk, hg2.pk))

        for dev_id, hg_id in pairs:
            try:
                with transaction.atomic():
                    _, created_flag = ZabbixHostGroupAssignment.objects.get_or_create(
                        zabbixserver=server,
                        assigned_object_type=ct_device,
                        assigned_object_id=dev_id,
                        zabbixhostgroup_id=hg_id,
                    )
                if created_flag:
                    created += 1
                else:
                    exists += 1
            except IntegrityError:
                exists += 1
            except Exception as exc:
                errors += 1
                logger.exception(
                    "ensure_hostgroup_assignments: failed for device=%s group=%s: %s",
                    dev_id,
                    hg_id,
                    exc,
                )

    logger.info(
        "ensure_hostgroup_assignments finished (server=%s): created=%s, exists=%s, errors=%s",
        zabbixserver_id,
        created,
        exists,
        errors,
    )


def sync_hostgroups_to_zabbix(zabbixserver_id: int) -> None:
    server = ZabbixServer.objects.get(pk=zabbixserver_id)
    hostgroups = list(ZabbixHostGroup.objects.filter(zabbixserver=server))

    logger.info(
        "sync_hostgroups_to_zabbix: %s groups to sync (server=%s)",
        len(hostgroups),
        zabbixserver_id,
    )

    for hg in hostgroups:
        try:
            safe_sync(HostGroupSync, hg)
        except Exception as exc:
            logger.exception(
                "sync_hostgroups_to_zabbix: failed for hg pk=%s: %s",
                getattr(hg, "pk", None),
                exc,
            )


def sync_proxy_groups(zabbixserver_id: int) -> None:
    server = ZabbixServer.objects.get(pk=zabbixserver_id)
    groups = list(ZabbixProxyGroup.objects.filter(zabbixserver=server))

    logger.info(
        "sync_proxy_groups: %s proxy groups to sync (server=%s)",
        len(groups),
        zabbixserver_id,
    )

    for obj in groups:
        try:
            safe_sync(ProxyGroupSync, obj)
        except Exception as exc:
            logger.exception(
                "sync_proxy_groups: failed for pk=%s: %s",
                getattr(obj, "pk", None),
                exc,
            )


def sync_proxies(zabbixserver_id: int) -> None:
    server = ZabbixServer.objects.get(pk=zabbixserver_id)
    proxies = list(ZabbixProxy.objects.filter(zabbixserver=server))

    logger.info(
        "sync_proxies: %s proxies to sync (server=%s)",
        len(proxies),
        zabbixserver_id,
    )

    for obj in proxies:
        try:
            safe_sync(ProxySync, obj)
        except Exception as exc:
            logger.exception(
                "sync_proxies: failed for pk=%s: %s",
                getattr(obj, "pk", None),
                exc,
            )


# ---------------------------------------------------------------------------
# Per-host джоба для нового Sync All
# ---------------------------------------------------------------------------

def synchost_assignment(assignment_id: int) -> None:
    """
    Per-host:

      1) HostSync без шаблонов (skip_templates=True).
      2) HostInterfaceSync для всех интерфейсов.
      3) HostSync с шаблонами (skip_templates=False).
    """
    assignment = ZabbixServerAssignment.objects.select_related(
        "assigned_object_type", "zabbixserver"
    ).get(pk=assignment_id)

    obj = assignment.assigned_object
    if obj is None:
        logger.warning("synchost_assignment: assignment %s has no assigned_object", assignment_id)
        return

    all_objects = get_assigned_zabbixobjects(obj)

    # 1. Host без шаблонов
    safe_sync(
        HostSync,
        assignment,
        extra_args={
            "all_objects": all_objects,
            "skip_templates": True,
        },
    )

    assignment.refresh_from_db()

    # 2. Интерфейсы
    hostid = assignment.hostid
    if hostid:
        for hostinterface in all_objects.get("hostinterfaces", []):
            try:
                safe_sync(
                    HostInterfaceSync,
                    hostinterface,
                    extra_args={"hostid": hostid},
                )
            except Exception as exc:
                logger.exception(
                    "synchost_assignment: HostInterfaceSync failed "
                    "(assignment=%s model=%s pk=%s): %s",
                    assignment_id,
                    hostinterface.__class__.__name__,
                    getattr(hostinterface, "pk", None),
                    exc,
                )

    # 3. Host с шаблонами
    safe_sync(
        HostSync,
        assignment,
        extra_args={
            "all_objects": all_objects,
            "skip_templates": False,
        },
    )

    logger.info(
        "synchost_assignment finished: assignment=%s, hostid=%s",
        assignment_id,
        assignment.hostid,
    )
