import logging
from collections import defaultdict
from typing import Iterable, List, Tuple

from rq import get_current_job
from zabbix_utils.exceptions import APIRequestError

from nbxsync.jobs.synctemplates import SyncTemplatesJob
from nbxsync.models import (
    ZabbixHostgroupAssignment,
    ZabbixHostInterface,
    ZabbixProxy,
    ZabbixProxyGroup,
    ZabbixServerAssignment,
)
from nbxsync.utils import get_assigned_zabbixobjects
from nbxsync.utils.sync import HostGroupSync, HostInterfaceSync, HostSync, ProxyGroupSync, ProxySync
from nbxsync.utils.sync.safe_sync import safe_sync

logger = logging.getLogger(__name__)


StageData = List[Tuple[ZabbixServerAssignment, dict]]


def _log_stage(stage: str, count: int, action: str) -> None:
    logger.info("Sync All stage '%s' %s (%s objects)", stage, action, count)


def _sync_queryset(sync_class, objects: Iterable, *, stage: str) -> None:
    items = list(objects)
    _log_stage(stage, len(items), "starting")
    for obj in items:
        try:
            safe_sync(sync_class, obj)
        except Exception:
            logger.exception("Failed to sync %s for %s", sync_class.__name__, obj)
    _log_stage(stage, len(items), "completed")


def _sync_host_groups(assignments: StageData) -> None:
    unique_ids = set()
    hostgroups = []
    for _, all_objects in assignments:
        for hostgroup in all_objects.get("hostgroups", []):
            key = getattr(hostgroup, "pk", None)
            if key in unique_ids:
                continue
            unique_ids.add(key)
            hostgroups.append(hostgroup)

    _sync_queryset(HostGroupSync, hostgroups, stage="Host groups")


def _sync_host_interfaces(assignments: StageData) -> None:
    interfaces: List[Tuple[ZabbixHostInterface, int]] = []
    for assignment, all_objects in assignments:
        hostid = assignment.hostid
        for hostinterface in all_objects.get("hostinterfaces", []):
            interfaces.append((hostinterface, hostid))

    _log_stage("Host interfaces", len(interfaces), "starting")
    for hostinterface, hostid in interfaces:
        try:
            safe_sync(HostInterfaceSync, hostinterface, extra_args={"hostid": hostid})
        except Exception:
            logger.exception("Failed to sync HostInterface for %s", hostinterface)
    _log_stage("Host interfaces", len(interfaces), "completed")


def _sync_hosts(assignments: StageData, *, include_templates: bool) -> None:
    stage = "Servers" if not include_templates else "Templates"
    _log_stage(stage, len(assignments), "starting")
    for assignment, all_objects in assignments:
        try:
            safe_sync(
                HostSync,
                assignment,
                extra_args={"all_objects": all_objects, "skip_templates": not include_templates},
            )
        except APIRequestError:
            # safe_sync already handles duplicate detection; log and continue for other errors
            logger.exception("Zabbix API error while syncing host %s", assignment)
        except Exception:
            logger.exception("Failed to sync host %s", assignment)
    _log_stage(stage, len(assignments), "completed")


def _collect_assignment_data(zabbixserver) -> StageData:
    data: StageData = []
    assignments = ZabbixServerAssignment.objects.filter(zabbixserver=zabbixserver).select_related("assigned_object")
    for assignment in assignments:
        data.append((assignment, get_assigned_zabbixobjects(assignment.assigned_object)))
    return data


def _log_assignment_counts(assignments: StageData) -> None:
    counters = defaultdict(int)
    for _, all_objects in assignments:
        counters["macros"] += len(all_objects.get("macros", []))
        counters["tags"] += len(all_objects.get("tags", []))
        counters["inventory"] += 1 if all_objects.get("hostinventory") else 0

    for name, count in (
        ("Macros", counters["macros"]),
        ("Tags", counters["tags"]),
        ("Inventory", counters["inventory"]),
    ):
        _log_stage(name, count, "completed")


def syncall(zabbixserver):
    job = get_current_job()
    if job:
        job.meta["progress"] = 0
        job.save_meta()

    assignments = _collect_assignment_data(zabbixserver)

    _sync_hosts(assignments, include_templates=False)
    _sync_queryset(
        ProxyGroupSync,
        ZabbixProxyGroup.objects.filter(zabbixserver=zabbixserver),
        stage="Proxy groups",
    )
    _sync_queryset(ProxySync, ZabbixProxy.objects.filter(zabbixserver=zabbixserver), stage="Proxies")
    _sync_host_groups(assignments)
    _sync_host_interfaces(assignments)
    _sync_hosts(assignments, include_templates=True)
    _log_assignment_counts(assignments)

    try:
        SyncTemplatesJob(instance=zabbixserver).run()
    except Exception:
        logger.exception("Failed to sync templates for %s", zabbixserver)

    if job:
        job.meta["progress"] = 100
        job.save_meta()
