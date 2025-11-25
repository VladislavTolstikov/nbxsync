import logging
from collections import defaultdict
from typing import Iterable, List, Tuple

from rq import get_current_job
from zabbix_utils.exceptions import APIRequestError

from nbxsync.jobs.synctemplates import SyncTemplatesJob
from nbxsync.models import (
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


class _ProgressTracker:
    def __init__(self, job, total: int) -> None:
        self.job = job
        self.total = total
        self.completed = 0

    def increment(self) -> None:
        if self.total <= 0:
            return

        self.completed += 1
        if self.job:
            self.job.meta["progress"] = min(100, int((self.completed / self.total) * 100))
            self.job.save_meta()


def _log_stage(stage: str, count: int, action: str) -> None:
    logger.info("Sync All stage '%s' %s (%s objects)", stage, action, count)


def _bulk_safe_sync(
    sync_class,
    objects: Iterable,
    *,
    stage: str,
    progress: _ProgressTracker | None,
    extra_args=None,
) -> None:
    items = list(objects)
    _log_stage(stage, len(items), "starting")
    for obj in items:
        try:
            kwargs = extra_args(obj) if callable(extra_args) else (extra_args or {})
            safe_sync(sync_class, obj, extra_args=kwargs)
        except Exception as exc:
            # Не дергаем __str__ у obj, чтобы не ловить "another command is already in progress"
            logger.exception(
                "Failed to sync %s (model=%s pk=%s): %s",
                sync_class.__name__,
                obj.__class__.__name__ if obj is not None else "None",
                getattr(obj, "pk", None),
                exc,
            )
        else:
            if progress:
                progress.increment()
    _log_stage(stage, len(items), "completed")


def _collect_host_groups(assignments: StageData) -> list:
    unique_ids = set()
    hostgroups = []
    for _, all_objects in assignments:
        for hostgroup in all_objects.get("hostgroups", []):
            key = getattr(hostgroup, "pk", None)
            if key in unique_ids:
                continue
            unique_ids.add(key)
            hostgroups.append(hostgroup)
    return hostgroups


def _collect_host_interfaces(assignments: StageData) -> List[Tuple[ZabbixHostInterface, int]]:
    interfaces: List[Tuple[ZabbixHostInterface, int]] = []
    for assignment, all_objects in assignments:
        hostid = assignment.hostid
        for hostinterface in all_objects.get("hostinterfaces", []):
            interfaces.append((hostinterface, hostid))
    return interfaces


def _sync_host_groups(hostgroups: Iterable, *, progress: _ProgressTracker | None) -> None:
    _bulk_safe_sync(HostGroupSync, hostgroups, stage="Host groups", progress=progress)


def _sync_host_interfaces(
    interfaces: List[Tuple[ZabbixHostInterface, int]],
    *,
    progress: _ProgressTracker | None,
) -> None:
    _log_stage("Host interfaces", len(interfaces), "starting")
    for hostinterface, hostid in interfaces:
        try:
            safe_sync(HostInterfaceSync, hostinterface, extra_args={"hostid": hostid})
        except Exception as exc:
            logger.exception(
                "Failed to sync HostInterface (model=%s pk=%s): %s",
                hostinterface.__class__.__name__,
                getattr(hostinterface, "pk", None),
                exc,
            )
        else:
            if progress:
                progress.increment()
    _log_stage("Host interfaces", len(interfaces), "completed")


def _sync_hosts(
    assignments: StageData,
    *,
    include_templates: bool,
    progress: _ProgressTracker | None,
) -> None:
    stage = "Servers" if not include_templates else "Templates"
    _log_stage(stage, len(assignments), "starting")
    for assignment, all_objects in assignments:
        try:
            safe_sync(
                HostSync,
                assignment,
                extra_args={"all_objects": all_objects, "skip_templates": not include_templates},
            )
        except APIRequestError as exc:
            logger.exception(
                "Zabbix API error while syncing host (model=%s pk=%s): %s",
                assignment.__class__.__name__,
                getattr(assignment, "pk", None),
                exc,
            )
        except Exception as exc:
            logger.exception(
                "Failed to sync host (model=%s pk=%s): %s",
                assignment.__class__.__name__,
                getattr(assignment, "pk", None),
                exc,
            )
        else:
            if progress:
                progress.increment()
    _log_stage(stage, len(assignments), "completed")


def _collect_assignment_data(zabbixserver) -> StageData:
    data: StageData = []
    assignments = (
        ZabbixServerAssignment.objects.filter(zabbixserver=zabbixserver)
        .select_related("assigned_object")
    )
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
    assignments = _collect_assignment_data(zabbixserver)

    proxy_groups = list(ZabbixProxyGroup.objects.filter(zabbixserver=zabbixserver))
    proxies = list(ZabbixProxy.objects.filter(zabbixserver=zabbixserver))
    hostgroups = _collect_host_groups(assignments)
    hostinterfaces = _collect_host_interfaces(assignments)

    total_objects = (
        len(assignments) * 2
        + len(proxy_groups)
        + len(proxies)
        + len(hostgroups)
        + len(hostinterfaces)
    )
    progress = _ProgressTracker(job, total_objects)

    # 1. Сервера без шаблонов
    _sync_hosts(assignments, include_templates=False, progress=progress)
    # 2. Прокси-группы и прокси
    _bulk_safe_sync(ProxyGroupSync, proxy_groups, stage="Proxy groups", progress=progress)
    _bulk_safe_sync(ProxySync, proxies, stage="Proxies", progress=progress)
    # 3. Группы
    _sync_host_groups(hostgroups, progress=progress)
    # 4. Интерфейсы
    _sync_host_interfaces(hostinterfaces, progress=progress)
    # 5. Хосты с шаблонами/макросами/тэгами/инвентарём
    _sync_hosts(assignments, include_templates=True, progress=progress)

    _log_assignment_counts(assignments)

    try:
        SyncTemplatesJob(instance=zabbixserver).run()
    except Exception:
        logger.exception("Failed to sync templates for %s", zabbixserver)

    if job:
        job.meta["progress"] = 100
        job.save_meta()
