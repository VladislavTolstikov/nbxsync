import logging
from typing import Callable, Iterable, Optional

from rq import get_current_job

from nbxsync.jobs.synctemplates import SyncTemplatesJob
from nbxsync.models import (
    ZabbixHostgroupAssignment,
    ZabbixMaintenance,
    ZabbixProxy,
    ZabbixProxyGroup,
    ZabbixServerAssignment,
)
from nbxsync.utils import get_assigned_zabbixobjects
from nbxsync.utils.sync import HostGroupSync, HostInterfaceSync, HostSync, MaintenanceSync, ProxyGroupSync, ProxySync
from nbxsync.utils.sync.safe_sync import safe_sync

logger = logging.getLogger(__name__)


def _update_progress(job, completed: int, total: int) -> None:
    if not job or total <= 0:
        return

    job.meta["progress"] = min(100, int((completed / total) * 100))
    job.save_meta()


def _bulk_safe_sync(
    sync_class,
    objects: Iterable,
    job,
    completed: int,
    total: int,
    extra_args: Optional[Callable] = None,
) -> int:
    for obj in objects:
        try:
            kwargs = extra_args(obj) if callable(extra_args) else None
            safe_sync(sync_class, obj, extra_args=kwargs or {})
        except Exception:
            logger.exception("Failed to sync %s for %s", sync_class.__name__, obj)
        completed += 1
        _update_progress(job, completed, total)

    return completed


def syncall(zabbixserver):
    job = get_current_job()
    if job:
        job.meta["progress"] = 0
        job.save_meta()

    proxy_groups = list(ZabbixProxyGroup.objects.filter(zabbixserver=zabbixserver))
    proxies = list(ZabbixProxy.objects.filter(zabbixserver=zabbixserver))
    maintenances = list(ZabbixMaintenance.objects.filter(zabbixserver=zabbixserver))
    hostgroup_assignments = list(
        ZabbixHostgroupAssignment.objects.filter(zabbixhostgroup__zabbixserver=zabbixserver)
    )
    assignments = list(ZabbixServerAssignment.objects.filter(zabbixserver=zabbixserver))

    total_steps = (
        len(proxy_groups)
        + len(proxies)
        + len(maintenances)
        + len(hostgroup_assignments)
        + len(assignments)
        + 1
    ) or 1

    completed = 0

    completed = _bulk_safe_sync(ProxyGroupSync, proxy_groups, job, completed, total_steps)
    completed = _bulk_safe_sync(ProxySync, proxies, job, completed, total_steps)
    completed = _bulk_safe_sync(MaintenanceSync, maintenances, job, completed, total_steps)
    completed = _bulk_safe_sync(HostGroupSync, hostgroup_assignments, job, completed, total_steps)

    for assignment in assignments:
        try:
            all_objects = get_assigned_zabbixobjects(assignment.assigned_object)

            for hostgroup in all_objects.get("hostgroups", []):
                safe_sync(HostGroupSync, hostgroup)

            if assignment.zabbixproxy and assignment.zabbixproxy.proxygroup:
                safe_sync(ProxyGroupSync, assignment.zabbixproxy.proxygroup)
            if assignment.zabbixproxy:
                safe_sync(ProxySync, assignment.zabbixproxy)

            if assignment.zabbixproxygroup:
                safe_sync(ProxyGroupSync, assignment.zabbixproxygroup)

            safe_sync(HostSync, assignment, extra_args={"all_objects": all_objects, "skip_templates": True})

            for hostinterface in all_objects.get("hostinterfaces", []):
                safe_sync(HostInterfaceSync, hostinterface, extra_args={"hostid": assignment.hostid})

            safe_sync(HostSync, assignment, extra_args={"all_objects": all_objects})
        except Exception:
            logger.exception("Failed to sync host assignment %s", assignment)

        completed += 1
        _update_progress(job, completed, total_steps)

    try:
        SyncTemplatesJob(instance=zabbixserver).run()
    except Exception:
        logger.exception("Failed to sync templates for %s", zabbixserver)
    finally:
        completed = total_steps
        _update_progress(job, completed, total_steps)

    _update_progress(job, total_steps, total_steps)
