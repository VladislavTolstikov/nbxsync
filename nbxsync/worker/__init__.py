import logging

from django_rq import job

from nbxsync.jobs import (
    SyncHostJob,
    DeleteHostJob,
    SyncProxyGroupJob,
    SyncProxyJob,
    SyncTemplatesJob,
    SyncMaintenceJob,
    DeleteMaintenanceJob,
)
from nbxsync.worker.syncall import syncall
from nbxsync.worker.global_sync import (
    sync_templates_from_zabbix,
    ensure_hostgroup_assignments,
    sync_hostgroups_to_zabbix,
    sync_proxy_groups,
    sync_proxies,
    synchost_assignment,
)

logger = logging.getLogger("worker")


@job("low")
def synchost(instance):
    worker = SyncHostJob(instance=instance)
    worker.run()


@job("low")
def deletehost(instance):
    worker = DeleteHostJob(instance=instance)
    worker.run()


@job("low")
def syncproxygroup(instance):
    worker = SyncProxyGroupJob(instance=instance)
    worker.run()


@job("low")
def syncproxy(instance):
    worker = SyncProxyJob(instance=instance)
    worker.run()


@job("low")
def synctemplates(instance):
    worker = SyncTemplatesJob(instance=instance)
    worker.run()


@job("low")
def syncmaintenance(instance):
    worker = SyncMaintenceJob(instance=instance)
    worker.run()


@job("low")
def deletemaintenance(instance):
    worker = DeleteMaintenanceJob(instance=instance)
    worker.run()
