from django_rq import get_queue

from netbox.jobs import JobRunner, system_job

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.services.reconcile import prepare_server_assignments
from nbxsync.settings import get_plugin_settings


def GetSyncInterval():
    pluginsettings = get_plugin_settings()
    return pluginsettings.backgroundsync.objects.interval


@system_job(interval=GetSyncInterval())
class SyncObjectsJob(JobRunner):
    class Meta:
        name = 'Zabbix Sync Hosts job'

    def run(self, *args, **kwargs):
        queue = get_queue('low')

        for server in ZabbixServer.objects.all():
            prepare_server_assignments(server)

        synced_objects = set()
        host_jobs = []

        for assignment in ZabbixServerAssignment.objects.all():
            instance = assignment.assigned_object
            if instance is None:
                continue

            identity = (
                assignment.assigned_object_type_id,
                assignment.assigned_object_id,
            )
            if identity in synced_objects:
                continue
            synced_objects.add(identity)

            host_jobs.append(
                queue.enqueue_job(
                    queue.create_job(
                        func='nbxsync.worker.synchost',
                        args=[instance],
                        timeout=9000,
                    )
                )
            )

        for server in ZabbixServer.objects.all():
            queue.enqueue(
                'nbxsync.services.reconcile.reconcile_managed_hosts',
                args=(server.pk,),
                timeout=9000,
                depends_on=host_jobs or None,
                description=f'Reconcile NbxSync managed hosts (server={server.pk})',
            )
