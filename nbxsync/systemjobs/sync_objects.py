from django_rq import get_queue

from netbox.jobs import JobRunner, system_job

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.services.reconcile import prepare_server_assignments
from nbxsync.settings import get_plugin_settings


HOST_OBJECT_MODELS = {'device', 'virtualmachine', 'virtualdevicecontext'}


def GetSyncInterval():
    pluginsettings = get_plugin_settings()
    return pluginsettings.backgroundsync.objects.interval


@system_job(interval=GetSyncInterval())
class SyncObjectsJob(JobRunner):
    class Meta:
        name = 'Zabbix Sync Hosts job'

    def run(self, *args, **kwargs):
        queue = get_queue('low')
        servers = list(ZabbixServer.objects.all())

        for server in servers:
            prepare_server_assignments(server)

        synced_objects = set()

        for assignment in ZabbixServerAssignment.objects.all():
            instance = assignment.assigned_object
            if instance is None:
                continue
            if instance._meta.model_name not in HOST_OBJECT_MODELS:
                continue

            identity = (
                assignment.assigned_object_type_id,
                assignment.assigned_object_id,
            )
            if identity in synced_objects:
                continue
            synced_objects.add(identity)

            queue.enqueue_job(
                queue.create_job(
                    func='nbxsync.worker.synchost',
                    args=[instance],
                    timeout=9000,
                )
            )

        # Reconciliation only touches tagged Zabbix hosts that have no local
        # ZabbixServerAssignment, so it is independent from the host jobs above.
        for server in servers:
            queue.enqueue(
                'nbxsync.services.reconcile.reconcile_managed_hosts',
                args=(server.pk,),
                timeout=9000,
                description=f'Reconcile NbxSync managed hosts (server={server.pk})',
            )
