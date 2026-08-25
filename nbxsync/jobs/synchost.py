from django.contrib.contenttypes.models import ContentType

from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.models import ZabbixServerAssignment
from nbxsync.services.host_policy import desired_host_status
from nbxsync.utils import get_assigned_zabbixobjects
from nbxsync.utils.sync import HostGroupSync, HostInterfaceSync, HostSync, ProxyGroupSync, ProxySync, run_zabbix_operation
from nbxsync.utils.sync.safe_delete import safe_delete
from nbxsync.utils.sync.safe_sync import safe_sync

__all__ = ('SyncHostJob',)


class SyncHostJob:
    def __init__(self, **kwargs):
        self.instance = kwargs.get('instance')

    def run(self):
        object_ct = ContentType.objects.get_for_model(self.instance)
        zabbixserver_assignments = ZabbixServerAssignment.objects.filter(
            assigned_object_type=object_ct,
            assigned_object_id=self.instance.pk,
        )

        zabbix_status = desired_host_status(self.instance)
        errors = []

        # One NetBox object may be assigned to several Zabbix servers. A failure
        # on one server must not prevent the remaining servers from being tried.
        for assignment in zabbixserver_assignments:
            try:
                if zabbix_status == ZabbixHostStatus.DELETED:
                    self.delete_host(assignment)
                else:
                    all_objects = self.sync_host(assignment)
                    self.verify_hostinterfaces(assignment, all_objects)
            except Exception as error:
                errors.append(
                    f'assignment={assignment.pk} '
                    f'server={assignment.zabbixserver_id}: {error}'
                )

        if errors:
            raise RuntimeError(
                'Host synchronization completed with errors on one or more '
                'Zabbix servers: ' + ' | '.join(errors)
            )

    def delete_host(self, assignment):
        safe_delete(HostSync, assignment)

    def verify_hostinterfaces(self, assignment, all_objects):
        run_zabbix_operation(
            HostSync,
            assignment,
            'verify_hostinterfaces',
            extra_args={'all_objects': all_objects},
        )

    def _filter_objects_for_server(self, all_objects, server_id):
        filtered = dict(all_objects)

        filtered['hostinterfaces'] = [
            obj for obj in all_objects.get('hostinterfaces', [])
            if obj.zabbixserver_id == server_id
        ]
        filtered['hostgroups'] = [
            obj for obj in all_objects.get('hostgroups', [])
            if obj.zabbixhostgroup.zabbixserver_id == server_id
        ]
        filtered['templates'] = [
            obj for obj in all_objects.get('templates', [])
            if obj.zabbixtemplate.zabbixserver_id == server_id
        ]

        return filtered

    def sync_host(self, assignment):
        try:
            all_objects = get_assigned_zabbixobjects(self.instance)
            all_objects = self._filter_objects_for_server(
                all_objects,
                assignment.zabbixserver_id,
            )
            assignment.assigned_objects = all_objects

            for hostgroup in all_objects['hostgroups']:
                safe_sync(HostGroupSync, hostgroup)

            if assignment.zabbixproxy:
                if assignment.zabbixproxy.proxygroup:
                    safe_sync(ProxyGroupSync, assignment.zabbixproxy.proxygroup)
                safe_sync(ProxySync, assignment.zabbixproxy)

            if assignment.zabbixproxygroup:
                safe_sync(ProxyGroupSync, assignment.zabbixproxygroup)

            safe_sync(
                HostSync,
                assignment,
                extra_args={'all_objects': all_objects, 'skip_templates': True},
            )

            for hostinterface in all_objects['hostinterfaces']:
                safe_sync(
                    HostInterfaceSync,
                    hostinterface,
                    extra_args={'hostid': assignment.hostid},
                )

            safe_sync(
                HostSync,
                assignment,
                extra_args={'all_objects': all_objects},
            )

            return all_objects

        except Exception as e:
            raise RuntimeError(f'Unexpected error: {e}')
