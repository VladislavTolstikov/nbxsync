from django.contrib.contenttypes.models import ContentType

from nbxsync.models import ZabbixServerAssignment
from nbxsync.utils.sync import HostSync
from nbxsync.utils.sync.safe_delete import safe_delete

__all__ = ('DeleteHostJob',)


class DeleteHostJob:
    def __init__(self, **kwargs):
        self.instance = kwargs.get('instance')  # Device or VirtualMachine

    def run(self):
        object_ct = ContentType.objects.get_for_model(self.instance)
        zabbixserver_assignments = ZabbixServerAssignment.objects.filter(
            assigned_object_type=object_ct,
            assigned_object_id=self.instance.pk,
        )

        errors = []
        for assignment in zabbixserver_assignments:
            try:
                self.delete_host(assignment)
            except Exception as error:
                errors.append(
                    f'assignment={assignment.pk} '
                    f'server={assignment.zabbixserver_id}: {error}'
                )

        if errors:
            raise RuntimeError(
                'Host deletion completed with errors on one or more Zabbix '
                'servers: ' + ' | '.join(errors)
            )

    def delete_host(self, assignment):
        safe_delete(HostSync, assignment)
