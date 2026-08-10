from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError

from nbxsync.models import (
    ZabbixHostInterface,
    ZabbixMaintenance,
    ZabbixMaintenanceObjectAssignment,
)


def install(HostSync):
    """Restore HostSync.delete for the customized HostSync implementation.

    The forked HostSync currently contains a placeholder delete() method. Keep
    the runtime patch isolated here until the customized class is cleaned up.
    """
    if getattr(HostSync, '_host_delete_installed', False):
        return

    def clear_local_ids(self):
        try:
            self.obj.hostid = None
            self.obj.save()
        except ValidationError:
            pass

        ZabbixHostInterface.objects.filter(
            assigned_object_type=self.obj.assigned_object_type,
            assigned_object_id=self.obj.assigned_object_id,
            zabbixserver=self.obj.zabbixserver,
        ).update(interfaceid=None)

    def delete(self):
        if not self.obj.hostid:
            try:
                self.obj.update_sync_info(
                    success=False,
                    message='Host already deleted or missing host ID.',
                )
            except Exception:
                pass
            return

        assigned_object = self.obj.assigned_object
        hostid = self.obj.hostid

        if assigned_object is None:
            try:
                self.api_object().delete([hostid])
                clear_local_ids(self)
            except Exception as error:
                raise RuntimeError(
                    f'Failed to delete orphaned host {hostid} from Zabbix: {error}'
                ) from error
            return

        try:
            object_ct = ContentType.objects.get_for_model(assigned_object)
            maintenances = self.api.maintenance.get(
                hostids=[hostid],
                selectHosts='extend',
            )

            for maintenance in maintenances:
                hosts = maintenance.get('hosts', [])
                maintenance_id = maintenance['maintenanceid']

                if len(hosts) > 1:
                    remaining_hosts = [
                        {'hostid': host['hostid']}
                        for host in hosts
                        if str(host['hostid']) != str(hostid)
                    ]
                    self.api.maintenance.update(
                        maintenanceid=maintenance_id,
                        hosts=remaining_hosts,
                    )
                    ZabbixMaintenanceObjectAssignment.objects.filter(
                        zabbixmaintenance__maintenanceid=maintenance_id,
                        zabbixmaintenance__zabbixserver=self.obj.zabbixserver,
                        assigned_object_type=object_ct,
                        assigned_object_id=assigned_object.id,
                    ).delete()
                else:
                    self.api.maintenance.delete([maintenance_id])
                    ZabbixMaintenance.objects.filter(
                        maintenanceid=maintenance_id,
                        zabbixserver=self.obj.zabbixserver,
                    ).delete()

            self.api_object().delete([hostid])
            clear_local_ids(self)

            try:
                self.obj.update_sync_info(
                    success=True,
                    message='Host deleted from Zabbix.',
                )
            except Exception:
                pass

        except Exception as error:
            try:
                self.obj.update_sync_info(
                    success=False,
                    message=f'Failed to delete host: {error}',
                )
            except Exception:
                pass
            raise RuntimeError(
                f'Failed to delete host {hostid} from Zabbix: {error}'
            ) from error

    HostSync.delete = delete
    HostSync._host_delete_installed = True
