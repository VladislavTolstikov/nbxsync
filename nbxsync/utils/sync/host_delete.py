from django.core.exceptions import ValidationError

from nbxsync.models import (
    ZabbixHostInterface,
    ZabbixMaintenance,
    ZabbixMaintenanceObjectAssignment,
)


OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'


def install(HostSync):
    """Restore HostSync.delete and make destructive deletion identity-safe."""
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

    def owner_tag_values(host):
        type_values = []
        id_values = []
        for tag in host.get('tags', []):
            tag_name = str(tag.get('tag', ''))
            value = str(tag.get('value', ''))
            if tag_name == OWNER_TYPE_TAG:
                type_values.append(value)
            elif tag_name == OWNER_ID_TAG:
                id_values.append(value)
        return type_values, id_values

    def verify_owned_host(self, hostid):
        found = self.api_object().get(
            hostids=[hostid],
            output=['hostid', 'host', 'name'],
            selectTags=['tag', 'value'],
        )
        if not found:
            clear_local_ids(self)
            try:
                self.obj.update_sync_info(
                    success=True,
                    message=f'Host {hostid} is already absent from Zabbix.',
                )
            except Exception:
                pass
            return None

        host = found[0]
        type_values, id_values = owner_tag_values(host)
        expected_type = str(self.obj.assigned_object_type_id)
        expected_id = str(self.obj.assigned_object_id)

        if (
            len(type_values) == 1
            and len(id_values) == 1
            and type_values[0] == expected_type
            and id_values[0] == expected_id
        ):
            return host

        if type_values or id_values:
            # The stored hostid points to a host carrying NbxSync identity for a
            # different or ambiguous owner. The local IDs are stale and must not
            # be used for any destructive operation.
            clear_local_ids(self)
            raise RuntimeError(
                f'Refused to delete hostid {hostid}: NbxSync ownership tags '
                f'type={type_values!r} id={id_values!r}, expected '
                f'type={expected_type!r} id={expected_id!r}'
            )

        # Untagged hosts may be legacy or manually created. Never make a
        # destructive ownership guess from name/IP alone.
        raise RuntimeError(
            f'Refused to delete hostid {hostid}: host has no NbxSync ownership tags'
        )

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

        hostid = self.obj.hostid

        try:
            owned_host = verify_owned_host(self, hostid)
            if owned_host is None:
                return

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
                        assigned_object_type_id=self.obj.assigned_object_type_id,
                        assigned_object_id=self.obj.assigned_object_id,
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
