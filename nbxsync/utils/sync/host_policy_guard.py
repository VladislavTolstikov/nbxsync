from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.services.host_policy import desired_host_status


def install(HostSync):
    """Enforce the shared lifecycle policy in HostSync create/update params."""
    if getattr(HostSync, '_host_policy_guard_installed', False):
        return

    original_get_create_params = HostSync.get_create_params

    def guarded_get_create_params(self):
        desired = desired_host_status(self.obj.assigned_object)
        if desired == ZabbixHostStatus.DELETED:
            raise RuntimeError(
                f'Refused to create/update deleted-status host '
                f'{self.obj.assigned_object}'
            )

        params = original_get_create_params(self)
        if desired == ZabbixHostStatus.DISABLED:
            params['status'] = 1
        else:
            params['status'] = 0
        return params

    HostSync.get_create_params = guarded_get_create_params
    HostSync._host_policy_guard_installed = True
