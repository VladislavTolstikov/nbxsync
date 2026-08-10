from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.services.host_policy import desired_host_status


def install(HostSync):
    """Enforce active/staged/planned Device state in HostSync parameters.

    DELETED lifecycle objects are intercepted by SyncHostJob/reconciliation before
    HostSync create/update is called. We deliberately leave that branch to the
    original method so legacy low-level HostSync tests/helpers are not repurposed
    as lifecycle dispatchers.
    """
    if getattr(HostSync, '_host_policy_guard_installed', False):
        return

    original_get_create_params = HostSync.get_create_params

    def guarded_get_create_params(self):
        params = original_get_create_params(self)
        desired = desired_host_status(self.obj.assigned_object)
        if desired == ZabbixHostStatus.DISABLED:
            params['status'] = 1
        elif desired == ZabbixHostStatus.ENABLED:
            params['status'] = 0
        return params

    HostSync.get_create_params = guarded_get_create_params
    HostSync._host_policy_guard_installed = True
