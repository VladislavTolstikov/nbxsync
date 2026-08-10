from .proxygroupsync import ProxyGroupSync
from .proxysync import ProxySync
from .hostsync import HostSync
from .hostgroupsync import HostGroupSync
from .hostinterfacesync import HostInterfaceSync
from .maintenancesync import MaintenanceSync
from .run_zabbix_operations import run_zabbix_operation
from .host_delete import install as install_host_delete
from .identity_guard import install as install_identity_guard


install_host_delete(HostSync)
install_identity_guard(HostSync, HostInterfaceSync)
