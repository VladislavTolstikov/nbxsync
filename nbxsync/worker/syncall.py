import logging
from typing import Iterable

from django_rq import get_queue
from rq import get_current_job

from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.models import ZabbixServerAssignment
from nbxsync.services.reconcile import desired_host_status, prepare_server_assignments

logger = logging.getLogger(__name__)

LOW_QUEUE_NAME = 'low'
HOST_OBJECT_MODELS = {'device', 'virtualmachine', 'virtualdevicecontext'}


def _iter_filtered_assignments(zabbixserver) -> Iterable[ZabbixServerAssignment]:
    """Yield every host assignment that should trigger object sync.

    The selected assignment belongs to the Sync All server, but the downstream
    object job synchronizes the NetBox object to all of its Zabbix assignments.
    Deletion candidates are yielded even without an IP. Active/staged/planned
    hosts still require an address before a normal host sync is attempted.
    """
    qs = ZabbixServerAssignment.objects.filter(zabbixserver=zabbixserver).select_related(
        'assigned_object_type'
    )
    for assignment in qs.iterator():
        obj = assignment.assigned_object
        if obj is None:
            continue
        if obj._meta.model_name not in HOST_OBJECT_MODELS:
            continue

        desired = desired_host_status(obj)
        if desired == ZabbixHostStatus.DELETED:
            yield assignment
            continue

        has_ip4 = getattr(obj, 'primary_ip4', None) is not None
        has_ip = getattr(obj, 'primary_ip', None) is not None
        if not (has_ip or has_ip4):
            logger.warning(
                'SyncAll skipped assignment=%s object=%s: no primary IP',
                assignment.pk,
                obj,
            )
            continue

        yield assignment


def syncall(zabbixserver) -> None:
    """Prepare one server, then sync selected objects to all assigned servers."""
    job = get_current_job()
    queue = get_queue(LOW_QUEUE_NAME)
    server_id = zabbixserver.pk

    logger.info('SyncAll dispatcher started for ZabbixServer id=%s', server_id)

    prepared = prepare_server_assignments(zabbixserver)
    logger.info(
        'SyncAll prepared %s auto-managed devices for server=%s',
        prepared,
        server_id,
    )

    j1 = queue.enqueue(
        'nbxsync.worker.ensure_hostgroup_assignments',
        args=(server_id,),
        timeout=9000,
        description=f'Ensure hostgroup assignments (server={server_id})',
    )

    j2 = queue.enqueue(
        'nbxsync.worker.sync_hostgroups_to_zabbix',
        args=(server_id,),
        timeout=9000,
        depends_on=j1,
        description=f'Sync hostgroups to Zabbix (server={server_id})',
    )

    j3 = queue.enqueue(
        'nbxsync.worker.sync_proxy_groups',
        args=(server_id,),
        timeout=9000,
        depends_on=j2,
        description=f'Sync proxy groups (server={server_id})',
    )

    j4 = queue.enqueue(
        'nbxsync.worker.sync_proxies',
        args=(server_id,),
        timeout=9000,
        depends_on=j3,
        description=f'Sync proxies (server={server_id})',
    )

    assignments = list(_iter_filtered_assignments(zabbixserver))
    logger.info(
        'SyncAll dispatcher: %s object triggers selected (server=%s)',
        len(assignments),
        server_id,
    )

    for assignment in assignments:
        device_name = str(assignment.assigned_object)
        queue.enqueue(
            'nbxsync.worker.synchost_assignment',
            args=(assignment.pk,),
            timeout=9000,
            job_id=f'synchost_{server_id}_{assignment.pk}',
            depends_on=j4,
            description=f'Sync host {device_name} to all assigned servers',
        )

    # Reconciliation only handles tagged Zabbix hosts that the ordinary object
    # sync cannot safely target, so it only needs the server-global preparation.
    queue.enqueue(
        'nbxsync.services.reconcile.reconcile_managed_hosts',
        args=(server_id,),
        timeout=9000,
        depends_on=j4,
        description=f'Reconcile NbxSync managed hosts (server={server_id})',
    )

    if job:
        job.meta['progress'] = 100
        job.save_meta()

    logger.info(
        'SyncAll dispatcher finished for ZabbixServer id=%s: '
        '%s object jobs and reconciliation enqueued',
        server_id,
        len(assignments),
    )
