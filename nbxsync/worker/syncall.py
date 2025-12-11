import logging
from typing import Iterable

from rq import get_current_job
from django_rq import get_queue

from nbxsync.models import ZabbixServerAssignment

logger = logging.getLogger(__name__)

LOW_QUEUE_NAME = "low"


def _iter_filtered_assignments(zabbixserver) -> Iterable[ZabbixServerAssignment]:
    """
    Берём assignment'ы:
      - привязаны к этому zabbixserver;
      - assigned_object.status in {active, staged};
      - есть primary_ip или primary_ip4.
    """
    qs = ZabbixServerAssignment.objects.filter(zabbixserver=zabbixserver).select_related(
        "assigned_object_type"
    )
    for a in qs.iterator():
        obj = a.assigned_object
        if obj is None:
            continue

        status = getattr(obj, "status", None)
        status_slug = getattr(status, "slug", None) or str(status).lower()
        if status_slug not in {"active", "staged"}:
            continue

        has_ip4 = getattr(obj, "primary_ip4", None) is not None
        has_ip = getattr(obj, "primary_ip", None) is not None
        if not (has_ip or has_ip4):
            continue

        yield a


def syncall(zabbixserver) -> None:
    """
    Новый Sync All:

      J1: sync_templates_from_zabbix
      J2: ensure_hostgroup_assignments
      J3: sync_hostgroups_to_zabbix
      J4: sync_proxy_groups
      J5: sync_proxies

      Потом пачка per-host джоб:
      synchost_assignment(assignment_id), depends_on=J5.
    """
    job = get_current_job()
    queue = get_queue(LOW_QUEUE_NAME)
    server_id = zabbixserver.pk

    logger.info("SyncAll dispatcher started for ZabbixServer id=%s", server_id)

    # --- глобальные джобы по цепочке ----------------------------------------
    # УБРАНО: sync_templates_from_zabbix

    j1 = queue.enqueue(
        "nbxsync.worker.ensure_hostgroup_assignments",
        args=(server_id,),
        timeout=9000,
        description=f"Ensure hostgroup assignments (server={server_id})",
    )

    j2 = queue.enqueue(
        "nbxsync.worker.sync_hostgroups_to_zabbix",
        args=(server_id,),
        timeout=9000,
        depends_on=j1,
        description=f"Sync hostgroups to Zabbix (server={server_id})",
    )

    j3 = queue.enqueue(
        "nbxsync.worker.sync_proxy_groups",
        args=(server_id,),
        timeout=9000,
        depends_on=j2,
        description=f"Sync proxy groups (server={server_id})",
    )

    j4 = queue.enqueue(
        "nbxsync.worker.sync_proxies",
        args=(server_id,),
        timeout=9000,
        depends_on=j3,
        description=f"Sync proxies (server={server_id})",
    )

    j_last = j4


    # --- per-host джобы ------------------------------------------------------
    assignments = list(_iter_filtered_assignments(zabbixserver))
    logger.info(
        "SyncAll dispatcher: %s assignments selected for per-host sync (server=%s)",
        len(assignments),
        server_id,
    )

    for a in assignments:
        job_id = f"synchost_{server_id}_{a.pk}"
        queue.enqueue(
            "nbxsync.worker.synchost_assignment",
            args=(a.pk,),
            timeout=9000,
            job_id=job_id,
            depends_on=j_last,
            description=f"Sync host assignment {a.pk} (server={server_id})",
        )


    if job:
        job.meta["progress"] = 100
        job.save_meta()

    logger.info(
        "SyncAll dispatcher finished for ZabbixServer id=%s: "
        "globals queued, %s per-host jobs enqueued",
        server_id,
        len(assignments),
    )
