from zabbix_utils.exceptions import APIRequestError

from nbxsync.models import ZabbixServer
from nbxsync.utils import ZabbixConnection
from nbxsync.utils.sync import run_zabbix_operation


def safe_sync(sync_class, obj, **kwargs):
    extra_args = kwargs.pop('extra_args', {})

    try:
        return run_zabbix_operation(sync_class, obj, 'sync', extra_args)
    except APIRequestError as err:
        if _is_already_exists_error(err):
            return _sync_existing(sync_class, obj, extra_args)
        raise RuntimeError(f'Error syncing {sync_class.__name__}: {err}') from err
    except Exception as err:
        raise RuntimeError(f'Error syncing {sync_class.__name__}: {err}') from err


def _sync_existing(sync_class, obj, extra_args):
    zabbixserver = sync_class.resolve_zabbixserver(obj)
    try:
        zabbixserver = ZabbixServer.objects.get(pk=zabbixserver.id)
    except ZabbixServer.DoesNotExist:
        obj.update_sync_info(success=False, message='Zabbix Server not found.')
        raise

    with ZabbixConnection(zabbixserver) as api:
        sync_instance = sync_class(api, obj, **(extra_args or {}))
        existing = sync_instance.find_by_name()

        if not existing:
            raise RuntimeError(
                f'{sync_class.__name__} already exists in Zabbix but could not be retrieved for update.'
            )

        current = existing[0]
        id_key = sync_instance.get_id_key()
        object_id = current.get(id_key)
        if object_id is None:
            raise RuntimeError(
                f'{sync_class.__name__} already exists in Zabbix but is missing expected key: {id_key}'
            )

        sync_instance.sync_to_zabbix(object_id)
        return current


def _is_already_exists_error(err: APIRequestError) -> bool:
    """Detect Zabbix duplicate errors from APIRequestError payloads."""

    candidates = [str(err)]

    for attr in ('data', 'message'):
        value = getattr(err, attr, None)
        if isinstance(value, str):
            candidates.append(value)

    combined = ' '.join(candidates).lower()
    return 'already exists' in combined
