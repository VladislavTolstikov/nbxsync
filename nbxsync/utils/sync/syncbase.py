import logging

from django.db import IntegrityError
from zabbix_utils.exceptions import APIRequestError

logger = logging.getLogger(__name__)
from nbxsync.choices.syncsot import SyncSOT
from nbxsync.settings import get_plugin_settings
from nbxsync.utils.resolve_attr import resolve_attr
from nbxsync.utils.resolve_zabbixserver import resolve_zabbixserver
from nbxsync.utils.set_nested_attr import set_nested_attr
from nbxsync.utils.sync.safe_sync import _is_already_exists_error


class ZabbixSyncBase:
    id_field: str  # e.g., 'proxy_groupid'
    name_field: str = 'name'
    sot_key: str = None
    zabbixserver_path: str = None

    def __init__(self, api, netbox_obj, **kwargs):
        self.api = api
        self.obj = netbox_obj
        self.context = kwargs  # catch any extra data

        # Move SoT resolution here
        self.pluginsettings = get_plugin_settings()
        if not self.sot_key:
            raise ValueError(f'{self.__class__.__name__} must define `sot_key`.')
        self.sot = getattr(self.pluginsettings.sot, self.sot_key, None)
        if self.sot is None:
            raise ValueError(f"No source-of-truth setting found for key '{self.sot_key}'.")

    @classmethod
    def resolve_zabbixserver(cls, obj):
        if callable(cls.zabbixserver_path):
            return cls.zabbixserver_path(obj)
        return resolve_zabbixserver(obj, fallback_path=cls.zabbixserver_path)

    def sync(self) -> None:
        obj_id = self.get_id()

        if obj_id:
            existing = self.find_by_id()
            if existing:
                self.sync_to_zabbix(obj_id)
                logger.debug(f'Found and synced {self.__class__.__name__} ID: {obj_id}')
                return

        self.create_in_zabbix()

    def try_create(self) -> str:
        try:
            # print('Create params:')
            # print(self.get_create_params())
            result = self.api_object().create(**self.get_create_params())
            # print('Zabbix result: ')
            # print(result)
            return result.get(self.result_key(), [None])[0]
        except Exception as err:
            msg = f'{self.__class__.__name__} creation failed: {err}'
            raise RuntimeError(msg)

    def create_or_get(self, *, create_params=None, natural_key_filter=None):
        params = create_params or self.get_create_params()
        id_key = self.get_id_key()
        try:
            result = self.api_object().create(**params)
            return result.get(self.result_key(), [None])[0]
        except APIRequestError as err:
            if not _is_already_exists_error(err):
                raise
            filter_params = natural_key_filter or self.get_natural_key_filter(params)
            existing = self.api_object().get(filter=filter_params, output=[id_key])
            if len(existing) != 1 or id_key not in existing[0]:
                raise RuntimeError(
                    f'{self.__class__.__name__} already exists in Zabbix but could not be uniquely resolved by natural key.'
                ) from err
            return existing[0][id_key]

    def create_in_zabbix(self):
        object_id = self.create_or_get()
        if not object_id:
            raise RuntimeError(f'{self.__class__.__name__} creation returned no ID.')

        try:
            self.set_id(object_id)
            try:
                self.obj.save(update_fields=[self.get_id_key()])
            except (TypeError, ValueError):
                self.obj.save()
        except IntegrityError:
            zabbixserver = self.resolve_zabbixserver(self.obj)
            current = self.obj.__class__.objects.filter(**{self.get_id_key(): object_id, 'zabbixserver': zabbixserver}).first()
            if current:
                logger.warning(
                    '%s with id %s already exists for Zabbix server %s, reusing existing row',
                    self.__class__.__name__,
                    object_id,
                    getattr(zabbixserver, 'id', zabbixserver),
                )
                self.obj = current
            else:
                raise

        self.obj.update_sync_info(success=True)
        return object_id

    def get_natural_key_filter(self, create_params: dict) -> dict:
        name_key = self.name_field.split('.')[-1] if self.name_field else 'name'
        return {name_key: create_params.get(name_key)}

    def find_by_name(self):
        name_key = 'name'
        if self.name_field:
            name_key = self.name_field.split('.')[-1]

        return self.api_object().get(search={name_key: self.get_name_value()})

    def find_by_id(self):
        id_key = self.get_id_key()
        return self.api_object().get(**{f'{id_key}s': self.get_id()})

    def handle_found(self, data: dict) -> str:
        object_id = data[self.get_id_key()]

        if self.sot == SyncSOT.ZABBIX:
            self.sync_from_zabbix(data)
        elif self.sot == SyncSOT.NETBOX:
            self.sync_to_zabbix(object_id)
        logger.debug(f'Found and synced {self.__class__.__name__} ID: {object_id}')
        return object_id

    def sync_from_zabbix(self, data: dict) -> None:
        raise NotImplementedError

    def sync_to_zabbix(self, object_id: str) -> None:
        self.set_id(object_id)
        self.obj.save()
        self.obj.update_sync_info(success=True)
        self.update_in_zabbix(object_id=object_id)

    def update_in_zabbix(self, **kwargs) -> None:
        # print('Update params:')
        # print(self.get_update_params(object_id=kwargs.get('object_id', None)))
        result = self.api_object().update(**self.get_update_params(object_id=kwargs.get('object_id', None)))
        # print(result)
        logger.debug(f'Updated {self.__class__.__name__} ID {self.get_id()}')

    # --- Object-specific methods to override per implementation ---

    def delete(self) -> None:
        raise NotImplementedError

    def api_object(self):
        raise NotImplementedError

    def get_id(self):
        try:
            return resolve_attr(self.obj, self.id_field)
        except AttributeError:
            return None

    def get_id_key(self):
        return self.id_field.split('.')[-1]  # Use only the final part of the id_field field

    def set_id(self, value):
        set_nested_attr(self.obj, self.id_field, value)

        # Save the nested object if needed
        if '.' in self.id_field:
            # Save the outermost related object
            parent_obj = resolve_attr(self.obj, '.'.join(self.id_field.split('.')[:-1]))
            if hasattr(parent_obj, 'save'):
                parent_obj.save()

    def get_create_params(self, **kwargs) -> dict:
        raise NotImplementedError

    def get_update_params(self, **kwarg) -> dict:
        raise NotImplementedError

    def result_key(self) -> str:
        """Return key in result dict, e.g., 'proxy_groupids'."""
        raise NotImplementedError

    def get_name_value(self):
        if self.name_field:
            return resolve_attr(self.obj, self.name_field)

        return getattr(self.obj, 'name', None)
