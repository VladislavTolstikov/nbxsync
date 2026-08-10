from unittest.mock import MagicMock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.utils.sync import HostSync


OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'


class IdentityGuardDuplicateTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='duplicate-guard-device')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.server = ZabbixServer.objects.create(
            name='Duplicate guard server',
            url='http://duplicate-guard.example.test',
            token='token',
        )
        self.assignment = ZabbixServerAssignment.objects.create(
            zabbixserver=self.server,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
        )
        self.api = MagicMock()
        self.sync = HostSync(self.api, self.assignment, all_objects={})
        self.sync._ensure_zbx_groups = MagicMock()
        self.sync.get_create_params = MagicMock(
            return_value={'host': self.device.name}
        )
        self.sync.try_create = MagicMock(
            side_effect=RuntimeError('Host with the same name already exists')
        )
        self.sync.sync_to_zabbix = MagicMock()

    def test_untagged_duplicate_is_never_adopted(self):
        self.api.host.get.return_value = [
            {'hostid': '4001', 'host': self.device.name, 'tags': []},
        ]

        with self.assertRaises(RuntimeError):
            self.sync._create_host()

        self.sync.sync_to_zabbix.assert_not_called()

    def test_correctly_owned_duplicate_can_be_recovered(self):
        self.api.host.get.return_value = [
            {
                'hostid': '4002',
                'host': self.device.name,
                'tags': [
                    {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
                    {'tag': OWNER_ID_TAG, 'value': str(self.device.pk)},
                ],
            },
        ]

        result = self.sync._create_host()

        self.assertEqual(result, '4002')
        self.sync.sync_to_zabbix.assert_called_once_with('4002')
