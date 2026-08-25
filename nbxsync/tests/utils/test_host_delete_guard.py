from unittest.mock import MagicMock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.utils.sync import HostSync


OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'


class HostDeleteGuardTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='delete-guard-device')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.server = ZabbixServer.objects.create(
            name='Delete guard server',
            url='http://delete-guard.example.test',
            token='token',
        )
        self.assignment = ZabbixServerAssignment.objects.create(
            zabbixserver=self.server,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
            hostid=1001,
        )
        self.api = MagicMock()
        self.api.maintenance.get.return_value = []
        self.sync = HostSync(
            self.api,
            self.assignment,
            all_objects={},
        )

    def owner_tags(self, object_id=None):
        return [
            {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
            {'tag': OWNER_ID_TAG, 'value': str(object_id or self.device.pk)},
        ]

    def test_matching_owner_tags_allow_delete(self):
        self.api.host.get.return_value = [
            {
                'hostid': '1001',
                'host': self.device.name,
                'tags': self.owner_tags(),
            },
        ]

        self.sync.delete()

        self.api.host.delete.assert_called_once_with([1001])
        self.assignment.refresh_from_db()
        self.assertIsNone(self.assignment.hostid)

    def test_foreign_owner_tags_refuse_delete_and_clear_stale_hostid(self):
        self.api.host.get.return_value = [
            {
                'hostid': '1001',
                'host': 'foreign-host',
                'tags': self.owner_tags(object_id=self.device.pk + 1),
            },
        ]

        with self.assertRaises(RuntimeError):
            self.sync.delete()

        self.api.host.delete.assert_not_called()
        self.assignment.refresh_from_db()
        self.assertIsNone(self.assignment.hostid)

    def test_untagged_host_is_never_deleted(self):
        self.api.host.get.return_value = [
            {'hostid': '1001', 'host': self.device.name, 'tags': []},
        ]

        with self.assertRaises(RuntimeError):
            self.sync.delete()

        self.api.host.delete.assert_not_called()
        self.assignment.refresh_from_db()
        self.assertEqual(self.assignment.hostid, 1001)

    def test_missing_host_clears_stale_local_id(self):
        self.api.host.get.return_value = []

        self.sync.delete()

        self.api.host.delete.assert_not_called()
        self.assignment.refresh_from_db()
        self.assertIsNone(self.assignment.hostid)
