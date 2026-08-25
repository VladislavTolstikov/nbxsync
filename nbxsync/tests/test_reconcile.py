from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase, TestCase
from ipam.models import IPAddress

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.choices import ZabbixHostInterfaceTypeChoices
from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.models import ZabbixHostInterface, ZabbixServer, ZabbixServerAssignment
from nbxsync.services.reconcile import (
    OWNER_ID_TAG,
    OWNER_TYPE_TAG,
    desired_host_status,
    reconcile_managed_hosts,
)


class DesiredHostStatusTests(SimpleTestCase):
    @staticmethod
    def device(status):
        return SimpleNamespace(
            status=status,
            _meta=SimpleNamespace(model_name='device'),
        )

    def test_active_is_enabled(self):
        self.assertEqual(
            desired_host_status(self.device('active')),
            ZabbixHostStatus.ENABLED,
        )

    def test_staged_and_planned_are_disabled(self):
        for status in ('staged', 'planned'):
            with self.subTest(status=status):
                self.assertEqual(
                    desired_host_status(self.device(status)),
                    ZabbixHostStatus.DISABLED,
                )

    def test_every_other_device_status_is_deleted(self):
        for status in ('offline', 'decommissioning', 'under-replacement', 'anything-custom'):
            with self.subTest(status=status):
                self.assertEqual(
                    desired_host_status(self.device(status)),
                    ZabbixHostStatus.DELETED,
                )


class ReconcileManagedHostsTests(TestCase):
    def setUp(self):
        self.server = ZabbixServer.objects.create(
            name='Zabbix reconcile test',
            url='http://zabbix.example.test',
            token='token',
        )
        self.device = create_test_device(name='reconcile-device')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.ip = IPAddress.objects.create(address='192.0.2.100/32')

    def owner_tags(self, object_id=None):
        return [
            {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
            {'tag': OWNER_ID_TAG, 'value': str(object_id or self.device.pk)},
        ]

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_untagged_host_is_never_deleted(self, mock_connection):
        api = MagicMock()
        api.host.get.return_value = [
            {'hostid': '1001', 'host': 'manual-host', 'tags': []},
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_not_called()

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_host_with_assignment_is_left_to_normal_sync(self, mock_connection):
        ZabbixServerAssignment.objects.create(
            zabbixserver=self.server,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
            hostid=1002,
        )

        api = MagicMock()
        api.host.get.return_value = [
            {'hostid': '1002', 'host': self.device.name, 'tags': self.owner_tags()},
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_not_called()

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_deleted_status_orphan_is_deleted_and_interface_id_cleared(self, mock_connection):
        self.device.status = 'decommissioning'
        self.device.save()

        interface = ZabbixHostInterface.objects.create(
            zabbixserver=self.server,
            type=ZabbixHostInterfaceTypeChoices.AGENT,
            interfaceid=9001,
            ip=self.ip,
            port=10050,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
        )

        api = MagicMock()
        api.host.get.return_value = [
            {'hostid': '1003', 'host': self.device.name, 'tags': self.owner_tags()},
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_called_once_with(['1003'])
        interface.refresh_from_db()
        self.assertIsNone(interface.interfaceid)

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_deleted_status_with_assignment_but_missing_hostid_is_deleted(self, mock_connection):
        self.device.status = 'decommissioning'
        self.device.save()
        assignment = ZabbixServerAssignment.objects.create(
            zabbixserver=self.server,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
            hostid=None,
        )

        api = MagicMock()
        api.host.get.return_value = [
            {'hostid': '1008', 'host': self.device.name, 'tags': self.owner_tags()},
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_called_once_with(['1008'])
        assignment.refresh_from_db()
        self.assertIsNone(assignment.hostid)

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_incomplete_owner_tags_are_not_deleted(self, mock_connection):
        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '1004',
                'host': self.device.name,
                'tags': [{'tag': OWNER_ID_TAG, 'value': str(self.device.pk)}],
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_not_called()

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_invalid_owner_tag_values_are_not_deleted(self, mock_connection):
        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '1005',
                'host': self.device.name,
                'tags': [
                    {'tag': OWNER_TYPE_TAG, 'value': 'not-an-id'},
                    {'tag': OWNER_ID_TAG, 'value': 'also-not-an-id'},
                ],
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_not_called()

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_valid_owner_type_with_missing_netbox_object_is_deleted(self, mock_connection):
        missing_object_id = self.device.pk + 1000000
        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '1006',
                'host': 'removed-device',
                'tags': self.owner_tags(object_id=missing_object_id),
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_called_once_with(['1006'])

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_missing_owner_with_matching_assignment_clears_assignment_hostid(self, mock_connection):
        missing_object_id = self.device.pk + 2000000
        # This test intentionally represents historical/stale data. The model's
        # normal save() correctly rejects an assignment whose GenericForeignKey
        # target no longer exists, so bulk_create is used to reproduce a row
        # that can remain after external or legacy cleanup.
        ZabbixServerAssignment.objects.bulk_create(
            [
                ZabbixServerAssignment(
                    zabbixserver=self.server,
                    assigned_object_type=self.device_ct,
                    assigned_object_id=missing_object_id,
                    hostid=1009,
                )
            ]
        )
        assignment = ZabbixServerAssignment.objects.get(
            zabbixserver=self.server,
            assigned_object_type=self.device_ct,
            assigned_object_id=missing_object_id,
        )

        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '1009',
                'host': 'removed-device-with-assignment',
                'tags': self.owner_tags(object_id=missing_object_id),
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_called_once_with(['1009'])
        assignment.refresh_from_db()
        self.assertIsNone(assignment.hostid)

    @patch('nbxsync.services.reconcile.device_is_auto_managed_on_server', return_value=True)
    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_live_auto_managed_orphan_is_kept_for_recovery(
        self,
        mock_connection,
        mock_auto_managed,
    ):
        api = MagicMock()
        api.host.get.return_value = [
            {'hostid': '1007', 'host': self.device.name, 'tags': self.owner_tags()},
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        mock_auto_managed.assert_called_once_with(self.device, self.server.pk)
        api.host.delete.assert_not_called()
