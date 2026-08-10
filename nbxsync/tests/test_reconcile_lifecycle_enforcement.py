from unittest.mock import MagicMock, patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.models import ZabbixServer
from nbxsync.services.reconcile import (
    OWNER_ID_TAG,
    OWNER_TYPE_TAG,
    reconcile_managed_hosts,
)


class ReconcileLifecycleEnforcementTests(TestCase):
    def setUp(self):
        self.server = ZabbixServer.objects.create(
            name='Lifecycle enforcement server',
            url='http://lifecycle.example.test',
            token='token',
        )
        self.device = create_test_device(name='lifecycle-device')
        self.device_ct = ContentType.objects.get_for_model(Device)

    def tags(self):
        return [
            {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
            {'tag': OWNER_ID_TAG, 'value': str(self.device.pk)},
        ]

    @patch('nbxsync.services.reconcile.device_is_auto_managed_on_server', return_value=True)
    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_staged_orphan_is_disabled_even_without_assignment(
        self,
        mock_connection,
        mock_managed,
    ):
        self.device.status = 'staged'
        self.device.save()

        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '5001',
                'host': self.device.name,
                'status': '0',
                'tags': self.tags(),
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        mock_managed.assert_called_once_with(self.device, self.server.pk)
        api.host.update.assert_called_once_with(hostid='5001', status=1)
        api.host.delete.assert_not_called()

    @patch('nbxsync.services.reconcile.device_is_auto_managed_on_server', return_value=True)
    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_active_orphan_is_enabled_even_without_assignment(
        self,
        mock_connection,
        mock_managed,
    ):
        self.device.status = 'active'
        self.device.save()

        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '5002',
                'host': self.device.name,
                'status': '1',
                'tags': self.tags(),
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.update.assert_called_once_with(hostid='5002', status=0)
        api.host.delete.assert_not_called()
