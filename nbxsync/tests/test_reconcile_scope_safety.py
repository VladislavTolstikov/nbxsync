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


class ReconcileScopeSafetyTests(TestCase):
    def setUp(self):
        self.server = ZabbixServer.objects.create(
            name='Scope safety server',
            url='http://scope-safety.example.test',
            token='token',
        )
        self.device = create_test_device(name='scope-safety-device')
        self.device_ct = ContentType.objects.get_for_model(Device)

    @patch('nbxsync.services.reconcile._target_for_server', side_effect=RuntimeError('mapping unavailable'))
    @patch('nbxsync.services.reconcile.autofill.select_rule')
    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_scope_evaluation_failure_never_deletes_live_orphan(
        self,
        mock_connection,
        mock_select_rule,
        mock_target,
    ):
        mock_select_rule.return_value = MagicMock()
        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '3001',
                'host': self.device.name,
                'tags': [
                    {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
                    {'tag': OWNER_ID_TAG, 'value': str(self.device.pk)},
                ],
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        mock_target.assert_called_once_with(self.device, self.server.pk)
        api.host.delete.assert_not_called()
