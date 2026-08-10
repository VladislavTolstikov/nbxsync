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


class ReconcileTagSafetyTests(TestCase):
    def setUp(self):
        self.server = ZabbixServer.objects.create(
            name='Reconcile tag safety',
            url='http://tag-safety.example.test',
            token='token',
        )
        self.device = create_test_device(name='tag-safety-device')
        self.device_ct = ContentType.objects.get_for_model(Device)

    @patch('nbxsync.services.reconcile.ZabbixConnection')
    def test_duplicate_owner_tags_are_never_used_for_delete(self, mock_connection):
        api = MagicMock()
        api.host.get.return_value = [
            {
                'hostid': '2001',
                'host': self.device.name,
                'tags': [
                    {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.pk)},
                    {'tag': OWNER_ID_TAG, 'value': str(self.device.pk)},
                    {'tag': OWNER_ID_TAG, 'value': str(self.device.pk + 1)},
                ],
            },
        ]
        mock_connection.return_value.__enter__.return_value = api

        reconcile_managed_hosts(self.server.pk)

        api.host.delete.assert_not_called()
