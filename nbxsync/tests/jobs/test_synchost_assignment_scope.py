from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.jobs.synchost import SyncHostJob
from nbxsync.models import ZabbixServer, ZabbixServerAssignment


class SyncHostAssignmentScopeTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='assignment-scope-device')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.server1 = ZabbixServer.objects.create(
            name='Scope server 1',
            url='http://scope1.example.test',
            token='token1',
        )
        self.server2 = ZabbixServer.objects.create(
            name='Scope server 2',
            url='http://scope2.example.test',
            token='token2',
        )
        self.assignment1 = ZabbixServerAssignment.objects.create(
            zabbixserver=self.server1,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
        )
        self.assignment2 = ZabbixServerAssignment.objects.create(
            zabbixserver=self.server2,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
        )

    @patch.object(SyncHostJob, 'verify_hostinterfaces')
    @patch.object(SyncHostJob, 'sync_host', return_value={})
    def test_assignment_id_limits_sync_to_one_server(self, mock_sync_host, mock_verify):
        SyncHostJob(
            instance=self.device,
            assignment_id=self.assignment1.pk,
        ).run()

        mock_sync_host.assert_called_once()
        synced_assignment = mock_sync_host.call_args.args[0]
        self.assertEqual(synced_assignment.pk, self.assignment1.pk)
        self.assertNotEqual(synced_assignment.pk, self.assignment2.pk)
        mock_verify.assert_called_once()
