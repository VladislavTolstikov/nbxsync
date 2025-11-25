from unittest.mock import MagicMock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.utils.sync import HostSync


class HostSyncIdentityTestCase(TestCase):
    def setUp(self):
        self.device_a = create_test_device(name="SW. Core 1")
        self.device_b = create_test_device(name="SW. Core 10")
        device_ct = ContentType.objects.get_for_model(Device)

        self.zabbixserver = ZabbixServer.objects.create(name="ZBX", url="http://zabbix", token="abc")

        self.assignment_a = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            assigned_object_type=device_ct,
            assigned_object_id=self.device_a.id,
            hostid="10101",
        )
        self.assignment_b = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            assigned_object_type=device_ct,
            assigned_object_id=self.device_b.id,
            hostid="10102",
        )

        self.all_objects = {
            "hostgroups": [],
            "hostinterfaces": [],
            "templates": [],
            "tags": [],
            "macros": [],
            "hostinventory": None,
        }

        self.assignment_a.assigned_objects = self.all_objects
        self.assignment_b.assigned_objects = self.all_objects

    def _make_api(self):
        api = MagicMock()
        api.host.create.return_value = {"hostids": ["20001"]}
        api.host.update.return_value = {"hostids": ["20001"]}
        api.hostinterface.get.return_value = []
        api.template.get.return_value = []
        return api

    def test_sync_updates_each_hostid_without_cross_rename(self):
        api = self._make_api()
        api.host.get.side_effect = [
            [{"hostid": "10101"}],
            [{"hostid": "10102"}],
        ]

        HostSync(api, self.assignment_a, all_objects=self.all_objects).sync()
        HostSync(api, self.assignment_b, all_objects=self.all_objects).sync()

        update_calls = api.host.update.call_args_list
        self.assertEqual(len(update_calls), 2)

        self.assertEqual(update_calls[0].kwargs["hostid"], "10101")
        self.assertEqual(update_calls[0].kwargs["host"], "SW. Core 1")
        self.assertEqual(update_calls[0].kwargs["name"], "SW. Core 1")

        self.assertEqual(update_calls[1].kwargs["hostid"], "10102")
        self.assertEqual(update_calls[1].kwargs["host"], "SW. Core 10")
        self.assertEqual(update_calls[1].kwargs["name"], "SW. Core 10")

    def test_sync_creates_host_without_name_lookup_when_missing_hostid(self):
        assignment = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=self.device_b.id,
            hostid=None,
        )
        assignment.assigned_objects = self.all_objects
        api = self._make_api()

        HostSync(api, assignment, all_objects=self.all_objects).sync()

        api.host.create.assert_called_once()
        create_kwargs = api.host.create.call_args.kwargs
        self.assertEqual(create_kwargs["host"], "SW. Core 10")
        self.assertEqual(create_kwargs["name"], "SW. Core 10")
        api.host.get.assert_not_called()
