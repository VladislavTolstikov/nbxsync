from unittest.mock import MagicMock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.models import ZabbixServer, ZabbixServerAssignment
from nbxsync.utils.sync import HostSync


OWNER_TYPE_TAG = 'nbxsync.object_type_id'
OWNER_ID_TAG = 'nbxsync.object_id'


class DummyHostgroup:
    def __init__(self, server_id):
        self.zabbixserver_id = server_id
        self.groupid = 1


class DummyHostgroupAssignment:
    def __init__(self, server_id):
        self.zabbixhostgroup = DummyHostgroup(server_id)

    def render(self):
        return 'Identity Test Hosts', True

    def is_template(self):
        return False


class HostSyncIdentityTestCase(TestCase):
    def setUp(self):
        self.device_a = create_test_device(name='SW. Core 1')
        self.device_b = create_test_device(name='SW. Core 10')
        self.device_ct = ContentType.objects.get_for_model(Device)

        self.zabbixserver = ZabbixServer.objects.create(
            name='ZBX',
            url='http://zabbix',
            token='abc',
        )

        self.assignment_a = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device_a.id,
            hostid='10101',
        )
        self.assignment_b = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device_b.id,
            hostid='10102',
        )

        self.all_objects = {
            'hostgroups': [DummyHostgroupAssignment(self.zabbixserver.id)],
            'hostinterfaces': [],
            'templates': [],
            'tags': [],
            'macros': [],
            'hostinventory': None,
        }

    def _owner_tags(self, object_id):
        return [
            {'tag': OWNER_TYPE_TAG, 'value': str(self.device_ct.id)},
            {'tag': OWNER_ID_TAG, 'value': str(object_id)},
        ]

    def _make_api(self):
        api = MagicMock()
        api.host.create.return_value = {'hostids': ['20001']}
        # Zabbix host.update normally returns the updated host ID, but leaving
        # it empty exercises HostSync's fallback to the current hostid and keeps
        # this test focused on identity isolation.
        api.host.update.return_value = {}
        api.hostinterface.get.return_value = []
        api.template.get.return_value = []
        api.hostgroup.get.return_value = [
            {'groupid': '1', 'name': 'Identity Test Hosts'},
        ]
        return api

    def test_sync_updates_each_hostid_without_cross_rename(self):
        api = self._make_api()

        def get_host(**kwargs):
            hostids = kwargs.get('hostids') or []
            hostid = str(hostids[0]) if hostids else ''
            if hostid == '10101':
                return [
                    {
                        'hostid': '10101',
                        'host': self.device_a.name,
                        'name': self.device_a.name,
                        'tags': self._owner_tags(self.device_a.id),
                        'interfaces': [],
                    }
                ]
            if hostid == '10102':
                return [
                    {
                        'hostid': '10102',
                        'host': self.device_b.name,
                        'name': self.device_b.name,
                        'tags': self._owner_tags(self.device_b.id),
                        'interfaces': [],
                    }
                ]
            return []

        api.host.get.side_effect = get_host

        HostSync(api, self.assignment_a, all_objects=self.all_objects).sync()
        HostSync(api, self.assignment_b, all_objects=self.all_objects).sync()

        update_calls = api.host.update.call_args_list
        self.assertEqual(len(update_calls), 2)

        self.assertEqual(update_calls[0].kwargs['hostid'], '10101')
        self.assertEqual(update_calls[0].kwargs['host'], 'SW. Core 1')
        self.assertEqual(update_calls[0].kwargs['name'], 'SW. Core 1')

        self.assertEqual(update_calls[1].kwargs['hostid'], '10102')
        self.assertEqual(update_calls[1].kwargs['host'], 'SW. Core 10')
        self.assertEqual(update_calls[1].kwargs['name'], 'SW. Core 10')

    def test_sync_creates_host_without_name_lookup_when_missing_hostid(self):
        self.assignment_b.hostid = None
        self.assignment_b.save(update_fields=['hostid'])
        self.assignment_b.assigned_objects = self.all_objects
        api = self._make_api()

        HostSync(api, self.assignment_b, all_objects=self.all_objects).sync()

        api.host.create.assert_called_once()
        create_kwargs = api.host.create.call_args.kwargs
        self.assertEqual(create_kwargs['host'], 'SW. Core 10')
        self.assertEqual(create_kwargs['name'], 'SW. Core 10')
        api.host.get.assert_not_called()

    def test_sync_recreates_missing_hostid(self):
        api = self._make_api()
        api.host.get.return_value = []
        api.host.create.return_value = {'hostids': ['30001']}

        with self.assertLogs('nbxsync.utils.sync.hostsync', level='WARNING') as log:
            HostSync(api, self.assignment_a, all_objects=self.all_objects).sync()

        self.assignment_a.refresh_from_db()
        self.assertEqual(self.assignment_a.hostid, 30001)
        api.host.update.assert_not_called()
        self.assertTrue(any('recreate' in entry.lower() for entry in log.output))
