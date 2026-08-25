from unittest.mock import MagicMock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from ipam.models import IPAddress

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.choices import (
    ZabbixHostInterfaceTypeChoices,
    ZabbixInterfaceTypeChoices,
    ZabbixInterfaceUseChoices,
)
from nbxsync.models import ZabbixHostInterface, ZabbixServer, ZabbixServerAssignment
from nbxsync.utils.sync.hostinterfacesync import HostInterfaceSync


class HostInterfaceSyncNonDefaultTestCase(TestCase):
    def setUp(self):
        self.device = create_test_device(name='HostInterfaceSyncNonDefaultDevice')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.zabbixserver = ZabbixServer.objects.create(
            name='Zabbix Main',
            url='http://example.com',
            token='dummy-token',
        )
        self.ip = IPAddress.objects.create(address='10.0.0.20/32')
        self.assignment = ZabbixServerAssignment.objects.create(
            zabbixserver=self.zabbixserver,
            hostid='10101',
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.id,
        )
        self.interface = ZabbixHostInterface.objects.create(
            zabbixserver=self.zabbixserver,
            type=ZabbixHostInterfaceTypeChoices.SNMP,
            interface_type=ZabbixInterfaceTypeChoices.NOTDEFAULT,
            useip=ZabbixInterfaceUseChoices.IP,
            ip=self.ip,
            port=161,
            snmp_version=2,
            snmp_usebulk=True,
            snmp_community='private-to-srx@public',
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.id,
        )

    def _sync(self):
        api = MagicMock()
        sync = HostInterfaceSync(api=api, netbox_obj=self.interface)
        sync.context = {}
        return sync, api

    @staticmethod
    def _zabbix_interface(interfaceid, community):
        return {
            'interfaceid': str(interfaceid),
            'hostid': '10101',
            'type': '2',
            'main': '0',
            'useip': '1',
            'ip': '10.0.0.20',
            'dns': '',
            'port': '161',
            'details': {
                'version': '2',
                'bulk': '1',
                'community': community,
            },
        }

    def test_different_snmp_community_is_not_adopted(self):
        sync, api = self._sync()
        api.hostinterface.get.return_value = [self._zabbix_interface(2001, 'public')]

        result = sync._find_existing_interface('10101')

        self.assertIsNone(result)

    def test_matching_snmp_community_is_adopted(self):
        sync, api = self._sync()
        api.hostinterface.get.return_value = [
            self._zabbix_interface(2001, 'public'),
            self._zabbix_interface(2002, 'private-to-srx@public'),
        ]

        result = sync._find_existing_interface('10101')

        self.assertEqual(result['interfaceid'], '2002')

    def test_sync_creates_second_nondefault_interface_for_new_community(self):
        sync, api = self._sync()
        api.hostinterface.get.return_value = [self._zabbix_interface(2001, 'public')]
        api.hostinterface.create.return_value = {'interfaceids': ['2002']}

        sync.sync()

        self.interface.refresh_from_db()
        self.assertEqual(self.interface.interfaceid, 2002)
        api.hostinterface.create.assert_called_once()
        api.hostinterface.update.assert_not_called()
