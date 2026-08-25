from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError, transaction
from django.test import TestCase
from ipam.models import IPAddress

from dcim.models import Device
from utilities.testing import create_test_device

from nbxsync.choices import (
    ZabbixHostInterfaceTypeChoices,
    ZabbixInterfaceTypeChoices,
    ZabbixInterfaceUseChoices,
)
from nbxsync.models import ZabbixHostInterface, ZabbixServer


class ZabbixHostInterfaceNonDefaultConstraintTestCase(TestCase):
    def setUp(self):
        self.device = create_test_device(name='HostInterfaceNonDefaultDevice')
        self.device_ct = ContentType.objects.get_for_model(Device)
        self.zabbixserver = ZabbixServer.objects.create(name='Zabbix Main')
        self.ip = IPAddress.objects.create(address='10.0.0.10/32')

    def _create_snmp_interface(self, *, interface_type, community):
        return ZabbixHostInterface.objects.create(
            zabbixserver=self.zabbixserver,
            type=ZabbixHostInterfaceTypeChoices.SNMP,
            interface_type=interface_type,
            useip=ZabbixInterfaceUseChoices.IP,
            ip=self.ip,
            port=161,
            snmp_community=community,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.id,
        )

    def test_multiple_nondefault_interfaces_of_same_type_are_allowed(self):
        first = self._create_snmp_interface(
            interface_type=ZabbixInterfaceTypeChoices.NOTDEFAULT,
            community='public',
        )
        second = self._create_snmp_interface(
            interface_type=ZabbixInterfaceTypeChoices.NOTDEFAULT,
            community='private-to-srx@public',
        )

        self.assertNotEqual(first.pk, second.pk)
        self.assertEqual(
            ZabbixHostInterface.objects.filter(
                zabbixserver=self.zabbixserver,
                type=ZabbixHostInterfaceTypeChoices.SNMP,
                assigned_object_type=self.device_ct,
                assigned_object_id=self.device.id,
            ).count(),
            2,
        )

    def test_only_one_default_interface_of_same_type_is_allowed(self):
        self._create_snmp_interface(
            interface_type=ZabbixInterfaceTypeChoices.DEFAULT,
            community='public',
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self._create_snmp_interface(
                    interface_type=ZabbixInterfaceTypeChoices.DEFAULT,
                    community='private-to-srx@public',
                )
