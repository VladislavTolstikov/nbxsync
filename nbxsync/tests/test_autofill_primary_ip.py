from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from ipam.models import IPAddress
from utilities.testing import create_test_device

from nbxsync.choices import (
    ZabbixHostInterfaceSNMPVersionChoices,
    ZabbixHostInterfaceTypeChoices,
    ZabbixInterfaceTypeChoices,
    ZabbixInterfaceUseChoices,
)
from nbxsync.models import ZabbixHostInterface, ZabbixServer
from nbxsync.services.autofill import AutofillResult, Rule, _ensure_interface


class AutofillPrimaryIPReconciliationTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='autofill-primary-ip-device')
        self.device_ct = ContentType.objects.get_for_model(
            self.device,
            for_concrete_model=False,
        )
        self.server = ZabbixServer.objects.create(
            name='Autofill primary IP test',
            url='http://zabbix.example.test',
            token='token',
        )
        self.old_ip = IPAddress.objects.create(address='10.0.75.18/24')
        self.primary_ip = IPAddress.objects.create(address='10.0.75.12/24')
        self.rule = Rule(
            templates=(),
            interface_type=ZabbixHostInterfaceTypeChoices.SNMP,
            port=161,
            snmp_version=ZabbixHostInterfaceSNMPVersionChoices.SNMPV2,
        )

    def _create_existing(self, **overrides):
        values = {
            'zabbixserver': self.server,
            'type': ZabbixHostInterfaceTypeChoices.SNMP,
            'interface_type': ZabbixInterfaceTypeChoices.DEFAULT,
            'assigned_object_type': self.device_ct,
            'assigned_object_id': self.device.pk,
            'interfaceid': 3908,
            'ip': self.old_ip,
            'useip': ZabbixInterfaceUseChoices.IP,
            'port': 162,
            'snmp_version': ZabbixHostInterfaceSNMPVersionChoices.SNMPV1,
        }
        values.update(overrides)
        return ZabbixHostInterface.objects.create(**values)

    def _fill(self):
        result = AutofillResult()
        _ensure_interface(
            self.device,
            self.device_ct,
            self.server,
            self.rule,
            self.primary_ip,
            result,
        )
        return result

    def test_existing_default_ip_interface_follows_device_primary_ip(self):
        interface = self._create_existing()
        result = self._fill()
        interface.refresh_from_db()

        self.assertEqual(interface.ip_id, self.primary_ip.pk)
        self.assertEqual(interface.interfaceid, 3908)
        self.assertEqual(interface.port, 162)
        self.assertEqual(
            interface.snmp_version,
            ZabbixHostInterfaceSNMPVersionChoices.SNMPV1,
        )
        self.assertEqual(result.interfaces_updated, 1)
        self.assertEqual(result.interfaces_created, 0)

        repeated = self._fill()
        self.assertEqual(repeated.interfaces_updated, 0)

    def test_new_interface_uses_device_primary_ip(self):
        result = self._fill()
        interface = ZabbixHostInterface.objects.get(
            zabbixserver=self.server,
            type=ZabbixHostInterfaceTypeChoices.SNMP,
            assigned_object_type=self.device_ct,
            assigned_object_id=self.device.pk,
        )

        self.assertEqual(interface.ip_id, self.primary_ip.pk)
        self.assertEqual(result.interfaces_created, 1)
        self.assertEqual(result.interfaces_updated, 0)

    def test_dns_interface_is_not_overwritten(self):
        interface = self._create_existing(
            useip=ZabbixInterfaceUseChoices.DNS,
            dns='mmfu12.example.test',
        )

        result = self._fill()
        interface.refresh_from_db()

        self.assertEqual(interface.ip_id, self.old_ip.pk)
        self.assertEqual(interface.dns, 'mmfu12.example.test')
        self.assertEqual(result.interfaces_updated, 0)
        self.assertTrue(any('IP' in warning for warning in result.warnings))

    def test_nondefault_interface_is_not_overwritten(self):
        interface = self._create_existing(
            interface_type=ZabbixInterfaceTypeChoices.NOTDEFAULT,
        )

        result = self._fill()
        interface.refresh_from_db()

        self.assertEqual(interface.ip_id, self.old_ip.pk)
        self.assertEqual(result.interfaces_updated, 0)
        self.assertTrue(any('IP' in warning for warning in result.warnings))
