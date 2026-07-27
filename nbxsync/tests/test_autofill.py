from types import SimpleNamespace

from django.test import SimpleTestCase

from nbxsync.choices import (
    ZabbixHostInterfaceSNMPVersionChoices,
    ZabbixHostInterfaceTypeChoices,
)
from nbxsync.services.autofill import select_rule


def make_device(role, manufacturer, model):
    return SimpleNamespace(
        role=SimpleNamespace(slug=role),
        device_type=SimpleNamespace(
            manufacturer=SimpleNamespace(name=manufacturer),
            model=model,
        ),
    )


class AutofillRuleTests(SimpleTestCase):
    def test_access_point_rule(self):
        rule = select_rule(make_device('ap', 'TP-link', 'EAP265 HD'))
        self.assertEqual(rule.templates, ('Template WiFi AP SNMP',))
        self.assertEqual(rule.interface_type, ZabbixHostInterfaceTypeChoices.SNMP)
        self.assertEqual(rule.snmp_version, ZabbixHostInterfaceSNMPVersionChoices.SNMPV2)

    def test_camera_rule(self):
        rule = select_rule(make_device('cam', 'RVi Group', 'RVi-NC4065F28'))
        self.assertEqual(rule.templates, ('Network Generic Device by SNMP',))

    def test_bmc_rule(self):
        rule = select_rule(make_device('cn', 'Lenovo', 'ThinkSystem SR650 V2'))
        self.assertEqual(rule.templates, ('Chassis by IPMI', 'Template Module ICMP Ping'))
        self.assertEqual(rule.interface_type, ZabbixHostInterfaceTypeChoices.IPMI)
        self.assertEqual(rule.port, 623)

    def test_dlink_1510_exact_rule(self):
        rule = select_rule(make_device('SW-l2', 'D-link', 'DGS-1510-28X L2+ Stackable Managed Switch'))
        self.assertEqual(rule.templates, ('D-Link DGS-1510-28X by SNMP',))

    def test_other_dlink_uses_family_template(self):
        rule = select_rule(make_device('SW-l2', 'D-link', 'DGS-1210-10P'))
        self.assertEqual(rule.templates, ('D-Link DES_DGS Switch by SNMP',))

    def test_srk_uses_snmp_v1(self):
        rule = select_rule(make_device('hvac', 'Климат-Контроль', 'СРК-3.1У (НН)'))
        self.assertEqual(rule.templates, ('SRK 3.1',))
        self.assertEqual(rule.snmp_version, ZabbixHostInterfaceSNMPVersionChoices.SNMPV1)

    def test_konica_uses_exact_model_template(self):
        rule = select_rule(make_device('mfu', 'Konica Minolta', 'c250i'))
        self.assertEqual(rule.templates, ('Konica Minolta c250i by SNMP',))

    def test_passive_role_has_no_rule(self):
        rule = select_rule(make_device('patch-panel', 'Cabeus', 'PL-24'))
        self.assertIsNone(rule)

    def test_manual_role_has_no_rule(self):
        rule = select_rule(make_device('SKUD', 'PERCo', 'CT/L14.1'))
        self.assertIsNone(rule)
