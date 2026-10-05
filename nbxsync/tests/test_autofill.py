from types import SimpleNamespace

from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase, TestCase

from utilities.testing import create_test_device

from nbxsync.choices import (
    ZabbixHostInterfaceSNMPVersionChoices,
    ZabbixHostInterfaceTypeChoices,
)
from nbxsync.models import ZabbixHostgroup, ZabbixHostgroupAssignment, ZabbixServer
from nbxsync.services.autofill import (
    AutofillResult,
    _ensure_groups,
    _group_names,
    select_rule,
)


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


class AutofillGroupNameTests(SimpleTestCase):
    def test_access_point_groups_for_mk_tk_sg(self):
        for site_key, group_site in (('MK', 'MK'), ('TK', 'TK'), ('SG', 'SG'), ('STUDGORODOK', 'SG')):
            with self.subTest(site_key=site_key):
                result = AutofillResult()
                names = _group_names(
                    make_device('ap', 'TP-Link', 'EAP265 HD'),
                    site_key,
                    result,
                )
                self.assertEqual(
                    names,
                    (
                        f'NET/{group_site}',
                        f'AP/{group_site}',
                        'TP-Link/EAP265 HD',
                    ),
                )

    def test_switch_groups_for_mk_tk_sg(self):
        switch_roles = (
            'sw-l2',
            'sw-l2-zk',
            'switches',
            'aggregation-switchboard',
            'csw',
            'sw-fc',
        )
        for role in switch_roles:
            for site_key, group_site in (('MK', 'MK'), ('TK', 'TK'), ('SG', 'SG'), ('STUDGORODOK', 'SG')):
                with self.subTest(role=role, site_key=site_key):
                    result = AutofillResult()
                    names = _group_names(
                        make_device(role, 'D-Link', 'DGS-1210-10P'),
                        site_key,
                        result,
                    )
                    self.assertEqual(
                        names,
                        (
                            f'NET/{group_site}',
                            f'Switch/{group_site}',
                            'D-Link/DGS-1210-10P',
                        ),
                    )

    def test_router_does_not_get_switch_group(self):
        result = AutofillResult()
        names = _group_names(
            make_device('rtr', 'MikroTik', 'CCR2004'),
            'TK',
            result,
        )
        self.assertEqual(names, ('NET/TK', 'MikroTik/CCR2004'))


class AutofillHostgroupReconciliationTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='autofill-existing-device')
        self.content_type = ContentType.objects.get_for_model(
            self.device,
            for_concrete_model=False,
        )
        self.server = ZabbixServer.objects.create(
            name='Autofill hostgroup test',
            url='http://zabbix.example.test',
            token='token',
        )

    def _assign(self, name):
        group, _ = ZabbixHostgroup.objects.get_or_create(
            zabbixserver=self.server,
            name=name,
        )
        ZabbixHostgroupAssignment.objects.get_or_create(
            zabbixhostgroup=group,
            assigned_object_type=self.content_type,
            assigned_object_id=self.device.pk,
        )

    def _assigned_names(self):
        return set(
            ZabbixHostgroup.objects.filter(
                zabbixhostgroupassignment__assigned_object_type=self.content_type,
                zabbixhostgroupassignment__assigned_object_id=self.device.pk,
            ).values_list('name', flat=True)
        )

    def _assert_incomplete_existing_device_is_completed_idempotently(self, required_names):
        for name in (required_names[0], required_names[-1], 'Manual/Keep'):
            self._assign(name)

        first_result = AutofillResult()
        _ensure_groups(
            self.device,
            self.content_type,
            self.server,
            required_names,
            first_result,
        )

        self.assertEqual(first_result.hostgroup_assignments_created, 1)
        self.assertEqual(
            self._assigned_names(),
            set(required_names) | {'Manual/Keep'},
        )

        second_result = AutofillResult()
        _ensure_groups(
            self.device,
            self.content_type,
            self.server,
            required_names,
            second_result,
        )

        self.assertEqual(second_result.hostgroups_created, 0)
        self.assertEqual(second_result.hostgroup_assignments_created, 0)
        self.assertEqual(
            self._assigned_names(),
            set(required_names) | {'Manual/Keep'},
        )

    def test_existing_ap_missing_functional_group_is_completed(self):
        self._assert_incomplete_existing_device_is_completed_idempotently(
            ('NET/MK', 'AP/MK', 'TP-Link/EAP265 HD')
        )

    def test_existing_switch_missing_functional_group_is_completed(self):
        self._assert_incomplete_existing_device_is_completed_idempotently(
            ('NET/TK', 'Switch/TK', 'D-Link/DGS-1210-10P')
        )
