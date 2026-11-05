from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import TestCase

from dcim.models import DeviceRole, Interface
from ipam.models import IPAddress
from utilities.testing import create_test_device

from nbxsync.validators.physical_interface_ip_cardinality import PhysicalInterfaceIPCardinalityValidator


class PhysicalInterfaceIPCardinalityValidatorTests(TestCase):
    def setUp(self):
        self.device = create_test_device(name='ip-policy-device')
        self.interface = Interface.objects.create(
            device=self.device, name='mgmt', type='1000base-t',
        )
        self.content_type = ContentType.objects.get_for_model(Interface)
        self.validator = PhysicalInterfaceIPCardinalityValidator()

    def set_role(self, slug):
        role, _ = DeviceRole.objects.get_or_create(
            slug=slug, defaults={'name': f'IP policy {slug}'},
        )
        self.device.role = role
        self.device.save(update_fields=['role'])

    def assigned(self, address, interface=None):
        return IPAddress.objects.create(
            address=address,
            assigned_object=interface or self.interface,
        )

    def candidate(self, address, interface=None):
        return IPAddress(
            address=address,
            assigned_object=interface or self.interface,
        )

    def test_first_ipv4_allowed_for_mfu(self):
        self.set_role('mfu')
        self.validator(self.candidate('10.0.75.12/24'))

    def test_second_ipv4_blocked_for_mfu(self):
        self.set_role('mfu')
        self.assigned('10.0.75.18/24')
        with self.assertRaisesMessage(ValidationError, 'Only one IPv4'):
            self.validator(self.candidate('10.0.75.12/24'))

    def test_one_ipv4_and_one_ipv6_are_allowed(self):
        self.set_role('mfu')
        self.assigned('10.0.75.12/24')
        self.validator(self.candidate('2001:db8::12/64'))

    def test_second_ipv6_blocked(self):
        self.set_role('mfu')
        self.assigned('2001:db8::10/64')
        with self.assertRaisesMessage(ValidationError, 'Only one IPv6'):
            self.validator(self.candidate('2001:db8::12/64'))

    def test_existing_single_address_can_be_edited(self):
        self.set_role('mfu')
        ip = self.assigned('10.0.75.12/24')
        ip.description = 'Existing address'
        self.validator(ip)

    def test_removing_old_address_allows_new_one(self):
        self.set_role('mfu')
        ip = self.assigned('10.0.75.18/24')
        ip.assigned_object = None
        ip.save(update_fields=['assigned_object_type', 'assigned_object_id'])
        self.validator(self.candidate('10.0.75.12/24'))

    def test_distinct_physical_interfaces_have_independent_limits(self):
        self.set_role('mfu')
        self.assigned('10.0.75.18/24')
        another = Interface.objects.create(
            device=self.device, name='lan', type='1000base-t',
        )
        self.validator(self.candidate('10.0.75.12/24', another))

    def test_router_firewall_core_server_and_storage_roles_allow_multiple(self):
        for role in ('rtr', 'fw', 'csw', 'server', 'nas', 'san'):
            with self.subTest(role=role):
                self.set_role(role)
                self.validator(self.candidate('10.0.75.12/24'))
                old = self.assigned('10.0.75.18/24')
                self.validator(self.candidate('10.0.75.12/24'))
                old.delete()

    def test_other_network_roles_are_not_automatically_exempt(self):
        for role in ('sw-l2', 'aggregation-switchboard', 'cam', 'ap', 'td', 'cn', 'hpcn'):
            with self.subTest(role=role):
                self.set_role(role)
                old = self.assigned('10.0.75.18/24')
                with self.assertRaisesMessage(ValidationError, 'Only one IPv4'):
                    self.validator(self.candidate('10.0.75.12/24'))
                old.delete()

    def test_patch_panel_rejects_even_first_ip(self):
        self.set_role('patch-panel')
        with self.assertRaisesMessage(ValidationError, 'Passive equipment'):
            self.validator(self.candidate('10.0.75.12/24'))

    def test_unassigned_ip_addresses_are_not_restricted(self):
        self.set_role('mfu')
        self.validator(IPAddress(address='10.0.75.12/24'))

    def test_interface_with_other_family_only_does_not_count(self):
        self.set_role('td')
        self.assigned('2001:db8::12/64')
        self.validator(self.candidate('10.0.75.12/24'))
