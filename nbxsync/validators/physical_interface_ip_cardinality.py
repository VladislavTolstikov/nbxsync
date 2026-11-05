"""NetBox custom validation for physical device interface IP assignments.

Enable explicitly in NetBox CUSTOM_VALIDATORS; installation alone does not
change any NetBox objects or apply this policy.
"""

from ipaddress import ip_interface

from extras.validators import CustomValidator


class PhysicalInterfaceIPCardinalityValidator(CustomValidator):
    """One address per IP family on physical interfaces unless the role allows it.

    The exception list is deliberately narrow. A dual-stack IPv4/IPv6
    interface is permitted. Virtual machine interfaces and unassigned IPs
    are outside this policy.
    """

    MULTI_ADDRESS_ROLES = frozenset({
        'rtr', 'fw', 'csw', 'server', 'nas', 'san',
    })

    # These roles represent passive network infrastructure, not IP endpoints.
    NO_IP_ROLES = frozenset({
        'patch-panel', 'crs', 'ethernet-plug', 'hardwired-device',
    })

    def validate(self, instance, request):
        # Apply only to IP addresses attached to a physical dcim.Interface.
        # VMInterface, FHRP groups, and unassigned address records are untouched.
        if not instance.address or not instance.assigned_object_type_id or not instance.assigned_object_id:
            return

        ct = instance.assigned_object_type
        if ct.app_label != 'dcim' or ct.model != 'interface':
            return

        interface = instance.assigned_object
        if interface is None:
            return

        role = getattr(getattr(interface.device, 'role', None), 'slug', '')
        role = str(role or '').strip().casefold()

        if role in self.NO_IP_ROLES:
            self.fail('Passive equipment must not have IP addresses assigned to its interfaces.', field='address')

        if role in self.MULTI_ADDRESS_ROLES:
            return

        family = ip_interface(str(instance.address)).version

        # Each address is a separate IPAddress object. Exclude the current
        # record so ordinary edits to a single assigned IP remain possible.
        others = instance.__class__.objects.filter(
            assigned_object_type_id=instance.assigned_object_type_id,
            assigned_object_id=instance.assigned_object_id,
        ).exclude(pk=instance.pk).values_list('address', flat=True)

        for existing in others:
            if ip_interface(str(existing)).version == family:
                self.fail(
                    f'Only one IPv{family} address is permitted on an interface of role '
                    f'{role or "undefined"}. Unassign the old IPv{family} first.',
                    field='address',
                )
