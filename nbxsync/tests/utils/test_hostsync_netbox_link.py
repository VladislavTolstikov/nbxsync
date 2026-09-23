from types import SimpleNamespace
from unittest.mock import MagicMock

from django.test import SimpleTestCase

from nbxsync.utils.sync.hostsync import HostSync


class HostSyncNetBoxLinkTestCase(SimpleTestCase):
    def make_sync(self, *, enabled, base_url='https://nbx.muctr.ru', hostid='12345'):
        sync = HostSync.__new__(HostSync)
        assigned = MagicMock()
        assigned.get_absolute_url.return_value = '/dcim/devices/2896/'
        assigned.__str__.return_value = 'nvmsk-ups-entel-01'

        sync.obj = SimpleNamespace(
            hostid=hostid,
            assigned_object=assigned,
        )
        sync.pluginsettings = SimpleNamespace(
            netbox_link=SimpleNamespace(
                enabled=enabled,
                base_url=base_url,
            )
        )
        sync.api = SimpleNamespace(host=MagicMock())
        return sync

    def test_enabled_adds_direct_netbox_url(self):
        sync = self.make_sync(enabled=True)

        result = sync._apply_netbox_link_macro([])

        self.assertEqual(
            result,
            [
                {
                    'macro': '{$NETBOX.URL}',
                    'value': 'https://nbx.muctr.ru/dcim/devices/2896/',
                    'description': 'NetBox object URL',
                    'type': 0,
                }
            ],
        )

    def test_enabled_replaces_existing_netbox_url(self):
        sync = self.make_sync(enabled=True)

        result = sync._apply_netbox_link_macro(
            [
                {
                    'macro': '{$NETBOX.URL}',
                    'value': 'https://old.example/dcim/devices/1/',
                    'description': 'old',
                    'type': 0,
                },
                {
                    'macro': '{$OTHER}',
                    'value': 'keep',
                    'description': '',
                    'type': 0,
                },
            ]
        )

        macros = {item['macro']: item for item in result}
        self.assertEqual(
            macros['{$NETBOX.URL}']['value'],
            'https://nbx.muctr.ru/dcim/devices/2896/',
        )
        self.assertEqual(macros['{$OTHER}']['value'], 'keep')

    def test_disabled_preserves_current_zabbix_macro(self):
        sync = self.make_sync(enabled=False)
        sync.api.host.get.return_value = [
            {
                'hostid': '12345',
                'macros': [
                    {
                        'macro': '{$NETBOX.URL}',
                        'value': 'https://manual.example/device/55/',
                        'description': 'manual',
                        'type': '0',
                    }
                ],
            }
        ]

        result = sync._apply_netbox_link_macro(
            [
                {
                    'macro': '{$OTHER}',
                    'value': 'keep',
                    'description': '',
                    'type': 0,
                }
            ]
        )

        macros = {item['macro']: item for item in result}
        self.assertEqual(
            macros['{$NETBOX.URL}'],
            {
                'macro': '{$NETBOX.URL}',
                'value': 'https://manual.example/device/55/',
                'description': 'manual',
                'type': 0,
            },
        )
        self.assertEqual(macros['{$OTHER}']['value'], 'keep')

    def test_disabled_without_existing_macro_adds_nothing(self):
        sync = self.make_sync(enabled=False)
        sync.api.host.get.return_value = [{'hostid': '12345', 'macros': []}]

        result = sync._apply_netbox_link_macro([])

        self.assertEqual(result, [])

    def test_enabled_requires_base_url(self):
        sync = self.make_sync(enabled=True, base_url=None)

        with self.assertRaisesRegex(
            RuntimeError,
            'netbox_link.enabled is true but netbox_link.base_url is not configured',
        ):
            sync._apply_netbox_link_macro([])

    def test_disabled_macro_read_failure_does_not_break_sync(self):
        sync = self.make_sync(enabled=False)
        sync.api.host.get.side_effect = RuntimeError('Zabbix unavailable')

        result = sync._apply_netbox_link_macro(
            [
                {
                    'macro': '{$OTHER}',
                    'value': 'keep',
                    'description': '',
                    'type': 0,
                }
            ]
        )

        self.assertEqual(
            result,
            [
                {
                    'macro': '{$OTHER}',
                    'value': 'keep',
                    'description': '',
                    'type': 0,
                }
            ],
        )
