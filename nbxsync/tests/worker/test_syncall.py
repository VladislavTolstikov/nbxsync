from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from zabbix_utils.exceptions import APIRequestError

from nbxsync.worker import syncall as worker


class SyncAllWorkerTests(SimpleTestCase):
    @patch('nbxsync.worker.syncall._collect_assignment_data')
    @patch('nbxsync.worker.syncall.ZabbixProxy.objects.filter')
    @patch('nbxsync.worker.syncall.ZabbixProxyGroup.objects.filter')
    @patch('nbxsync.worker.syncall.safe_sync')
    def test_stage_order(self, mock_safe_sync, mock_proxygroup_filter, mock_proxy_filter, mock_collect_data):
        assignment = MagicMock(hostid=1)
        hostgroup = MagicMock(pk=1)
        hostinterface = MagicMock()

        mock_collect_data.return_value = [
            (assignment, {'hostgroups': [hostgroup], 'hostinterfaces': [hostinterface]})
        ]
        mock_proxygroup_filter.return_value = [MagicMock()]
        mock_proxy_filter.return_value = [MagicMock()]

        worker.syncall(MagicMock())

        called_classes = [call.args[0] for call in mock_safe_sync.call_args_list]
        self.assertEqual(
            called_classes,
            [
                worker.HostSync,
                worker.ProxyGroupSync,
                worker.ProxySync,
                worker.HostGroupSync,
                worker.HostInterfaceSync,
                worker.HostSync,
            ],
        )

    @patch('nbxsync.worker.syncall._collect_assignment_data')
    @patch('nbxsync.worker.syncall.safe_sync')
    def test_idempotent_on_already_exists(self, mock_safe_sync, mock_collect_data):
        assignment = MagicMock(hostid=1)
        hostgroup = MagicMock(pk=1)
        hostinterface = MagicMock()

        mock_collect_data.return_value = [
            (assignment, {'hostgroups': [hostgroup], 'hostinterfaces': [hostinterface]})
        ]


        def side_effect(sync_class, *args, **kwargs):
            if sync_class is worker.HostSync and not kwargs.get('extra_args', {}).get('skip_templates'):
                raise APIRequestError({'data': 'already exists'})

        mock_safe_sync.side_effect = side_effect

        worker.syncall(MagicMock())

        hostsync_calls = [call for call in mock_safe_sync.call_args_list if call.args[0] is worker.HostSync]
        self.assertEqual(len(hostsync_calls), 2)
