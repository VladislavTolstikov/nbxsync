from unittest.mock import MagicMock, patch

import pytest
from zabbix_utils.exceptions import APIRequestError

from dcim.models import Device
from django.contrib.contenttypes.models import ContentType
from utilities.testing import create_test_device

from nbxsync.models import ZabbixHostgroup, ZabbixHostgroupAssignment, ZabbixServer
from nbxsync.utils.sync.hostgroupsync import HostGroupSync
from nbxsync.utils.sync.safe_sync import safe_sync


class DummyHostgroupAPI:
    def __init__(self):
        self.updated = None
        self.hostgroup = self

    def get(self, search=None, **kwargs):
        name = (search or {}).get('name', 'Existing Group')
        return [{'groupid': 123, 'name': name}]

    def update(self, **params):
        self.updated = params
        return {'groupids': [params.get('groupid')]}


@pytest.mark.django_db
@patch('nbxsync.utils.sync.safe_sync.run_zabbix_operation')
@patch('nbxsync.utils.sync.safe_sync.ZabbixConnection')
@patch('nbxsync.utils.sync.safe_sync.ZabbixServer')
def test_safe_sync_reuses_existing_hostgroup(mock_server, mock_connection, mock_run_op):
    mock_run_op.side_effect = APIRequestError({'message': 'Application error', 'data': 'Host group already exists'})

    server = ZabbixServer.objects.create(name='Server', url='http://example', token='abc123')
    device = create_test_device(name='host-1')
    assignment = ZabbixHostgroupAssignment.objects.create(
        zabbixhostgroup=ZabbixHostgroup.objects.create(
            name='Existing Group',
            value='Existing Group',
            zabbixserver=server,
        ),
        assigned_object_type=ContentType.objects.get_for_model(Device),
        assigned_object_id=device.id,
    )

    dummy_api = DummyHostgroupAPI()
    mock_connection.return_value.__enter__.return_value = dummy_api
    mock_server.objects.get.return_value = server

    result = safe_sync(HostGroupSync, assignment, extra_args={'foo': 'bar'})

    mock_run_op.assert_called_once_with(HostGroupSync, assignment, 'sync', {'foo': 'bar'})
    assignment.refresh_from_db()
    assert assignment.zabbixhostgroup.groupid == 123
    assert dummy_api.updated['groupid'] == 123
    assert result['groupid'] == 123


@pytest.mark.django_db
@patch('nbxsync.utils.sync.safe_sync.run_zabbix_operation', side_effect=APIRequestError({'data': 'Something else'}))
def test_safe_sync_raises_non_duplicate_errors(mock_run_op):
    server = ZabbixServer.objects.create(name='Server', url='http://example', token='abc123')
    device = create_test_device(name='host-2')
    assignment = ZabbixHostgroupAssignment.objects.create(
        zabbixhostgroup=ZabbixHostgroup.objects.create(
            name='Another Group',
            value='Another Group',
            zabbixserver=server,
        ),
        assigned_object_type=ContentType.objects.get_for_model(Device),
        assigned_object_id=device.id,
    )

    with pytest.raises(RuntimeError):
        safe_sync(HostGroupSync, assignment)


@patch('nbxsync.utils.sync.safe_sync.run_zabbix_operation')
def test_safe_sync_passes_through_on_success(mock_run_op):
    obj = MagicMock()
    mock_run_op.return_value = {'ok': True}

    result = safe_sync(HostGroupSync, obj, extra_args={'foo': 'bar'})

    mock_run_op.assert_called_once_with(HostGroupSync, obj, 'sync', {'foo': 'bar'})
    assert result == {'ok': True}
