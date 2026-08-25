import logging

from ipam.models import IPAddress
from nbxsync.models import ZabbixServerAssignment

from .syncbase import ZabbixSyncBase


logger = logging.getLogger(__name__)


class HostInterfaceSync(ZabbixSyncBase):
    id_field = 'interfaceid'
    sot_key = 'hostinterface'

    def api_object(self):
        return self.api.hostinterface

    def get_name_value(self):
        return self.obj.assigned_object.name

    def _get_hostid(self):
        hostid = self.context.get('hostid')
        if hostid:
            return hostid

        assignment = ZabbixServerAssignment.objects.filter(
            assigned_object_type=self.obj.assigned_object_type,
            assigned_object_id=self.obj.assigned_object_id,
            zabbixserver=self.obj.zabbixserver,
        ).first()

        return assignment.hostid if assignment else None

    def _get_ipaddr(self):
        if not self.obj.ip_id:
            return ''

        ip_obj = getattr(self.obj, 'ip', None)
        if ip_obj is None:
            ip_obj = IPAddress.objects.get(id=self.obj.ip_id)

        # NetBox 4.5 exposes IPAddress.address as a string. Older releases may
        # return a netaddr object; converting to str works for both.
        return str(ip_obj.address).split('/', 1)[0]

    def get_create_params(self) -> dict:
        hostid = self._get_hostid()
        if not hostid:
            return {}

        result = {
            'hostid': hostid,
            'type': self.obj.type,
            'ip': self._get_ipaddr(),
            'dns': self.obj.dns,
            'port': str(self.obj.port),
            'useip': self.obj.useip,
            'main': 1 if int(self.obj.interface_type or 0) == 1 else 0,
        }

        if self.obj.type == 2:  # SNMP
            snmp_dict = {
                'version': self.obj.snmp_version,
                'bulk': 1 if self.obj.snmp_usebulk else 0,
            }

            if self.obj.snmp_version in [1, 2]:
                if self.obj.snmp_community:
                    snmp_dict['community'] = self.obj.snmp_community
                else:
                    snmp_comm_macro = getattr(
                        self.pluginsettings.snmpconfig,
                        'snmp_comm',
                        '{$SNMP_COMMUNITY}',
                    )
                    snmp_dict['community'] = snmp_comm_macro

            if self.obj.snmp_version == 3:
                snmp_authpass_macro = getattr(
                    self.pluginsettings.snmpconfig,
                    'snmp_authpass',
                    '{$SNMPV3_AUTHPASS}',
                )
                snmp_privpass_macro = getattr(
                    self.pluginsettings.snmpconfig,
                    'snmp_privpass',
                    '{$SNMPV3_PRIVPASS}',
                )

                snmp_dict['contextname'] = self.obj.snmpv3_context_name
                snmp_dict['securityname'] = self.obj.snmpv3_security_name
                snmp_dict['securitylevel'] = self.obj.snmpv3_security_level
                snmp_dict['authpassphrase'] = snmp_authpass_macro
                snmp_dict['privpassphrase'] = snmp_privpass_macro
                snmp_dict['authprotocol'] = self.obj.snmpv3_authentication_protocol
                snmp_dict['privprotocol'] = self.obj.snmpv3_privacy_protocol

            result['details'] = snmp_dict

        return result

    def get_update_params(self, **kwargs) -> dict:
        params = self.get_create_params()
        params.pop('hostid', None)
        params['interfaceid'] = self.obj.interfaceid
        return params

    def result_key(self) -> str:
        return 'interfaceids'

    @staticmethod
    def _int_value(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _snmp_details_match(self, interface, params):
        actual = interface.get('details') or {}
        expected = params.get('details') or {}

        version = self._int_value(expected.get('version'))
        if self._int_value(actual.get('version')) != version:
            return False

        if self._int_value(actual.get('bulk'), 1) != self._int_value(expected.get('bulk'), 1):
            return False

        if version in (1, 2):
            return str(actual.get('community', '')) == str(expected.get('community', ''))

        if version == 3:
            for key in ('contextname', 'securityname'):
                if str(actual.get(key, '')) != str(expected.get(key, '')):
                    return False

            for key in ('securitylevel', 'authprotocol', 'privprotocol'):
                if self._int_value(actual.get(key)) != self._int_value(expected.get(key)):
                    return False

        return True

    def _exact_match(self, interface, params):
        if self._int_value(interface.get('type')) != self._int_value(params.get('type')):
            return False

        if self._int_value(interface.get('main')) != self._int_value(params.get('main')):
            return False

        if str(interface.get('port', '')) != str(params.get('port', '')):
            return False

        if self._int_value(interface.get('useip'), 1) != self._int_value(params.get('useip'), 1):
            return False

        if self._int_value(params.get('useip'), 1) == 1:
            if interface.get('ip') != params.get('ip'):
                return False
        elif interface.get('dns') != params.get('dns'):
            return False

        if self._int_value(params.get('type')) == 2:
            return self._snmp_details_match(interface, params)

        return True

    def _find_existing_interface(self, hostid):
        params = self.get_create_params()
        if not params:
            return None

        expected_type = int(params['type'])
        expected_main = int(params['main'])

        interfaces = self.api.hostinterface.get(
            hostids=hostid,
            output='extend',
            selectDetails='extend',
        )

        same_type_main = [i for i in interfaces if self._int_value(i.get('type')) == expected_type and self._int_value(i.get('main')) == expected_main]

        # Zabbix permits only one default interface of each type. If the
        # NetBox object represents that default interface, adopt it even when
        # its endpoint/details need to be updated.
        if expected_main == 1 and len(same_type_main) == 1:
            return same_type_main[0]

        # Non-default interfaces may share the same IP/port. Their SNMP
        # details (especially community/context) must therefore be part of
        # the identity check or one interface can overwrite another.
        for interface in same_type_main:
            if self._exact_match(interface, params):
                return interface

        return None

    def _set_interfaceid(self, interfaceid):
        self.obj.interfaceid = int(interfaceid)
        self.obj.save(update_fields=['interfaceid'])

    def _update_existing(self, message=''):
        params = self.get_update_params()
        if not params.get('interfaceid'):
            self.obj.update_sync_info(
                success=False,
                message='HostInterfaceSync update failed: interfaceid is empty',
            )
            return

        self.api.hostinterface.update(**params)
        self.obj.update_sync_info(success=True, message=message)

    def _create_interface(self):
        params = self.get_create_params()
        if not params:
            self.obj.update_sync_info(
                success=False,
                message='HostInterfaceSync create failed: hostid not found',
            )
            return

        result = self.api.hostinterface.create(**params)
        new_id = int(result['interfaceids'][0])
        self._set_interfaceid(new_id)
        self.obj.update_sync_info(success=True, message='Created interface')

    def _adopt_and_update(self, hostid, message='Adopted existing interface and updated'):
        existing = self._find_existing_interface(hostid)
        if not existing:
            return False

        self._set_interfaceid(existing['interfaceid'])
        self._update_existing(message=message)
        return True

    def sync_from_zabbix(self, data: dict) -> None:
        try:
            self.obj.interfaceid = int(data['interfaceid'])
            self.obj.type = int(data.get('type', self.obj.type))
            self.obj.useip = int(data.get('useip', self.obj.useip))
            self.obj.interface_type = int(data.get('main', self.obj.interface_type))
            self.obj.dns = data.get('dns', '')
            self.obj.port = int(data.get('port')) if data.get('port') else None

            ip = data.get('ip')
            if ip:
                ip_obj = IPAddress.objects.filter(address__net_host=ip).first()
                self.obj.ip = ip_obj

            snmp_data = data.get('details', {})
            if self.obj.type == 2:  # SNMP
                self.obj.snmp_version = snmp_data.get('version', self.obj.snmp_version)
                self.obj.snmp_usebulk = snmp_data.get('bulk', 1) == 1

                if self.obj.snmp_version in [1, 2]:
                    self.obj.snmp_community = snmp_data.get('community', '')

                elif self.obj.snmp_version == 3:
                    self.obj.snmpv3_context_name = snmp_data.get('contextname', '')
                    self.obj.snmpv3_security_name = snmp_data.get('securityname', '')
                    self.obj.snmpv3_security_level = snmp_data.get('securitylevel')
                    self.obj.snmpv3_authentication_protocol = snmp_data.get('authprotocol')
                    self.obj.snmpv3_privacy_protocol = snmp_data.get('privprotocol')

            self.obj.save()
            self.obj.update_sync_info(success=True, message='')

        except Exception as err:
            self.obj.update_sync_info(success=False, message=str(err))

    def sync(self, obj_id=None):
        hostid = self._get_hostid()
        if not hostid:
            self.obj.update_sync_info(
                success=False,
                message='HostInterfaceSync failed: hostid not found',
            )
            return

        if self.obj.interfaceid:
            try:
                found = self.api.hostinterface.get(
                    interfaceids=self.obj.interfaceid,
                    output='extend',
                    selectDetails='extend',
                )
            except Exception as err:
                # Важно: не сбрасываем interfaceid при временной ошибке API.
                self.obj.update_sync_info(
                    success=False,
                    message=f'HostInterfaceSync failed to verify interfaceid {self.obj.interfaceid}: {err}',
                )
                return

            if found:
                zbx_interface = found[0]

                if str(zbx_interface.get('hostid')) == str(hostid):
                    try:
                        self._update_existing()
                    except Exception as err:
                        self.obj.update_sync_info(success=False, message=str(err))
                    return

                # interfaceid указывает на интерфейс другого хоста.
                # Это битая привязка: очищаем и ниже пробуем adopt/create.
                self.obj.interfaceid = None
                self.obj.save(update_fields=['interfaceid'])
            else:
                # ID реально отсутствует в Zabbix: очищаем и ниже пробуем adopt/create.
                self.obj.interfaceid = None
                self.obj.save(update_fields=['interfaceid'])

        if not self.obj.interfaceid:
            try:
                if self._adopt_and_update(hostid):
                    return
            except Exception as err:
                self.obj.update_sync_info(
                    success=False,
                    message=f'HostInterfaceSync failed to adopt existing interface: {err}',
                )
                return

        if not self.obj.interfaceid:
            try:
                self._create_interface()
                return
            except Exception as err:
                msg = str(err)

                if 'more than one default interface of the same type' in msg:
                    try:
                        if self._adopt_and_update(
                            hostid,
                            message='Adopted existing default interface and updated',
                        ):
                            return
                    except Exception as adopt_err:
                        self.obj.update_sync_info(success=False, message=str(adopt_err))
                        return

                self.obj.update_sync_info(success=False, message=msg)
                return
