from ipam.models import IPAddress
from nbxsync.models import ZabbixServerAssignment

from .syncbase import ZabbixSyncBase


class HostInterfaceSync(ZabbixSyncBase):
    id_field = 'interfaceid'
    sot_key = 'hostinterface'

    def api_object(self):
        return self.api.hostinterface

    def get_name_value(self):
        return self.obj.assigned_object.name

    def get_create_params(self) -> dict:
        hostid = self.context.get('hostid', None)
        zbxserverassignment = None

        if not hostid:
            # No HostID, get it from the assignment
            zbxserverassignment = ZabbixServerAssignment.objects.filter(assigned_object_type=self.obj.assigned_object_type, assigned_object_id=self.obj.assigned_object.id).first()
            # If the assignment isnt found... Return
            if not zbxserverassignment:
                return {}

            # Update the hostid field :)
            hostid = zbxserverassignment.hostid

        ipaddr = ''
        if self.obj.ip_id:
            ipaddr = IPAddress.objects.get(id=self.obj.ip_id).address.ip

        result = {
            'hostid': hostid,
            'type': self.obj.type,
            'ip': str(ipaddr),
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
                    snmp_comm_macro = getattr(self.pluginsettings.snmpconfig, 'snmp_comm', '{$SNMP_COMMUNITY}')
                    snmp_dict['community'] = snmp_comm_macro

            if self.obj.snmp_version == 3:
                snmp_authpass_macro = getattr(self.pluginsettings.snmpconfig, 'snmp_authpass', '{$SNMPV3_AUTHPASS}')
                snmp_privpass_macro = getattr(self.pluginsettings.snmpconfig, 'snmp_privpass', '{$SNMPV3_PRIVPASS}')

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
        params['interfaceid'] = self.obj.interfaceid
        return params

    def result_key(self) -> str:
        return 'interfaceids'

    def sync_from_zabbix(self, data: dict) -> None:
        try:
            self.obj.interfaceid = int(data['interfaceid'])
            self.obj.type = int(data.get('type', self.obj.type))
            self.obj.useip = int(data.get('useip', self.obj.useip))
            self.obj.interface_type = int(data.get('main', self.obj.interface_type))  # 'main' indicates default interface
            self.obj.dns = data.get('dns', '')
            self.obj.port = int(data.get('port')) if data.get('port') else None

            ip = data.get('ip')
            if ip:
                from ipam.models import IPAddress

                ip_obj = IPAddress.objects.filter(address__net_host=ip).first()
                self.obj.ip = ip_obj

            # SNMP handling
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

                    # Optional passphrases are Zabbix macros, don't overwrite them unless required
                    # self.obj.snmpv3_authentication_passphrase = snmp_data.get('authpassphrase', '')
                    # self.obj.snmpv3_privacy_passphrase = snmp_data.get('privpassphrase', '')

            self.obj.save()
            self.obj.update_sync_info(success=True, message='')

        except Exception as err:
            self.obj.update_sync_info(success=False, message=str(err))
    def sync(self, obj_id=None):
        """
        Override sync to auto-recreate interface if Zabbix returns
        'Cannot switch host for interface' or interface does not exist.
        """
        # Если interfaceid есть — проверяем, существует ли он в Zabbix
        if self.obj.interfaceid:
            try:
                found = self.api.hostinterface.get(interfaceids=self.obj.interfaceid)
                if not found:
                    # интерфейса нет → пересоздаём
                    self.obj.interfaceid = None
                    self.obj.save(update_fields=["interfaceid"])
            except Exception:
                # Любая ошибка API → сбрасываем interfaceid
                self.obj.interfaceid = None
                self.obj.save(update_fields=["interfaceid"])

        # Если interfaceid отсутствует → create
        if not self.obj.interfaceid:
            params = self.get_create_params()
            try:
                result = self.api.hostinterface.create(**params)
                new_id = int(result["interfaceids"][0])
                self.obj.interfaceid = new_id
                self.obj.save(update_fields=["interfaceid"])
                self.obj.update_sync_info(success=True)
                return
            except Exception as e:
                self.obj.update_sync_info(success=False, message=str(e))
                return

        # Иначе делаем update (стандартный путь)
        try:
            params = self.get_update_params()
            self.api.hostinterface.update(**params)
            self.obj.update_sync_info(success=True)
        except Exception as e:
            msg = str(e)
            if "Cannot switch host for interface" in msg:
                # Zabbix запрещает update → recreate
                self.obj.interfaceid = None
                self.obj.save(update_fields=["interfaceid"])
                params = self.get_create_params()
                try:
                    result = self.api.hostinterface.create(**params)
                    new_id = int(result["interfaceids"][0])
                    self.obj.interfaceid = new_id
                    self.obj.save(update_fields=["interfaceid"])
                    self.obj.update_sync_info(success=True, message="Recreated interface")
                    return
                except Exception as e2:
                    self.obj.update_sync_info(success=False, message=str(e2))
                    return
            else:
                self.obj.update_sync_info(success=False, message=msg)
                raise

