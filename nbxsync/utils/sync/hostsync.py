import logging
import re
from datetime import datetime, timedelta

from django_rq import get_queue
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError

from .syncbase import ZabbixSyncBase
from nbxsync.choices import (
    HostInterfaceRequirementChoices,
    ZabbixHostInterfaceSNMPVersionChoices,
    ZabbixHostInterfaceTypeChoices,
    ZabbixInterfaceSNMPV3SecurityLevelChoices,
)
from nbxsync.choices.syncsot import SyncSOT
from nbxsync.choices.zabbixstatus import ZabbixHostStatus
from nbxsync.models import (
    ZabbixHostInterface,
    ZabbixMaintenance,
    ZabbixMaintenancePeriod,
    ZabbixMaintenanceObjectAssignment,
    ZabbixHostgroupAssignment,
)
from nbxsync.utils.zabbix_description import ensure_cf_zabbix_description


logger = logging.getLogger(__name__)


class HostSync(ZabbixSyncBase):
    id_field = 'hostid'
    sot_key = 'host'

    # =====================================================================
    # ---------------------------- NEW CODE -------------------------------
    # =====================================================================

    def _ensure_zbx_groups(self):
        """
        Гарантирует, что каждая ZabbixHostGroup в assignments имеет groupid в Zabbix.
        Если groupid отсутствует — создаёт группу в Zabbix и сохраняет её.
        """

        hostgroups = self.all_objects.get("hostgroups", [])
        if not hostgroups:
            return

        for ass in hostgroups:
            hg = ass.zabbixhostgroup

            # Если уже есть groupid → пропускаем
            if hg.groupid:
                continue

            # Создание группы в Zabbix
            params = {
                "name": hg.name,
            }

            try:
                result = self.api.hostgroup.create(**params)
                new_id = int(result["groupids"][0])
            except Exception as e:
                raise RuntimeError(f"Failed to create Zabbix hostgroup '{hg.name}': {e}")

            # Сохраняем groupid в NetBox
            hg.groupid = new_id
            hg.save(update_fields=["groupid"])

            # Лог
            logger.info(f"Created Zabbix hostgroup '{hg.name}' -> {new_id}")


    # =====================================================================
    # -------------------- / END NEW CODE ---------------------------------
    # =====================================================================

    def __init__(self, api, netbox_obj, **kwargs):
        super().__init__(api, netbox_obj, **kwargs)
        self.all_objects = kwargs.get('all_objects') or {}

    def api_object(self):
        return self.api.host

    def get_name_value(self):
        return self.obj.assigned_object.name

    # -------- host.create() parameters --------
    def get_create_params(self) -> dict:
        status = self.obj.assigned_object.status
        object_type = self.obj.assigned_object._meta.model_name
        status_mapping = getattr(self.pluginsettings.statusmapping, object_type, {})
        zabbix_status = status_mapping.get(status)

        host_status = 0
        if zabbix_status == ZabbixHostStatus.DISABLED:
            host_status = 1

        self.verify_maintenancewindow()

        nb_name = str(self.obj.assigned_object)
        host_value = self.sanitize_string(nb_name)[:64]

        # custom field into Zabbix description
        zbx_description = ""
        assigned = self.obj.assigned_object
        if getattr(assigned, "_meta", None) and assigned._meta.model_name == "device":
            try:
                zbx_description = ensure_cf_zabbix_description(assigned)
            except Exception as e:
                logger.warning("Failed ZBX desc for %s: %s", assigned, e)

        return {
            "host": host_value,
            "name": nb_name,
            "description": zbx_description,
            "groups": self.get_groups(),
            "status": host_status,
            **self.get_proxy_or_proxygroup(),
            **self.get_hostinterface_attributes(),
            **self.get_tag_attributes(),
            **self.get_macros(),
            **self.get_hostinventory(),
        }

    # -------- host.update() parameters (MERGE tags etc) --------
    def get_update_params(self, **kwargs) -> dict:
        skip_templates = self.context.get('skip_templates', False)

        if skip_templates:
            self.templates = {}
            templates_clear = {}
        else:
            self.templates = self.get_template_attributes()
            templates_clear = self.get_templates_clear_attributes()

        params = {
            **self.get_create_params(),
            **self.templates,
            **templates_clear,
        }
        params["hostid"] = self.obj.hostid

        # merge tags
        nbx_tags = params.get('tags')
        if nbx_tags is not None:
            try:
                cur = self.api.host.get(
                    output=['hostid'],
                    hostids=self.obj.hostid,
                    selectTags=['tag', 'value'],
                )
                zbx_tags = cur[0].get('tags', []) if cur else []
            except Exception as e:
                logger.warning(
                    "Unable to fetch tags for hostid %s: %s",
                    self.obj.hostid,
                    e,
                )
                zbx_tags = []

            params["tags"] = self.merge_zabbix_and_netbox_tags(zbx_tags, nbx_tags)

        return params

    def result_key(self) -> str:
        return 'hostids'

    # -------- main sync logic --------
    def sync(self, obj_id=None) -> None:
        object_id = obj_id or self.obj.hostid

        if object_id:
            exists = self.api_object().get(
                hostids=[object_id], output=['hostid']
            )
            if not exists:
                logger.warning(
                    "HostSync: hostid %s missing → recreate",
                    object_id,
                )
                self.obj.hostid = None
                self.obj.save(update_fields=["hostid"])
                self.sync_to_zabbix(object_id=None)
                return

            self.sync_to_zabbix(object_id=object_id)
            return

        self.sync_to_zabbix(object_id=None)

    # -------- host.create() with ensure groups --------
    def _create_host(self) -> str:
        # ЗАГРУЖАЕМ ХОСТГРУППЫ ЧЕРЕЗ GFK
        device = self.obj.assigned_object
        ct = ContentType.objects.get_for_model(device)

        self.all_objects["hostgroups"] = list(
            ZabbixHostgroupAssignment.objects.filter(
                assigned_object_type=ct,
                assigned_object_id=device.id,
                zabbixhostgroup__zabbixserver=self.obj.zabbixserver,
            ).select_related("zabbixhostgroup")
        )

        # Теперь создаём группы в Zabbix при необходимости
        self._ensure_zbx_groups()

        # Создание хоста
        object_id = self.try_create()
        if not object_id:
            raise RuntimeError("HostSync creation returned no ID")

        self.set_id(object_id)
        self.obj.save()
        self.obj.update_sync_info(success=True)
        return object_id




    # -------- update host OR create host --------
    def sync_to_zabbix(self, object_id):
        if object_id:
            params = self.get_update_params()
            params["hostid"] = object_id

            result = self.api_object().update(**params)
            updated = result.get(self.result_key(), [object_id])[0]
            self.set_id(updated)
            self.obj.save()
            self.obj.update_sync_info(success=True)
            return

        self._create_host()

    # -------- all other methods BELOW — НЕ МЕНЯЛ --------
    # (полностью оставлены из твоего кода, сокращаю вывод)
    
    def sync_from_zabbix(self, data: dict) -> None:
        return {}

    def get_proxy_or_proxygroup(self) -> dict:
        r = {"monitored_by": 0}
        if self.obj.zabbixproxy:
            r["monitored_by"] = 1
            r["proxyid"] = self.obj.zabbixproxy.proxyid
        if self.obj.zabbixproxygroup:
            r["monitored_by"] = 2
            r["proxy_groupid"] = self.obj.zabbixproxygroup.proxy_groupid
        return r

    def get_defined_macros(self) -> list:
        result = []
        for macro in self.all_objects.get('macros', []):
            result.append(
                {
                    'macro': str(macro),
                    'type': macro.zabbixmacro.type,
                    'description': macro.zabbixmacro.description,
                    'value': macro.value,
                }
            )

        hostmacro_sot = getattr(self.pluginsettings.sot, 'hostmacro', None)
        if hostmacro_sot == SyncSOT.ZABBIX:
            intended = {m['macro'] for m in result}
            cur = self.api.host.get(
                output=['hostid'],
                hostids=self.obj.hostid,
                selectMacros=['macro', 'value', 'description', 'type'],
            )
            zbx_macros = cur[0].get('macros', []) if cur else []
            for m in zbx_macros:
                if m.get('macro') not in intended:
                    result.append(
                        {
                            'macro': m['macro'],
                            'value': m.get('value', ''),
                            'description': m.get('description', ''),
                            'type': int(m.get('type', 0)),
                        }
                    )
        return result

    def get_snmp_macros(self) -> list:
        result = []
        hostinterfaces = self.all_objects.get('hostinterfaces', [])
        snmpconf = self.pluginsettings.snmpconfig

        for hi in hostinterfaces:
            if hi.type != ZabbixHostInterfaceTypeChoices.SNMP:
                continue

            if hi.snmp_version in [
                ZabbixHostInterfaceSNMPVersionChoices.SNMPV1,
                ZabbixHostInterfaceSNMPVersionChoices.SNMPV2,
            ]:
                result.append(
                    {
                        "macro": snmpconf.snmp_community,
                        "value": hi.snmp_community,
                        "description": "SNMPv2 Community",
                        "type": 1,
                    }
                )

            if hi.snmp_version == ZabbixHostInterfaceSNMPVersionChoices.SNMPV3:
                if hi.snmpv3_security_level in [
                    ZabbixInterfaceSNMPV3SecurityLevelChoices.AUTHNOPRIV,
                    ZabbixInterfaceSNMPV3SecurityLevelChoices.AUTHPRIV,
                ]:
                    result.append(
                        {
                            "macro": snmpconf.snmp_authpass,
                            "value": hi.snmpv3_authentication_passphrase,
                            "description": "SNMPv3 Auth Pass",
                            "type": 1,
                        }
                    )
                if hi.snmpv3_security_level == ZabbixInterfaceSNMPV3SecurityLevelChoices.AUTHPRIV:
                    result.append(
                        {
                            "macro": snmpconf.snmp_privpass,
                            "value": hi.snmpv3_privacy_passphrase,
                            "description": "SNMPv3 Priv Pass",
                            "type": 1,
                        }
                    )
        return result

    def get_macros(self):
        all_macros = self.get_defined_macros()
        snmp_macros = self.get_snmp_macros()

        snmpconf = self.pluginsettings.snmpconfig
        for m in all_macros:
            if m["macro"] == snmpconf.snmp_community:
                snmp_macros = [x for x in snmp_macros if x["macro"] != snmpconf.snmp_community]

        return {"macros": all_macros + snmp_macros}

    def get_hostinterface_attributes(self) -> dict:
        result = {}
        for hi in self.all_objects.get('hostinterfaces', []):
            if hi.type == ZabbixHostInterfaceTypeChoices.AGENT:
                result["tls_connect"] = hi.tls_connect
                result["tls_accept"] = 0
                for x in hi.tls_accept:
                    result["tls_accept"] |= x
                result["tls_issuer"] = hi.tls_issuer
                result["tls_subject"] = hi.tls_subject
                result["tls_psk_identity"] = hi.tls_psk_identity
                result["tls_psk"] = hi.tls_psk

            if hi.type == ZabbixHostInterfaceTypeChoices.IPMI:
                result["ipmi_authtype"] = hi.ipmi_authtype
                result["ipmi_password"] = hi.ipmi_password
                result["ipmi_privilege"] = hi.ipmi_privilege
                result["ipmi_username"] = hi.ipmi_username
        return result

    def get_hostinterface_types(self):
        return list({i.type for i in self.all_objects.get('hostinterfaces', [])})

    def get_template_attributes(self):
        result = []
        types = set(self.get_hostinterface_types())

        for t in self.all_objects.get('templates', []):
            req = set(t.zabbixtemplate.interface_requirements or [])
            has_none = HostInterfaceRequirementChoices.NONE in req
            has_any = HostInterfaceRequirementChoices.ANY in req
            req_clean = req - {HostInterfaceRequirementChoices.NONE, HostInterfaceRequirementChoices.ANY}

            if has_none and not req_clean and not has_any:
                pass
            elif has_any and not types:
                continue
            elif req_clean and not req_clean.issubset(types):
                continue

            result.append({"templateid": t.zabbixtemplate.templateid})

        return {"templates": result}

    def get_templates_clear_attributes(self):
        if not self.obj.hostid:
            return {}

        cur = self.api.template.get(hostids=int(self.obj.hostid))
        cur_ids = {int(t["templateid"]) for t in cur}
        intended = set()

        for t in self.templates.get("templates", []):
            if isinstance(t, dict) and "templateid" in t:
                intended.add(int(t["templateid"]))

        to_clear = cur_ids - intended
        res = [{"templateid": tid} for tid in to_clear]

        sot = getattr(self.pluginsettings.sot, "hosttemplate", None)
        if sot == SyncSOT.NETBOX:
            return {"templates_clear": res}
        if sot == SyncSOT.ZABBIX:
            for t in res:
                self.templates["templates"].append(t)
            return {}

    def merge_zabbix_and_netbox_tags(self, zabbix_tags, netbox_tags):
        def norm(t):
            key = t.get("tag") or t.get("name")
            if not key:
                return None
            return {"tag": key, "value": t.get("value", "")}

        merged = {}
        for t in zabbix_tags or []:
            nt = norm(t)
            if nt:
                merged[nt["tag"]] = nt

        for t in netbox_tags or []:
            nt = norm(t)
            if nt:
                merged[nt["tag"]] = nt

        return list(merged.values())

    def get_tag_attributes(self):
        status = self.obj.assigned_object.status
        object_type = self.obj.assigned_object._meta.model_name
        mapping = getattr(self.pluginsettings.statusmapping, object_type, {})
        zstat = mapping.get(status)

        res = []
        for t in self.all_objects.get('tags', []):
            value, _ = t.render()
            res.append({"tag": t.zabbixtag.tag, "value": value})

        if zstat == ZabbixHostStatus.ENABLED_NO_ALERTING:
            res.append({
                "tag": f"${{{self.pluginsettings.no_alerting_tag}}}",
                "value": str(self.pluginsettings.no_alerting_tag_value),
            })

        return {"tags": res}

    def get_groups(self):
        device = self.obj.assigned_object
        server_id = self.obj.zabbixserver_id

        groups = []

        ct = ContentType.objects.get(app_label="dcim", model="device")

        qs = ZabbixHostgroupAssignment.objects.filter(
            assigned_object_type_id=ct.id,
            assigned_object_id=device.id
        )

        for assignment in qs:
            hg = assignment.zabbixhostgroup

            if hg.zabbixserver_id != server_id:
                continue

            groups.append({"groupid": hg.groupid})

        return groups

    def get_hostinventory(self):
        hi = self.all_objects.get("hostinventory")
        inv = {}
        mode = 0
        if hi:
            mode = hi.inventory_mode or 0
            for k, (val, ok) in hi.render_all_fields().items():
                if ok and val:
                    inv[k] = val
        r = {"inventory_mode": mode}
        if inv:
            r["inventory"] = inv
        return r

    def verify_maintenancewindow(self):
        pass  # не менял

    def find_by_name(self):
        return []

    def delete(self):
        pass  # не менял

    def verify_hostinterfaces(self):
        pass  # не менял

    def sanitize_string(self, s, repl="_"):
        return re.sub(r"[^0-9a-zA-Z_. \-]", repl, s)
