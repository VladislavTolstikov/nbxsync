import logging
import re

from django.db import connection, transaction

from nbxsync.models import ZabbixHostInterface, ZabbixServerAssignment


logger = logging.getLogger(__name__)

OWNER_TYPE_TAG = "nbxsync.object_type_id"
OWNER_ID_TAG = "nbxsync.object_id"


def _owner_values(obj):
    return str(obj.assigned_object_type_id), str(obj.assigned_object_id)


def _expected_host_key(obj):
    name = str(obj.assigned_object)
    return re.sub(r"[^0-9a-zA-Z_. \-]", "_", name)[:64]


def _tag_map(host):
    return {
        str(tag.get("tag")): str(tag.get("value", ""))
        for tag in host.get("tags", [])
        if tag.get("tag")
    }


def _expected_endpoints(host_sync):
    ips = set()
    dns_names = set()

    for interface in host_sync.all_objects.get("hostinterfaces", []):
        if int(interface.useip) == 1 and interface.ip_id:
            ips.add(str(interface.ip.address.ip))
        elif interface.dns:
            dns_names.add(str(interface.dns))

    return ips, dns_names


def _actual_endpoints(host):
    ips = set()
    dns_names = set()

    for interface in host.get("interfaces", []):
        if int(interface.get("useip", 1)) == 1 and interface.get("ip"):
            ips.add(str(interface["ip"]))
        elif interface.get("dns"):
            dns_names.add(str(interface["dns"]))

    return ips, dns_names


def _clear_object_interface_ids(obj):
    ZabbixHostInterface.objects.filter(
        zabbixserver_id=obj.zabbixserver_id,
        assigned_object_type_id=obj.assigned_object_type_id,
        assigned_object_id=obj.assigned_object_id,
    ).update(interfaceid=None)


def install(HostSync, HostInterfaceSync):
    if getattr(HostSync, "_identity_guard_installed", False):
        return

    original_host_sync = HostSync.sync
    original_host_set_id = HostSync.set_id
    original_get_tag_attributes = HostSync.get_tag_attributes
    original_interface_sync = HostInterfaceSync.sync
    original_set_interfaceid = HostInterfaceSync._set_interfaceid

    def guarded_get_tag_attributes(self):
        result = original_get_tag_attributes(self)
        tags = result.setdefault("tags", [])
        owner_type, owner_id = _owner_values(self.obj)

        tags = [
            tag for tag in tags
            if tag.get("tag") not in {OWNER_TYPE_TAG, OWNER_ID_TAG}
        ]
        tags.extend([
            {"tag": OWNER_TYPE_TAG, "value": owner_type},
            {"tag": OWNER_ID_TAG, "value": owner_id},
        ])
        result["tags"] = tags
        return result

    def guarded_host_set_id(self, value):
        if value is None:
            return original_host_set_id(self, value)

        hostid = int(value)
        server_id = int(self.obj.zabbixserver_id)

        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s, %s)",
                    [server_id, hostid],
                )

            duplicate = (
                ZabbixServerAssignment.objects
                .filter(zabbixserver_id=server_id, hostid=hostid)
                .exclude(pk=self.obj.pk)
                .first()
            )
            if duplicate:
                raise RuntimeError(
                    f"HostSync refused hostid {hostid}: already assigned to "
                    f"{duplicate.assigned_object} (assignment {duplicate.pk})"
                )

            return original_host_set_id(self, hostid)

    def guarded_host_sync(self, obj_id=None):
        object_id = obj_id or self.obj.hostid
        if not object_id:
            return original_host_sync(self, obj_id=obj_id)

        found = self.api.host.get(
            hostids=[object_id],
            output=["hostid", "host", "name"],
            selectTags=["tag", "value"],
            selectInterfaces=["interfaceid", "ip", "dns", "useip"],
        )
        if not found:
            return original_host_sync(self, obj_id=obj_id)

        host = found[0]
        tags = _tag_map(host)
        expected_type, expected_id = _owner_values(self.obj)
        owner_tag_present = OWNER_TYPE_TAG in tags or OWNER_ID_TAG in tags
        owner_matches = (
            tags.get(OWNER_TYPE_TAG) == expected_type
            and tags.get(OWNER_ID_TAG) == expected_id
        )

        identity_valid = owner_matches
        if not owner_tag_present:
            name_matches = str(host.get("host", "")) == _expected_host_key(self.obj)
            expected_ips, expected_dns = _expected_endpoints(self)
            actual_ips, actual_dns = _actual_endpoints(host)
            endpoint_matches = bool(
                expected_ips.intersection(actual_ips)
                or expected_dns.intersection(actual_dns)
            )
            identity_valid = name_matches or endpoint_matches

        if not identity_valid:
            logger.error(
                "HostSync refused foreign hostid %s for %s; Zabbix host is %s",
                object_id,
                self.obj.assigned_object,
                host.get("host"),
            )
            self.obj.hostid = None
            self.obj.save(update_fields=["hostid"])
            _clear_object_interface_ids(self.obj)
            return original_host_sync(self, obj_id=None)

        return original_host_sync(self, obj_id=object_id)

    def guarded_set_interfaceid(self, interfaceid):
        interfaceid = int(interfaceid)
        hostid = self._get_hostid()
        if not hostid:
            raise RuntimeError(
                f"HostInterfaceSync refused interfaceid {interfaceid}: hostid is empty"
            )

        found = self.api.hostinterface.get(
            interfaceids=interfaceid,
            output=["interfaceid", "hostid"],
        )
        if not found:
            raise RuntimeError(
                f"HostInterfaceSync refused interfaceid {interfaceid}: not found in Zabbix"
            )
        if str(found[0].get("hostid")) != str(hostid):
            raise RuntimeError(
                f"HostInterfaceSync refused interfaceid {interfaceid}: belongs to "
                f"hostid {found[0].get('hostid')}, expected {hostid}"
            )

        server_id = int(self.obj.zabbixserver_id)
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s, %s)",
                    [server_id, interfaceid],
                )

            duplicates = list(
                ZabbixHostInterface.objects
                .filter(zabbixserver_id=server_id, interfaceid=interfaceid)
                .exclude(pk=self.obj.pk)
            )
            for duplicate in duplicates:
                assignment = ZabbixServerAssignment.objects.filter(
                    zabbixserver_id=server_id,
                    assigned_object_type_id=duplicate.assigned_object_type_id,
                    assigned_object_id=duplicate.assigned_object_id,
                ).first()
                if assignment and str(assignment.hostid) == str(hostid):
                    raise RuntimeError(
                        f"HostInterfaceSync refused interfaceid {interfaceid}: already "
                        f"assigned to {duplicate.assigned_object} (record {duplicate.pk})"
                    )

                logger.warning(
                    "Clearing stale interfaceid %s from %s (record %s)",
                    interfaceid,
                    duplicate.assigned_object,
                    duplicate.pk,
                )
                duplicate.interfaceid = None
                duplicate.save(update_fields=["interfaceid"])

            return original_set_interfaceid(self, interfaceid)

    def guarded_interface_sync(self, obj_id=None):
        if self.obj.interfaceid:
            try:
                guarded_set_interfaceid(self, self.obj.interfaceid)
            except Exception as err:
                self.obj.update_sync_info(success=False, message=str(err))
                return

        return original_interface_sync(self, obj_id=obj_id)

    HostSync.get_tag_attributes = guarded_get_tag_attributes
    HostSync.set_id = guarded_host_set_id
    HostSync.sync = guarded_host_sync
    HostInterfaceSync._set_interfaceid = guarded_set_interfaceid
    HostInterfaceSync.sync = guarded_interface_sync
    HostSync._identity_guard_installed = True
