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


def _owner_tag_values(host):
    type_values = []
    id_values = []
    for tag in host.get("tags", []):
        tag_name = str(tag.get("tag", ""))
        value = str(tag.get("value", ""))
        if tag_name == OWNER_TYPE_TAG:
            type_values.append(value)
        elif tag_name == OWNER_ID_TAG:
            id_values.append(value)
    return type_values, id_values


def _owner_identity(host, expected_type, expected_id):
    type_values, id_values = _owner_tag_values(host)
    present = bool(type_values or id_values)
    matches = (
        len(type_values) == 1
        and len(id_values) == 1
        and type_values[0] == expected_type
        and id_values[0] == expected_id
    )
    return present, matches, type_values, id_values


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

    def guarded_create_host(self):
        """Create a host without ever adopting a foreign duplicate by name."""
        self._ensure_zbx_groups()

        try:
            object_id = self.try_create()
        except RuntimeError as err:
            if "Host with the same name" not in str(err):
                raise

            host_name = self.get_create_params().get("host")
            if not host_name:
                raise RuntimeError(
                    "HostSync: duplicate host name reported but create params "
                    "do not contain 'host'"
                ) from err

            existing = self.api_object().get(
                filter={"host": [host_name]},
                output=["hostid", "host", "name"],
                selectTags=["tag", "value"],
            )
            if len(existing) != 1:
                raise RuntimeError(
                    f"HostSync: duplicate name reported for '{host_name}', but "
                    f"expected one existing host and found {len(existing)}"
                ) from err

            host = existing[0]
            expected_type, expected_id = _owner_values(self.obj)
            _, owner_matches, type_values, id_values = _owner_identity(
                host,
                expected_type,
                expected_id,
            )
            if not owner_matches:
                raise RuntimeError(
                    f"HostSync refused to adopt duplicate host '{host_name}' "
                    f"(hostid={host.get('hostid')}): NbxSync ownership tags "
                    f"type={type_values!r} id={id_values!r}, expected "
                    f"type={expected_type!r} id={expected_id!r}"
                ) from err

            object_id = str(host["hostid"])
            self.sync_to_zabbix(object_id)
            return object_id

        if not object_id:
            raise RuntimeError("HostSync creation returned no ID")

        self.set_id(object_id)
        self.obj.save()
        self.obj.update_sync_info(success=True)
        return object_id

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
        expected_type, expected_id = _owner_values(self.obj)
        owner_tag_present, owner_matches, _, _ = _owner_identity(
            host,
            expected_type,
            expected_id,
        )

        identity_valid = owner_matches
        if not owner_tag_present:
            # Legacy untagged adoption is intentionally strict. Name alone can
            # collide and an IP alone can be reused, so require both name and an
            # endpoint to match before treating a hostid as ours.
            name_matches = str(host.get("host", "")) == _expected_host_key(self.obj)
            expected_ips, expected_dns = _expected_endpoints(self)
            actual_ips, actual_dns = _actual_endpoints(host)
            endpoint_matches = bool(
                expected_ips.intersection(actual_ips)
                or expected_dns.intersection(actual_dns)
            )
            identity_valid = name_matches and endpoint_matches

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
            # The guarded _create_host path will refuse to adopt an unowned
            # duplicate name, so recovery cannot silently hijack another host.
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
    HostSync._create_host = guarded_create_host
    HostSync.sync = guarded_host_sync
    HostInterfaceSync._set_interfaceid = guarded_set_interfaceid
    HostInterfaceSync.sync = guarded_interface_sync
    HostSync._identity_guard_installed = True
