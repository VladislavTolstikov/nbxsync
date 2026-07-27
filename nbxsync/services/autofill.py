from __future__ import annotations

from dataclasses import dataclass, field
import re

from django.contrib.contenttypes.models import ContentType

from nbxsync.choices import (
    ZabbixHostInterfaceSNMPVersionChoices,
    ZabbixHostInterfaceTypeChoices,
    ZabbixHostInventoryModeChoices,
    ZabbixInterfaceUseChoices,
)
from nbxsync.models import (
    ZabbixHostgroup,
    ZabbixHostgroupAssignment,
    ZabbixHostInterface,
    ZabbixHostInventory,
    ZabbixProxy,
    ZabbixServer,
    ZabbixServerAssignment,
    ZabbixTemplate,
    ZabbixTemplateAssignment,
)


class AutofillError(Exception):
    pass


@dataclass(frozen=True)
class Target:
    server_id: int
    proxy_id: int | None


@dataclass(frozen=True)
class Rule:
    templates: tuple[str, ...]
    interface_type: int | None = None
    port: int | None = None
    snmp_version: int | None = None
    source: str = ''


@dataclass
class AutofillResult:
    server_assignments_created: int = 0
    server_assignments_updated: int = 0
    hostgroups_created: int = 0
    hostgroup_assignments_created: int = 0
    interfaces_created: int = 0
    template_assignments_created: int = 0
    inventory_created: int = 0
    inventory_updated: int = 0
    warnings: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f'servers +{self.server_assignments_created}/~{self.server_assignments_updated}; '
            f'groups +{self.hostgroups_created}; group assignments +{self.hostgroup_assignments_created}; '
            f'interfaces +{self.interfaces_created}; templates +{self.template_assignments_created}; '
            f'inventory +{self.inventory_created}/~{self.inventory_updated}'
        )


SERVER_SITE_MAP = {
    'MIUSSI': 'MK', 'MK': 'MK',
    'TUSHINO': 'TK', 'TK': 'TK',
    'STUDGORODOK': 'TK', 'SG': 'TK',
    'NOVOMOSKOVSK': 'NI', 'NI': 'NI',
    'TIRHTU': 'TZ', 'TARAZ': 'TZ', 'TZ': 'TZ',
}
GROUP_SITE_MAP = {**SERVER_SITE_MAP, 'STUDGORODOK': 'SG', 'SG': 'SG'}
TARGETS_BY_SITE = {
    'MK': (Target(1, None),),
    'TK': (Target(1, 1), Target(2, 4)),
    'NI': (Target(1, 2), Target(3, 5)),
    'TZ': (Target(1, 3), Target(4, 6)),
}
ROLE_TYPE_MAP = {
    'aggregation-switchboard': 'NET', 'c2000_controllers': 'ENG',
    'cn': 'SRV', 'hpcn': 'SRV', 'csw': 'NET', 'rtr': 'NET',
    'sw-fc': 'NET', 'iot': 'ENG', 'end-device': 'NET',
    'wifi-bridge': 'NET', 'cam': 'HID', 'safety': 'ENG',
    'skud': 'ENG', 'nas': 'SRV', 'rtp': 'SRV', 'sw-l2-zk': 'NET',
    'td': 'SRV', 'srv-dvr': 'SRV', 'netping': 'SRV', 'ip-ph': 'NET',
    'ap': 'NET', 'sw-l2': 'NET', 'switches': 'NET', 'backup': 'SRV',
    'pdu': 'ENG', 'fw': 'NET', 'engineering-systems': 'ENG',
    'server': 'SRV', 'guard_data': 'SRV', 'hvac': 'ENG',
    'san': 'SRV', 'srv-oth': 'SRV', 'mfu': 'HID', 'power': 'ENG',
}
PASSIVE_ROLES = {
    'org', 'patch-panel', 'ethernet-plug', 'crs', 'mediaconverter',
    'poe-injector', 'hardwired-device', 'usb', 'mm-project', 'nbk',
    'sw-l3n', 'psw', 'telephoniya',
}
MANUAL_ROLES = {'wrk', 'server', 'nas', 'san', 'srv-dvr', 'rtp', 'skud', 'guard_data'}
NETWORK_ROLES = {'sw-l2', 'sw-l2-zk', 'switches', 'aggregation-switchboard', 'csw', 'rtr', 'fw'}
BMC_ROLES = {'cn', 'hpcn', 'backup'}
ICMP_TEMPLATE = 'Template Module ICMP Ping'
MFU_TEMPLATE_BY_MODEL = {
    '287': 'Konica Minolta 287 by SNMP',
    'c250i': 'Konica Minolta c250i by SNMP',
    'c227': 'Konica Minolta c227 by SNMP',
    '300i': 'Konica Minolta 300i by SNMP',
    'c300i': 'Konica Minolta c300i by SNMP',
    'c287': 'Konica Minolta c287 by SNMP',
    '226': 'Konica Minolta 226 by SNMP',
    'ineo 452': 'Konica Minolta ineo 452 by SNMP',
}


def _norm(value) -> str:
    return re.sub(r'\s+', ' ', str(value or '').strip()).casefold()


def _role(device) -> str:
    return _norm(getattr(getattr(device, 'role', None), 'slug', ''))


def _manufacturer_model(device) -> tuple[str, str, str, str]:
    device_type = getattr(device, 'device_type', None)
    manufacturer = getattr(device_type, 'manufacturer', None)
    manufacturer_name = str(getattr(manufacturer, 'name', '') or '').strip()
    model_name = str(getattr(device_type, 'model', '') or '').strip()
    return _norm(manufacturer_name), _norm(model_name), manufacturer_name, model_name


def _site_key(device) -> str:
    site = getattr(device, 'site', None)
    for value in (getattr(site, 'slug', ''), getattr(site, 'name', '')):
        key = re.sub(r'[^A-Z0-9]', '', str(value or '').upper())
        if key in SERVER_SITE_MAP:
            return key
    raise AutofillError(f'Unsupported site: {site or "not set"}')


def _snmp(template: str, version=ZabbixHostInterfaceSNMPVersionChoices.SNMPV2, source='') -> Rule:
    return Rule((template,), ZabbixHostInterfaceTypeChoices.SNMP, 161, version, source)


def select_rule(device) -> Rule | None:
    role = _role(device)
    manufacturer, model, _, _ = _manufacturer_model(device)

    if role in PASSIVE_ROLES or role in MANUAL_ROLES:
        return None
    if role == 'ap':
        return _snmp('Template WiFi AP SNMP', source='role:ap')
    if role == 'cam':
        return _snmp('Network Generic Device by SNMP', source='role:cam')
    if role in BMC_ROLES:
        return Rule(("Chassis by IPMI", ICMP_TEMPLATE), ZabbixHostInterfaceTypeChoices.IPMI, 623, source=f'role:{role}')
    if role in {'c2000_controllers', 'ip-ph'}:
        return Rule((ICMP_TEMPLATE,), source=f'role:{role}')
    if manufacturer == 'aten' and model == 'cl5716i':
        return Rule((ICMP_TEMPLATE,), source='ATEN CL5716I')
    if role == 'sw-fc' and manufacturer in {'brocade', 'lenovo'}:
        return _snmp('Brocade FC by SNMP', source='FC switch')

    if role in NETWORK_ROLES:
        if manufacturer in {'tp-link', 'tp link', 'tplink'}:
            return _snmp('TP-LINK by SNMP', source='TP-Link')
        if manufacturer == 'huawei':
            return _snmp('Huawei VRP by SNMP', source='Huawei')
        if manufacturer == 'juniper':
            return _snmp('Juniper by SNMP', source='Juniper')
        if manufacturer in {'rvi group', 'aruba'}:
            return _snmp('Network Generic Device by SNMP', source='generic switch')
        if manufacturer == 'ubiquiti':
            template = 'Ubiquiti EdgeSwitch 24 250W by SNMP' if 'edgeswitch 24' in model else 'Network Generic Device by SNMP'
            return _snmp(template, source='Ubiquiti')
        if manufacturer == 'cisco':
            template = 'Cisco_SG300-52-d' if 'business 350' in model else 'Cisco IOS by SNMP'
            return _snmp(template, source='Cisco')
        if manufacturer == 'mikrotik':
            return _snmp('Mikrotik by SNMP', source='MikroTik')
        if manufacturer in {'d-link', 'dlink'}:
            if 'dgs-1510-28x' in model:
                template = 'D-Link DGS-1510-28X by SNMP'
            elif 'des-3226s' in model:
                template = 'D-Link DES-3226S by SNMP'
            else:
                template = 'D-Link DES_DGS Switch by SNMP'
            return _snmp(template, source='D-Link')
        if manufacturer == 'hp' and 'officeconnect' in model:
            return _snmp('Network Generic Device by SNMP', source='HP OfficeConnect')

    if role == 'hvac' and ('срк-3.1' in model or 'srk-3.1' in model):
        return _snmp('SRK 3.1', ZabbixHostInterfaceSNMPVersionChoices.SNMPV1, 'SRK-3.1')

    if role == 'pdu':
        if manufacturer == 'apc' and 'ap8853' in model:
            return _snmp('APC AP8853 Rack PDU SNMP minimal', source='APC AP8853')
        if manufacturer == 'eaton' and model == '9155':
            return _snmp('Eaton UPS by SNMP', ZabbixHostInterfaceSNMPVersionChoices.SNMPV1, 'Eaton 9155')
        if manufacturer == 'delta' and 'rt 20kva' in model:
            return _snmp('Template UPS Delta RT 20kVA SNMP', source='Delta RT 20KVA')
        if manufacturer == 'entel' and ('mpx-p60bpvp' in model or 'u200au3' in model):
            return _snmp('Entel MPX-P60BPVP SNMP', source='Entel MPX')

    if role == 'mfu':
        if manufacturer == 'konica minolta' and model in MFU_TEMPLATE_BY_MODEL:
            return _snmp(MFU_TEMPLATE_BY_MODEL[model], source=f'Konica Minolta {model}')
        if manufacturer == 'kyocera':
            template = 'Kyocera TASKalfa 5004i by SNMP' if '5004i' in model else 'Kyocera Printers Template'
            return _snmp(template, source='Kyocera')

    return None


def _preflight(device, rule: Rule):
    site_key = _site_key(device)
    targets = TARGETS_BY_SITE[SERVER_SITE_MAP[site_key]]
    server_ids = {target.server_id for target in targets}
    servers = {row.pk: row for row in ZabbixServer.objects.filter(pk__in=server_ids)}
    if set(servers) != server_ids:
        raise AutofillError(f'Missing Zabbix servers: {sorted(server_ids - set(servers))}')

    templates = {
        (row.zabbixserver_id, row.name): row
        for row in ZabbixTemplate.objects.filter(zabbixserver_id__in=server_ids, name__in=rule.templates)
    }
    missing = [
        f'{name} on {servers[target.server_id].name}'
        for target in targets for name in rule.templates
        if (target.server_id, name) not in templates
    ]
    if missing:
        raise AutofillError('Missing templates: ' + '; '.join(missing))

    proxy_ids = {target.proxy_id for target in targets if target.proxy_id is not None}
    proxies = {row.pk: row for row in ZabbixProxy.objects.filter(pk__in=proxy_ids)}
    if set(proxies) != proxy_ids:
        raise AutofillError(f'Missing proxies: {sorted(proxy_ids - set(proxies))}')
    return site_key, targets, servers, templates, proxies


def _group_names(device, site_key: str, result: AutofillResult) -> tuple[str, ...]:
    role = _role(device)
    category = ROLE_TYPE_MAP.get(role)
    _, _, manufacturer, model = _manufacturer_model(device)
    names = []
    if category:
        names.append(f'{category}/{GROUP_SITE_MAP[site_key]}')
    else:
        result.warnings.append(f'No group category for role {role or "not set"}.')
    if manufacturer and model:
        names.append(f'{manufacturer}/{model}')
    return tuple(dict.fromkeys(names))


def _ensure_server(device, ct, target, proxy, result):
    assignment, created = ZabbixServerAssignment.objects.get_or_create(
        zabbixserver_id=target.server_id,
        assigned_object_type=ct,
        assigned_object_id=device.pk,
        defaults={'zabbixproxy': proxy},
    )
    if created:
        result.server_assignments_created += 1
    elif proxy and assignment.zabbixproxy_id is None and assignment.zabbixproxygroup_id is None:
        assignment.zabbixproxy = proxy
        assignment.save()
        result.server_assignments_updated += 1
    elif proxy and assignment.zabbixproxy_id not in (None, proxy.pk):
        result.warnings.append(f'{assignment.zabbixserver}: existing proxy was preserved.')


def _ensure_groups(device, ct, server, names, result):
    for name in names:
        group, created = ZabbixHostgroup.objects.get_or_create(zabbixserver=server, name=name)
        result.hostgroups_created += int(created)
        _, created = ZabbixHostgroupAssignment.objects.get_or_create(
            zabbixhostgroup=group,
            assigned_object_type=ct,
            assigned_object_id=device.pk,
        )
        result.hostgroup_assignments_created += int(created)


def _ensure_interface(device, ct, server, rule, primary_ip, result):
    if rule.interface_type is None:
        return
    defaults = {
        'useip': ZabbixInterfaceUseChoices.IP,
        'ip': primary_ip,
        'dns': '',
        'port': rule.port,
    }
    if rule.interface_type == ZabbixHostInterfaceTypeChoices.SNMP:
        defaults.update(snmp_version=rule.snmp_version, snmp_usebulk=True, snmp_community='')
    interface, created = ZabbixHostInterface.objects.get_or_create(
        zabbixserver=server,
        type=rule.interface_type,
        assigned_object_type=ct,
        assigned_object_id=device.pk,
        defaults=defaults,
    )
    if created:
        result.interfaces_created += 1
        return
    differences = []
    if interface.ip_id != primary_ip.pk:
        differences.append('IP')
    if interface.port != rule.port:
        differences.append('port')
    if rule.interface_type == ZabbixHostInterfaceTypeChoices.SNMP and interface.snmp_version != rule.snmp_version:
        differences.append('SNMP version')
    if differences:
        result.warnings.append(f'{server.name}: existing interface preserved; differs in {", ".join(differences)}.')


def _ensure_templates(device, ct, server, templates, rule, result):
    for name in rule.templates:
        _, created = ZabbixTemplateAssignment.objects.get_or_create(
            zabbixtemplate=templates[(server.pk, name)],
            assigned_object_type=ct,
            assigned_object_id=device.pk,
        )
        result.template_assignments_created += int(created)


def _ensure_inventory(device, ct, result):
    site_rack_template = (
        '{% if object.rack %}{{ object.rack.name }}'
        '{% if object.position %}, U{{ object.position }}{% endif %}'
        '{% endif %}'
    )

    inventory, created = ZabbixHostInventory.objects.get_or_create(
        assigned_object_type=ct,
        assigned_object_id=device.pk,
        defaults={
            'inventory_mode': ZabbixHostInventoryModeChoices.MANUAL,
            'site_rack': site_rack_template,
        },
    )

    if created:
        result.inventory_created += 1
        return

    changed = False

    if inventory.inventory_mode != ZabbixHostInventoryModeChoices.MANUAL:
        inventory.inventory_mode = ZabbixHostInventoryModeChoices.MANUAL
        changed = True

    if inventory.site_rack != site_rack_template:
        inventory.site_rack = site_rack_template
        changed = True

    if changed:
        inventory.save()
        result.inventory_updated += 1


def fill_nbxsync_device(device) -> AutofillResult:
    if not getattr(device, 'pk', None):
        raise AutofillError('Device must be saved first.')
    if _norm(getattr(device, 'status', '')) != 'active':
        raise AutofillError('Fill NbxSync is allowed only for active devices.')
    if getattr(device, 'site', None) is None:
        raise AutofillError('Device has no site.')
    primary_ip = getattr(device, 'primary_ip4', None) or getattr(device, 'primary_ip6', None)
    if primary_ip is None:
        raise AutofillError('Device has no primary IPv4 or IPv6 address.')

    role = _role(device)
    if role in PASSIVE_ROLES:
        raise AutofillError(f'Role {role} is excluded from autofill.')
    if role in MANUAL_ROLES:
        raise AutofillError(f'Role {role} requires manual NbxSync configuration.')

    rule = select_rule(device)
    if rule is None:
        manufacturer, model, _, _ = _manufacturer_model(device)
        raise AutofillError(
            f'No approved mapping for role={role or "not set"}, '
            f'manufacturer={manufacturer or "not set"}, model={model or "not set"}.'
        )

    site_key, targets, servers, templates, proxies = _preflight(device, rule)
    ct = ContentType.objects.get_for_model(device, for_concrete_model=False)
    result = AutofillResult()
    group_names = _group_names(device, site_key, result)

    for target in targets:
        proxy = proxies.get(target.proxy_id) if target.proxy_id is not None else None
        _ensure_server(device, ct, target, proxy, result)
        _ensure_groups(device, ct, servers[target.server_id], group_names, result)
        _ensure_interface(device, ct, servers[target.server_id], rule, primary_ip, result)
        _ensure_templates(device, ct, servers[target.server_id], templates, rule, result)

    _ensure_inventory(device, ct, result)
    result.warnings.append(f'Mapping: {rule.source or "approved table"}.')
    return result
