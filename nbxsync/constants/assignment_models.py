from django.db.models import Q

# Добавлено: dcim.sitegroup
ASSIGNMENT_MODELS = Q(
    Q(app_label='dcim', model='device')
    | Q(app_label='dcim', model='virtualdevicecontext')
    | Q(app_label='dcim', model='manufacturer')
    | Q(app_label='dcim', model='devicerole')
    | Q(app_label='dcim', model='devicetype')
    | Q(app_label='dcim', model='platform')
    | Q(app_label='dcim', model='sitegroup')            # ← ДОБАВЛЕНО
    | Q(app_label='virtualization', model='virtualmachine')
    | Q(app_label='virtualization', model='cluster')
    | Q(app_label='virtualization', model='clustertype')
)

# Тут SiteGroup НЕ добавляем — инвентаризация применяется только к device/vm
DEVICE_OR_VM_ASSIGNMENT_MODELS = Q(
    Q(app_label='dcim', model='device')
    | Q(app_label='dcim', model='virtualdevicecontext')
    | Q(app_label='virtualization', model='virtualmachine')
)

# Макросы как были — всё правильно
MACRO_ASSIGNMENT_MODELS = Q(
    Q(app_label='nbxsync', model='zabbixserver')
    | Q(app_label='nbxsync', model='zabbixtemplate')
)

# Maintenance: тоже без sitegroup — по логике Zabbix maintenance привязывается к host/hostgroup
MAINTENANCE_ASSIGNMENT_OBJECTS = Q(
    Q(Q(app_label='nbxsync', model='zabbixhostgroup')) 
    | DEVICE_OR_VM_ASSIGNMENT_MODELS
)

MAINTENANCE_ASSIGNMENT_TAGS = Q(app_label='nbxsync', model='zabbixtag')
