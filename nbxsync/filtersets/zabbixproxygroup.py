from django.db.models import Q
from django_filters import CharFilter, ModelChoiceFilter, NumberFilter

from netbox.filtersets import NetBoxModelFilterSet

from nbxsync.models import ZabbixProxyGroup, ZabbixServer


__all__ = ('ZabbixProxyGroupFilterSet',)


class ZabbixProxyGroupFilterSet(NetBoxModelFilterSet):
    q = CharFilter(method='search', label='Search')

    name = CharFilter(lookup_expr='icontains')
    proxy_groupid = NumberFilter()
    min_online = NumberFilter()
    failover_delay = NumberFilter()
    zabbixserver = ModelChoiceFilter(queryset=ZabbixServer.objects.all())
    zabbixserver_name = CharFilter(field_name='zabbixserver__name', lookup_expr='icontains')
    ordering = CharFilter(method='filter_ordering')

    class Meta:
        model = ZabbixProxyGroup
        fields = (
            'id',
            'proxy_groupid',
            'name',
            'min_online',
            'failover_delay',
            'zabbixserver',
            'zabbixserver_name',
        )

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(name__icontains=value) | Q(description__icontains=value)).distinct()

    def filter_ordering(self, queryset, name, value):
        allowed = {
            'proxy_groupid': 'proxy_groupid',
            'name': 'name',
            'min_online': 'min_online',
            'failover_delay': 'failover_delay',
            'zabbixserver': 'zabbixserver__name',
        }
        fields = []
        for item in str(value).split(','):
            item = item.strip()
            if not item:
                continue
            descending = item.startswith('-')
            key = item[1:] if descending else item
            field = allowed.get(key)
            if field:
                fields.append(f'-{field}' if descending else field)
        return queryset.order_by(*fields) if fields else queryset
