from django.db.models import Q
from django_filters import CharFilter, NumberFilter

from netbox.filtersets import NetBoxModelFilterSet

from nbxsync.models import ZabbixTemplate

__all__ = ('ZabbixTemplateFilterSet',)


class ZabbixTemplateFilterSet(NetBoxModelFilterSet):
    q = CharFilter(method='search', label='Search')
    name = CharFilter(lookup_expr='icontains')
    templateid = NumberFilter()
    zabbixserver_name = CharFilter(field_name='zabbixserver__name', lookup_expr='icontains')
    ordering = CharFilter(method='filter_ordering')

    class Meta:
        model = ZabbixTemplate
        fields = (
            'id',
            'name',
            'templateid',
            'zabbixserver',
            'zabbixserver_name',
        )

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(templateid__icontains=value)
            | Q(zabbixserver__name__icontains=value)
        ).distinct()

    def filter_ordering(self, queryset, name, value):
        allowed = {
            'templateid': 'templateid',
            'name': 'name',
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
