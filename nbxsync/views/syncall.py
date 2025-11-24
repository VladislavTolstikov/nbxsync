from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect
from django.utils.translation import gettext as _
from django.views import View
from django_rq import get_queue

from nbxsync.models import ZabbixServer
from nbxsync.worker.syncall import syncall


__all__ = (
    'TriggerZabbixServerSyncAllView',
)


class TriggerZabbixServerSyncAllView(View):
    def post(self, request, pk):
        server = get_object_or_404(ZabbixServer, pk=pk)

        # enqueue job to low-priority queue (NetBox 4.4)
        get_queue('low').enqueue(syncall, server)

        messages.success(
            request,
            _('Full Zabbix sync job enqueued for %(name)s') % {'name': server}
        )
        return redirect(server.get_absolute_url())
