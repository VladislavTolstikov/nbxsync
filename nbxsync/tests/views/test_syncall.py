from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from utilities.testing import create_test_user

from nbxsync.models import ZabbixServer
from nbxsync.views.syncall import syncall


class SyncAllImportTests(SimpleTestCase):
    def test_import_plugin_module(self):
        import nbxsync  # noqa: F401

    def test_import_urls(self):
        import nbxsync.urls  # noqa: F401

    def test_import_syncall_view(self):
        import nbxsync.views.syncall  # noqa: F401


class TriggerZabbixServerSyncAllViewTests(TestCase):
    def setUp(self):
        self.user = create_test_user()
        self.user.is_superuser = True
        self.user.save()

        self.server = ZabbixServer.objects.create(
            name='Test Server',
            url='http://example.com',
            token='secret-token',
            validate_certs=True,
        )

    @patch('nbxsync.views.syncall.get_queue')
    def test_post_enqueues_syncall_job(self, mock_get_queue):
        queue = MagicMock()
        mock_get_queue.return_value = queue

        self.client.force_login(self.user)

        url = reverse('plugins:nbxsync:zabbixserver_syncall', args=[self.server.pk])
        response = self.client.post(url)

        self.assertEqual(response.status_code, 302)
        mock_get_queue.assert_called_once_with('low')
        queue.enqueue.assert_called_once_with(syncall, self.server)
