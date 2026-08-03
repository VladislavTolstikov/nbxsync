from types import SimpleNamespace

from django.test import SimpleTestCase

from nbxsync.services.site_resolution import resolve_site_key


def node(name='', slug='', parent=None, pk=None):
    return SimpleNamespace(name=name, slug=slug, parent=parent, pk=pk)


class SiteResolutionTests(SimpleTestCase):
    def test_resolves_address_site_from_site_group(self):
        site = node(name='ул. Сулейменова, 7', slug='ul-suleimenova-7', pk=10)
        site.group = node(name='Тараз', slug='TARAZ', pk=20)
        device = SimpleNamespace(site=site)

        self.assertEqual(resolve_site_key(device), 'TARAZ')

    def test_resolves_from_parent_site_group(self):
        parent = node(name='Тараз', slug='TARAZ', pk=30)
        child = node(name='Филиалы Казахстана', slug='kz-branches', parent=parent, pk=31)
        site = node(name='ул. Сулейменова, 7', slug='ul-suleimenova-7', pk=32)
        site.group = child
        device = SimpleNamespace(site=site)

        self.assertEqual(resolve_site_key(device), 'TARAZ')

    def test_keeps_direct_site_slug_fallback(self):
        site = node(name='Тараз', slug='TZ', pk=40)
        site.group = None
        device = SimpleNamespace(site=site)

        self.assertEqual(resolve_site_key(device), 'TZ')
