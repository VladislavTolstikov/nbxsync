# nbxsync/utils/zabbix_description.py

from __future__ import annotations

from typing import Optional

from django.conf import settings
from dcim.models import Device, Location
from tenancy.models import Tenant


CF_KEY = "Zabbix_description"  # slug кастомного поля в NetBox


def _build_location_path(device: Device) -> str:
    parts = []

    site = device.site
    if site:
        if site.group:
            parts.append(site.group.name)
        parts.append(site.name)

    deepest_desc: Optional[str] = None

    loc: Optional[Location] = device.location
    if loc:
        chain = []
        cur = loc
        while cur is not None:
            chain.append(cur)
            cur = cur.parent

        chain.reverse()
        # как в скрипте: пропускаем корень, если цепочка >1
        if len(chain) > 1:
            chain_for_names = chain[1:]
        else:
            chain_for_names = chain

        for o in chain_for_names:
            if o.name:
                parts.append(o.name)

        last = chain[-1]
        if last.description:
            txt = last.description.strip()
            if txt:
                deepest_desc = txt

    if deepest_desc:
        parts.append(deepest_desc)

    return " / ".join(parts) if parts else "-"


def _build_device_label(device: Device) -> str:
    dt = device.device_type
    if not dt:
        return "-"

    mfg = dt.manufacturer.name if dt.manufacturer else ""
    model = dt.model or ""
    label = " ".join(x for x in (mfg, model) if x)
    return label or "-"


def _find_responsible(device: Device) -> str:
    tenant: Optional[Tenant] = device.tenant
    if not tenant:
        return "-"
    return tenant.name or tenant.slug or "-"


def _device_link(device: Device) -> str:
    """
    Строим абсолютную ссылку через SITE_URL + get_absolute_url().
    В NetBox 4.x SITE_URL должен быть задан в configuration.py.
    """
    base = getattr(settings, "SITE_URL", "").rstrip("/")
    return f"{base}{device.get_absolute_url()}"


def build_zabbix_description(device: Device) -> str:
    """
    Формирует текст для описания хоста Zabbix в нужном формате:

    Локация: ...
    Модель:  ...
    NetBox:  ...
    Ответственный: ...
    """
    loc_path = _build_location_path(device)
    dev_label = _build_device_label(device)
    link = _device_link(device)
    resp = _find_responsible(device)

    return (
        f"Локация: {loc_path}\n"
        f"Модель:  {dev_label}\n"
        f"NetBox:  {link}\n"
        f"Ответственный: {resp}"
    )


def ensure_cf_zabbix_description(device: Device) -> str:
    """
    Строит строку, сравнивает с custom_field_data[CF_KEY],
    при необходимости обновляет и сохраняет девайс.

    Возвращает актуальное значение описания.
    """
    desc = build_zabbix_description(device)

    # custom_field_data — dict, может быть None
    data = device.custom_field_data or {}
    old = (data.get(CF_KEY) or "").strip()

    if old != desc:
        data[CF_KEY] = desc
        device.custom_field_data = data
        # сохраняем только custom_field_data, чтобы не трогать лишнее
        device.save(update_fields=["custom_field_data"])

    return desc
