"""Fetching the gateways' local keys (path 1a).

Replaces the emulator / Frida: each gateway's localKey is returned in the `localKey` field of
tuya.m.my.group.device.list (verified on real hardware, 10/10 gateways). The LAN IP comes from
smartlife.m.device.get v4.0 (meta.lan_ip). These keys are SECRETS: never in cleartext in the logs,
the diagnostics or the repository.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GatewayKey:
    """A gateway's local material (secret: do not display local_key)."""

    dev_id: str
    name: str | None
    local_key: str
    ip: str | None = None


def _is_gateway(device: dict) -> bool:
    return (device.get("category") or "").startswith("wg")


async def async_fetch_gateway_keys(client) -> dict[str, GatewayKey]:
    """Return {gateway_devId: GatewayKey} (localKey + LAN IP) from the cloud. Must not be displayed.

    `client`: CloudClient. Heads have no key of their own: they go through their gateway's key
    (DanfossDevice.mesh_id = the gateway's devId, node_id = Zigbee cid).
    """
    out: dict[str, GatewayKey] = {}
    for d in await client.async_list_raw():
        if _is_gateway(d) and d.get("localKey"):
            out[d["devId"]] = GatewayKey(
                dev_id=d["devId"],
                name=d.get("name"),
                local_key=d["localKey"],
            )
    # Fill in the LAN IP (one request per gateway).
    for gw in out.values():
        try:
            meta = await client.async_device_meta(gw.dev_id)
        except Exception:  # noqa: BLE001 - the IP may be missing; the gateway stays listed
            continue
        gw.ip = meta.get("lan_ip")
    return out
