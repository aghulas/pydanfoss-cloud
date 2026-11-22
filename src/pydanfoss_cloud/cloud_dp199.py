"""Decode the DP 199 cloud-MQTT push (the head's raw Zigbee frame) into read state.

DP 199 (``gw_from_device``) is the upstream half of the gateway's Zigbee tunnel: the head's raw ZCL
frame. The local LAN tunnel reads exactly these bytes (``local/tunnel.py``), and the cloud receives
them too — ~63 % of the cloud-MQTT traffic is DP 199 (``docs/cloud-mqtt-interface.md`` §7). Decoding
it lets tunnel-grade reads (programming mode ``0x0025``, setpoint source, adaptation, the ``0x40xx``
manufacturer attributes, window state) come from the cloud push, without the LAN tunnel.

This module REUSES the existing tunnel decoder (``decode_frame``, ``decode_attributes``,
``decode_report_attributes``, ``name_attributes`` and its attribute tables) — it does not
reimplement ZCL parsing — and the cloud fault table (``decode.decode_fault``). It only adds the
cloud-side glue: pick the naming table by ``(cluster, manufacturer)``, scale the few spontaneous
attributes the tunnel survey never reads (local temperature, battery, software error), and project
the decoded attributes onto a :class:`~pydanfoss_cloud.decode.DanfossDevice` read state.

Honesty rules (``docs/mqtt-routing-target.md`` §4):

- A decoded attribute is surfaced only where its meaning/scale is established. An attribute with no
  known name is kept raw under ``raw`` (hex-keyed), exactly as the tunnel survey leaves unknowns.
- No optimistic state: the overlay fills a read value only when a push actually carried it, and a
  push is opportunistic (DP 199 carries an attribute only when something reads it on the LAN), so
  this SUPPLEMENTS the tunnel/HTTP reads, it does not replace them.
"""

from __future__ import annotations

import base64
import dataclasses
from dataclasses import dataclass, field
from typing import Any

from .decode import DanfossDevice, decode_fault
from .local import tunnel

# ZCL clusters seen in DP 199 (docs/cloud-mqtt-interface.md §7).
CLUSTER_THERMOSTAT = tunnel.CLUSTER_THERMOSTAT  # 0x0201
CLUSTER_POWER = 0x0001
CLUSTER_DIAGNOSTICS = 0x0B05

# ZCL general command ids handled here.
CMD_READ_ATTRIBUTES_RESPONSE = 0x01
CMD_REPORT_ATTRIBUTES = 0x0A

# Spontaneous Report-Attributes attributes the tunnel survey never asks for, so they are absent from
# its tables (``ATTRS_STD`` / ``ATTRS_DANFOSS_*``). (attribute id, decoded name); scaled below.
_LOCAL_TEMPERATURE = (0x0000, "local_temperature")  # thermostat, int16 hundredths of °C
_BATTERY = (0x0021, "battery")  # power cluster, uint8 in HALF percent (126 -> 63 %)
_LOAD_ESTIMATE = (0x404A, "load_estimate")  # thermostat mfr, int16 (DP 108)
_LOAD_ROOM_MEAN = (0x4040, "load_room_mean")  # thermostat mfr, int16 (DP 107)
_SW_ERROR = (0x4000, "sw_error_code")  # diagnostics mfr, bitmap16 (DP 184 / 45)

# Decoded names that carry a hundredths-of-°C scale and are NOT already scaled by
# ``tunnel.name_attributes`` (which scales current/min/max_setpoint but not local_temperature).
_HUNDREDTHS_EXTRA = frozenset({"local_temperature"})


def _naming_table(cluster: int, manufacturer: int | None) -> list[tuple[int, str]]:
    """The (attribute id, name) table for a frame's ``(cluster, manufacturer)``.

    Built from the tunnel's own survey tables plus the spontaneous-report attributes above. An
    empty list means nothing is named for that cluster: every attribute then falls through to
    ``raw``.
    """
    mfr = manufacturer == tunnel.MANUFACTURER
    if cluster == CLUSTER_THERMOSTAT and manufacturer is None:
        return [_LOCAL_TEMPERATURE, *tunnel.ATTRS_STD]
    if cluster == CLUSTER_THERMOSTAT and mfr:
        return [*tunnel.ATTRS_DANFOSS_A, *tunnel.ATTRS_DANFOSS_B, _LOAD_ESTIMATE, _LOAD_ROOM_MEAN]
    if cluster == CLUSTER_POWER and manufacturer is None:
        return [_BATTERY]
    if cluster == CLUSTER_DIAGNOSTICS and mfr:
        return [_SW_ERROR]
    return []


@dataclass(frozen=True)
class Dp199Decode:
    """One decoded DP 199 frame.

    ``attributes`` holds the named, established-scale values; ``faults`` the decoded software-error
    list (when the frame is a diagnostics report); ``raw`` the attributes whose id has no known name
    (hex-keyed), kept so nothing is silently dropped.
    """

    cluster: int
    command: int
    manufacturer: int | None
    attributes: dict[str, Any] = field(default_factory=dict)
    faults: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, int | None] = field(default_factory=dict)


def _to_bytes(raw: bytes | bytearray | str) -> bytes | None:
    """Normalise a DP 199 value to bytes. The channel carries it base64-encoded (like the tunnel's
    ``dps["199"]``), so a string is decoded as base64. Returns None if it cannot be decoded."""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw, validate=True)
        except (ValueError, TypeError):
            return None
    return None


def decode_dp199(raw: bytes | bytearray | str) -> Dp199Decode | None:
    """Decode a DP 199 payload (raw bytes, or the channel's base64 string) into named read values.

    Returns None when the bytes are not a recognised DP 199 ZCL frame, or when the frame is neither
    a Read Attributes Response (0x01) nor a Report Attributes (0x0A) — e.g. the poll-control
    check-in, which carries no attribute to read.
    """
    data = _to_bytes(raw)
    if data is None:
        return None
    frame = tunnel.decode_frame(data)
    if frame is None or frame["specific"]:
        return None
    cluster: int = frame["cluster"]
    command: int = frame["command"]
    manufacturer: int | None = frame["manufacturer"]
    payload: bytes = frame["payload"]

    if command == CMD_READ_ATTRIBUTES_RESPONSE:
        values = tunnel.decode_attributes(payload)
    elif command == CMD_REPORT_ATTRIBUTES:
        values = tunnel.decode_report_attributes(payload)
    else:
        return None

    table = _naming_table(cluster, manufacturer)
    named = tunnel.name_attributes(values, table)
    # Scale the extras the tunnel's name table does not scale on its own.
    for key in _HUNDREDTHS_EXTRA:
        if isinstance(named.get(key), (int, float)):
            named[key] = named[key] / 100
    if isinstance(named.get("battery"), (int, float)):
        named["battery"] = named["battery"] / 2  # raw half-percent (§7: 126 -> 63)
    faults: list[dict[str, Any]] = []
    if "sw_error_code" in named:
        faults = decode_fault(named.pop("sw_error_code"))

    named_ids = {a for a, _ in table}
    raw_unknown = {f"0x{a:04x}": v for a, v in values.items() if a not in named_ids}

    return Dp199Decode(
        cluster=cluster,
        command=command,
        manufacturer=manufacturer,
        attributes=named,
        faults=faults,
        raw=raw_unknown,
    )


# Decoded attribute name -> DanfossDevice field. Only attributes with an established read-state home
# are merged onto the device; the rest stay in the overlay's ``local_attrs`` / diagnostics.
_DEVICE_FIELD = {
    "local_temperature": "temperature",
    "current_setpoint": "setpoint",
    "min_setpoint": "min_setpoint",
    "max_setpoint": "max_setpoint",
    "battery": "battery",
    "window_detection": "window_detection",
}
# Decoded attribute name -> the ``local_attrs`` key ``z2m_contract.encode_z2m_state`` already reads.
_LOCAL_ATTR = {
    "setpoint_source": "setpoint_source",
    "radiator_covered": "radiator_covered",
    "heat_available": "heat_available",
    "control_speed": "regulation_speed",
    "setpoint_offset": "setpoint_offset",
    "descale_day": "descale_day",
    "descale_time": "descale_time",
    "adaptation_state": "adaptation_run",
    "window_state": "window_state",
}


@dataclass
class HeadOverlay:
    """Per-head accumulation of the latest DP 199 pushes, dated.

    ``device_fields`` are merged onto the polled :class:`DanfossDevice` (``apply_to``);
    ``local_attrs`` feed ``encode_z2m_state(dev, local_attrs=...)``; ``attributes`` keeps the full
    named set for diagnostics; ``raw`` the unknowns; and ``updated`` the epoch of the last push
    merged. A push only overwrites the keys it actually carried (no optimistic state): quiet
    attributes keep their previous pushed value.
    """

    device_fields: dict[str, Any] = field(default_factory=dict)
    local_attrs: dict[str, Any] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, int | None] = field(default_factory=dict)
    updated: float = 0.0

    def merge(self, decoded: Dp199Decode, *, now: float) -> bool:
        """Merge one decoded frame. Returns True if it carried any surfaced value (so the caller
        republishes), False for an empty/raw-only frame."""
        changed = False
        for name, value in decoded.attributes.items():
            self.attributes[name] = value
            if name in _DEVICE_FIELD:
                self.device_fields[_DEVICE_FIELD[name]] = value
                changed = True
            if name in _LOCAL_ATTR:
                self.local_attrs[_LOCAL_ATTR[name]] = value
                changed = True
        if "programming_mode" in decoded.attributes:
            mode = decoded.attributes["programming_mode"]
            self.device_fields["schedule_active"] = bool(mode & 0x01)
            self.device_fields["preheat"] = bool(mode & 0x02)
            changed = True
        if decoded.faults or "sw_error_code" in decoded.attributes:
            # A diagnostics report with no bits set clears the faults (empty list), which is a real
            # read: an all-clear is as meaningful as an error.
            self.device_fields["faults"] = list(decoded.faults)
            self.attributes["faults"] = list(decoded.faults)
            changed = True
        if decoded.raw:
            self.raw.update(decoded.raw)
        if changed or decoded.raw:
            self.updated = now
        return changed

    def apply_to(self, dev: DanfossDevice) -> DanfossDevice:
        """A copy of ``dev`` with the overlay's device fields set (never mutates ``dev``)."""
        if not self.device_fields:
            return dev
        return dataclasses.replace(dev, **self.device_fields)


__all__ = [
    "CLUSTER_DIAGNOSTICS",
    "CLUSTER_POWER",
    "CLUSTER_THERMOSTAT",
    "CMD_READ_ATTRIBUTES_RESPONSE",
    "CMD_REPORT_ATTRIBUTES",
    "Dp199Decode",
    "HeadOverlay",
    "decode_dp199",
]
