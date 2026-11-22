"""Local Zigbee tunnel through the Tuya gateways (DP 198 -> DP 199 response).

For the operations the cloud misses: reading the weekly schedule directly from the head,
reading/writing the real ZCL attributes (including the preheat state, attribute 0x0025). Tuya LAN
protocol 3.5 (tinytuya). Ported from ~/dev/danfoss-ally-mcp/local.py.

The frame functions are PURE (testable without a network). ``LocalLink`` isolates tinytuya (a double
is possible in tests). Blocking calls: the HA layer runs them in the executor.

Tunnel header: ``40 01 | cluster (2 bytes, LE) | 04 01 01 00 | ZCL frame``. The gateway relays the
DP 198 frame to the head and pushes the response into DP 199 (1 to 9 s). As the ZCL sequence is
renumbered, a response is recognised by its content.
"""

from __future__ import annotations

import base64
import contextlib
import math
import threading
import time
from collections.abc import Callable
from typing import Any

from ..keys import GatewayKey

CLUSTER_THERMOSTAT = 0x0201
MANUFACTURER = 0x1246
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAY_BIT = {
    "sunday": 0x01,
    "monday": 0x02,
    "tuesday": 0x04,
    "wednesday": 0x08,
    "thursday": 0x10,
    "friday": 0x20,
    "saturday": 0x40,
}
MAX_TRANSITIONS = 6
# Valve descaling days (Danfoss attribute 0x4010, enum8): Sunday = 0, ... Saturday = 6,
# 7 = undefined.
DESCALE_DAYS = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "undefined",
]

# Attributes read: (id, name). 0x0025 = programming mode (bit 1 = preheat).
ATTRS_STD = [
    (0x0012, "current_setpoint"),
    (0x0015, "min_setpoint"),
    (0x0016, "max_setpoint"),
    (0x0025, "programming_mode"),
    (0x0030, "setpoint_source"),
]
# Danfoss attributes (manufacturer code 0x1246), split into two surveys like the MCP connector.
# ``read_attributes`` gates on the FIRST id being present in the head's answer, so ``descale_day``
# (always answered) stays first; the new attributes are appended.
ATTRS_DANFOSS_A = [
    (0x4010, "descale_day"),
    (0x4011, "descale_time"),
    (0x4014, "orientation"),
    (0x4016, "radiator_covered"),
    (0x4020, "control_speed"),
    (0x4030, "heat_available"),
    (0x4031, "heat_request"),
    (0x4032, "load_balancing"),
    (0x4003, "window_open_external"),
    (0x4015, "external_room_sensor"),
]
ATTRS_DANFOSS_B = [
    (0x404B, "setpoint_offset"),
    (0x404D, "adaptation_state"),
    (0x404E, "adaptation_setting"),
    (0x404F, "preheat_active"),
    (0x4051, "window_detection"),
    (0x4000, "window_state"),
]
# Settings writable by their room DP (official names of the product schema 03wvrwpv, contract §4.2).
# The real preheat, though, is bit 1 of attribute 0x0025 (see write_preheat): writing DP 1 only
# touches the gateway cache (observed on real hardware, 2026-11-15).
SETTINGS_DP: dict[str, tuple[str, type]] = {
    "window_detection": ("116", bool),  # window_toggle
    "child_lock": ("30", bool),  # child_lock
}
# "Now" setpoint override: DP 171 OccupiedSetpoint. MIND the scale, see setpoint_value: the MCP
# (verified on real hardware) writes it in hundredths of °C, whereas the product schema announces a
# 10^0 scale - inconsistency flagged, we follow the real observation (hundredths).
DP_SETPOINT = "171"

_HUNDREDTHS = {"current_setpoint", "min_setpoint", "max_setpoint"}
_BOOLEANS = {
    "radiator_covered",
    "heat_available",
    "heat_request",
    "load_balancing",
    "preheat_active",
    "window_detection",
    "window_open_external",
    "orientation",
    # 0x404E ``adaptation_run_settings``: Zigbee2MQTT exposes it as a binary (valueOn [true, 1],
    # valueOff [false, 0]) - "automatic adaptation run enabled". So we decode it to a bool too.
    "adaptation_setting",
}
_LENGTH = {
    0x10: 1,
    0x18: 1,
    0x19: 2,
    0x20: 1,
    0x21: 2,
    0x22: 3,
    0x23: 4,
    0x28: 1,
    0x29: 2,
    0x2B: 4,
    0x30: 1,
    0x31: 2,
}
_SIGNED = {0x28, 0x29, 0x2B}


class LocalUnavailable(Exception):
    """Local access impossible (keys missing, tinytuya missing, gateway unreachable)."""


class LocalError(Exception):
    """Invalid local request (malformed schedule, unhandled ZCL type...)."""


# --- frames (pure functions) ------------------------------------------------------------------


def header(cluster: int) -> bytes:
    return bytes.fromhex("4001") + cluster.to_bytes(2, "little") + bytes.fromhex("04010100")


def frame_read_attributes(attributes: list[int], manufacturer: bool, seq: int = 0x40) -> bytes:
    if manufacturer:
        start = bytes([0x04]) + MANUFACTURER.to_bytes(2, "little") + bytes([seq, 0x00])
    else:
        start = bytes([0x00, seq, 0x00])
    body = b"".join(a.to_bytes(2, "little") for a in attributes)
    return header(CLUSTER_THERMOSTAT) + start + body


def frame_read_schedule(day: str, seq: int = 0x40) -> bytes:
    return header(CLUSTER_THERMOSTAT) + bytes([0x01, seq, 0x02, DAY_BIT[day], 0x01])


def frame_write_attributes(
    records: list[tuple[int, int, int]], manufacturer: bool, seq: int = 0x40
) -> bytes:
    """ZCL attribute write (cmd 0x02). records = [(attribute, ZCL type, value)]."""
    if manufacturer:
        start = bytes([0x04]) + MANUFACTURER.to_bytes(2, "little") + bytes([seq, 0x02])
    else:
        start = bytes([0x00, seq, 0x02])
    body = b""
    for a, typ, v in records:
        if typ not in _LENGTH:
            raise LocalError(f"unhandled ZCL type: 0x{typ:02x}")
        body += (
            a.to_bytes(2, "little")
            + bytes([typ])
            + int(v).to_bytes(_LENGTH[typ], "little", signed=typ in _SIGNED)
        )
    return header(CLUSTER_THERMOSTAT) + start + body


def minutes(h: str) -> int:
    try:
        hh, mm = (int(x) for x in h.split(":"))
    except (ValueError, AttributeError) as e:
        raise LocalError(f"invalid time: {h!r} (expected HH:MM)") from e
    if not (0 <= hh <= 24 and 0 <= mm < 60 and hh * 60 + mm <= 1440):
        raise LocalError(f"invalid time: {h!r}")
    return hh * 60 + mm


def check_transitions(transitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Valid day: 1 to 6 transitions, 1st at 00:00, increasing times, setpoints 5-35/0.5."""
    if not transitions or len(transitions) > MAX_TRANSITIONS:
        raise LocalError(f"a day has 1 to {MAX_TRANSITIONS} transitions")
    out: list[dict[str, Any]] = []
    prev = -1
    for t in transitions:
        m, c = minutes(t["h"]), float(t["c"])
        if m <= prev or m >= 1440:
            raise LocalError("transitions must be in order, without duplicates, before 24:00")
        if not 5 <= c <= 35 or round(c * 2) != c * 2:
            raise LocalError(f"setpoint out of range: {c} (5 to 35 °C by 0.5)")
        out.append({"h": f"{m // 60:02d}:{m % 60:02d}", "c": c})
        prev = m
    if out[0]["h"] != "00:00":
        raise LocalError("the first transition of a day must be at 00:00")
    return out


def frame_write_schedule(day: str, transitions: list[dict[str, Any]], seq: int = 0x40) -> bytes:
    tr = check_transitions(transitions)
    body = bytes([len(tr), DAY_BIT[day], 0x01])
    for t in tr:
        body += minutes(t["h"]).to_bytes(2, "little") + int(round(t["c"] * 100)).to_bytes(
            2, "little", signed=True
        )
    return header(CLUSTER_THERMOSTAT) + bytes([0x01, seq, 0x01]) + body


def decode_frame(raw: bytes) -> dict[str, Any] | None:
    """DP 199 frame -> {cluster, manufacturer, command, specific, payload}; None if unrecognised."""
    if len(raw) < 11 or raw[:2] != b"\x40\x01":
        return None
    z = raw[8:]
    fc, i, man = z[0], 1, None
    if fc & 0x04:
        if len(z) < 5:
            return None
        man, i = int.from_bytes(z[1:3], "little"), 3
    return {
        "cluster": int.from_bytes(raw[2:4], "little"),
        "manufacturer": man,
        "command": z[i + 1],
        "specific": bool(fc & 0x01),
        "payload": z[i + 2 :],
    }


def decode_attributes(payload: bytes) -> dict[int, int | None]:
    out: dict[int, int | None] = {}
    i = 0
    while i + 3 <= len(payload):
        a, status = int.from_bytes(payload[i : i + 2], "little"), payload[i + 2]
        i += 3
        if status != 0:
            out[a] = None
            continue
        n = _LENGTH.get(payload[i])
        if n is None or i + 1 + n > len(payload):
            break
        out[a] = int.from_bytes(payload[i + 1 : i + 1 + n], "little", signed=payload[i] in _SIGNED)
        i += 1 + n
    return out


def decode_schedule(payload: bytes) -> tuple[int, list[dict[str, Any]]]:
    """GetWeeklySchedule response -> (day bit, transitions [{h, c}])."""
    n, day = payload[0], payload[1]
    tr = []
    for k in range(n):
        o = 3 + 4 * k
        if o + 4 > len(payload):
            break
        m = int.from_bytes(payload[o : o + 2], "little")
        c = int.from_bytes(payload[o + 2 : o + 4], "little", signed=True) / 100
        tr.append({"h": f"{m // 60:02d}:{m % 60:02d}", "c": c})
    return day, tr


def name_attributes(values: dict[int, int | None], table: list[tuple[int, str]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for a, name in table:
        v = values.get(a)
        if v is None:
            continue
        if name in _HUNDREDTHS:
            out[name] = v / 100
        elif name in _BOOLEANS:
            out[name] = bool(v)
        elif name == "descale_day":
            out[name] = DESCALE_DAYS[v] if v < len(DESCALE_DAYS) else v
        elif name == "descale_time":
            out[name] = f"{v // 60:02d}:{v % 60:02d}"
        elif name == "programming_mode":
            out[name] = v
            out["preheat"] = bool(v & 0x02)  # 3 = schedule + preheat, 1 = schedule only
        elif name == "adaptation_state":
            out[name] = v
            out["adaptation_done"] = bool(v & 0x02)
        else:
            out[name] = v
    return out


def comfort_slots(transitions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Comfort slots of a day: segments whose setpoint exceeds the day's lowest.

    Two consecutive comfort segments at the same setpoint are merged. The reference is the day's
    lowest setpoint (the setback), not the one at 00:00: otherwise a day starting in comfort (a slot
    at 00:00) would show no slot.
    """
    if not transitions:
        return []
    base: float = min(t["c"] for t in transitions)
    out: list[dict[str, Any]] = []
    for i, t in enumerate(transitions):
        if t["c"] > base:
            end = transitions[i + 1]["h"] if i + 1 < len(transitions) else "24:00"
            if out and out[-1]["end"] == t["h"] and out[-1]["setpoint"] == t["c"]:
                out[-1]["end"] = end
            else:
                out.append({"start": t["h"], "end": end, "setpoint": t["c"]})
    return out


def descale_records(day: str, hour: str) -> list[tuple[int, int, int]]:
    """Weekly valve descaling: Danfoss attributes 0x4010 (day, enum8) and 0x4011 (time in minutes,
    uint16). To be written with ``write_attributes(..., manufacturer=True)``."""
    d = (day or "").lower()
    if d not in DESCALE_DAYS[:7]:
        raise LocalError(f"unknown descale day: {day}")
    return [(0x4010, 0x30, DESCALE_DAYS.index(d)), (0x4011, 0x21, minutes(hour) % 1440)]


# Danfoss settings written through the tunnel, one attribute each, UNDER SUPERVISED TRIAL ONLY
# (not proven on real hardware): name (as ``name_attributes`` reads it) -> (attribute, ZCL type).
# Types as Zigbee2MQTT writes them (zigbee-herdsman-converters, src/devices/danfoss.ts): boolean
# (0x10), uint8 (0x20), int8 (0x28, signed), int16 (0x29, signed).
TUNNEL_SETTINGS: dict[str, tuple[int, int]] = {
    "radiator_covered": (0x4016, 0x10),
    "control_speed": (0x4020, 0x20),  # Zigbee2MQTT ``algorithm_scale_factor``
    "setpoint_offset": (0x404B, 0x28),  # Zigbee2MQTT ``regulation_setpoint_offset``
    "window_open_external": (0x4003, 0x10),  # Zigbee2MQTT ``window_open_external``
    "load_balancing": (0x4032, 0x10),  # Zigbee2MQTT ``load_balancing_enable``
    "external_room_sensor": (0x4015, 0x29),  # Zigbee2MQTT ``external_measured_room_sensor``
    # 0x404E ``adaptation_run_settings``: enable/disable the automatic (nightly) adaptation run, a
    # boolean (Zigbee2MQTT ``m.binary``, valueOn [true, 1] / valueOff [false, 0]). It is bitmap8,
    # written with ZCL type 0x18 (confirmed by the reference MCP connector, which reads 0x404E typed
    # 0x18). NOT the separate command attribute 0x404C ``adaptation_run_control`` (enum8:
    # idle / initiate / cancel), which the tunnel does not read back and which is out of scope here.
    "adaptation_setting": (0x404E, 0x18),
}
# Ranges of Zigbee2MQTT. The offset is carried in TENTHS of °C (raw -25..25): Zigbee2MQTT's scale,
# NOT confirmed on real hardware here.
CONTROL_SPEED_MIN, CONTROL_SPEED_MAX = 1, 10
SETPOINT_OFFSET_MIN, SETPOINT_OFFSET_MAX = -2.5, 2.5
# External measured room sensor (0x4015, int16): HUNDREDTHS of °C, raw -8000..3500 (Zigbee2MQTT
# ``external_measured_room_sensor``). The sentinel raw -8000 (= -80 °C) DISABLES the external
# sensor: the head then regulates on its own internal measurement. The head also resets the value
# to -8000 by itself after 30 min to 3 h without an update (radiator_covered mode dependent), so a
# live feed must be refreshed periodically (docs/tunnel-settings-trial.md).
EXTERNAL_ROOM_SENSOR_DISABLED = -8000  # raw hundredths, -80.0 °C
EXTERNAL_ROOM_SENSOR_MIN, EXTERNAL_ROOM_SENSOR_MAX = -80.0, 35.0  # °C (raw -8000..3500)


def setting_raw(setting: str, value: Any) -> int:
    """Validate a tunnel setting's value and return the raw integer to write.

    ``radiator_covered``: a bool. ``control_speed``: an integer 1 to 10. ``setpoint_offset``: °C,
    -2.5 to 2.5 by 0.1, written in tenths (raw -25..25). Raises LocalError otherwise: nothing is
    sent for a value that does not pass.
    """
    if setting in (
        "radiator_covered",
        "window_open_external",
        "load_balancing",
        "adaptation_setting",
    ):
        if not isinstance(value, bool):
            raise LocalError(f"{setting} expects a boolean, got {value!r}")
        return int(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise LocalError(f"{setting} expects a number, got {value!r}")
    if setting == "control_speed":
        if value != int(value) or not CONTROL_SPEED_MIN <= value <= CONTROL_SPEED_MAX:
            raise LocalError(
                f"regulation speed out of range: {value} "
                f"(integer {CONTROL_SPEED_MIN} to {CONTROL_SPEED_MAX})"
            )
        return int(value)
    if setting == "setpoint_offset":
        raw = round(float(value) * 10)
        if abs(raw - float(value) * 10) > 1e-6 or not (
            SETPOINT_OFFSET_MIN * 10 <= raw <= SETPOINT_OFFSET_MAX * 10
        ):
            raise LocalError(
                f"setpoint offset out of range: {value} "
                f"({SETPOINT_OFFSET_MIN} to {SETPOINT_OFFSET_MAX} °C by 0.1)"
            )
        return int(raw)
    if setting == "external_room_sensor":
        raw = round(float(value) * 100)
        if abs(raw - float(value) * 100) > 1e-6 or not (
            EXTERNAL_ROOM_SENSOR_DISABLED <= raw <= round(EXTERNAL_ROOM_SENSOR_MAX * 100)
        ):
            raise LocalError(
                f"external room sensor out of range: {value} "
                f"({EXTERNAL_ROOM_SENSOR_MIN} to {EXTERNAL_ROOM_SENSOR_MAX} °C; "
                f"{EXTERNAL_ROOM_SENSOR_MIN} °C disables it)"
            )
        return int(raw)
    raise LocalError(f"unknown tunnel setting: {setting} (known: {', '.join(TUNNEL_SETTINGS)})")


def setting_records(setting: str, value: Any) -> list[tuple[int, int, int]]:
    """The write record of one tunnel setting (validated). For ``write_attributes(...,
    manufacturer=True)``."""
    raw = setting_raw(setting, value)
    attribute, typ = TUNNEL_SETTINGS[setting]
    return [(attribute, typ, raw)]


def setpoint_value(celsius: float) -> int:
    """Setpoint override (DP 171) -> integer to publish. Valid 5-35 °C by 0.5.

    Scale: hundredths of °C (21 °C -> 2100), per the MCP connector's real observation of 2026-11-15.
    The official product schema (DP 171 ``OccupiedSetpoint``) announces a 10^0 scale: this
    inconsistency is known and settled in favour of the real observation (see DP_SETPOINT).
    """
    c = float(celsius)
    if not 5 <= c <= 35 or round(c * 2) != c * 2:
        raise LocalError(f"setpoint out of range: {celsius} (5 to 35 °C by 0.5)")
    return int(round(c * 100))


def transitions_from_slots(
    slots: list[dict[str, str]], comfort: float, setback: float
) -> list[dict[str, Any]]:
    """Comfort slots ``[{start, end}]`` -> a day's transitions (setback outside)."""
    tr: list[dict[str, Any]] = [{"h": "00:00", "c": float(setback)}]
    prev_end = 0
    for f in sorted(slots, key=lambda x: minutes(x["start"])):
        d, e = minutes(f["start"]), minutes(f["end"])
        if d < prev_end or e <= d:
            raise LocalError("malformed comfort slots (overlap or end before start)")
        if d == 0:
            tr[0]["c"] = float(comfort)
        else:
            tr.append({"h": f"{d // 60:02d}:{d % 60:02d}", "c": float(comfort)})
        if e < 1440:
            tr.append({"h": f"{e // 60:02d}:{e % 60:02d}", "c": float(setback)})
        prev_end = e
    return check_transitions(tr)


# --- link to a gateway (tinytuya) -------------------------------------------------------------


class LocalLink:
    """Persistent connection to a gateway (tinytuya). A double is possible in tests."""

    def __init__(self, gateway: GatewayKey) -> None:
        if not gateway.ip:
            raise LocalUnavailable(f"unknown LAN IP for gateway {gateway.name or gateway.dev_id}")
        try:
            import tinytuya
        except ImportError as e:
            raise LocalUnavailable("tinytuya not installed") from e
        self._tt = tinytuya
        self._p = tinytuya.Device(
            gateway.dev_id, gateway.ip, gateway.local_key, version=3.5, persist=True
        )
        self._p.set_socketTimeout(3)
        self._subs: dict[str, Any] = {}
        self._heartbeat_at = time.time()

    def _sub(self, dev_id: str, node_id: str) -> Any:
        if dev_id not in self._subs:
            self._subs[dev_id] = self._tt.Device(dev_id, cid=node_id, parent=self._p)
        return self._subs[dev_id]

    def send(self, dev_id: str, node_id: str, frame: bytes) -> None:
        self._sub(dev_id, node_id).set_value("198", base64.b64encode(frame).decode(), nowait=True)

    def send_dp(self, dev_id: str, node_id: str, dp: str, value: Any) -> None:
        """Publish a value on a head DP (outside the tunnel): setpoint override (171), room settings
        (116, 30). Fire-and-forget: the gateway acknowledges via a refresh."""
        self._sub(dev_id, node_id).set_value(dp, value, nowait=True)

    def receive(self) -> tuple[str | None, dict[str, Any]] | None:
        if time.time() - self._heartbeat_at > 20:
            with contextlib.suppress(Exception):
                self._p.heartbeat(nowait=True)
            self._heartbeat_at = time.time()
        try:
            m = self._p.receive()
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(m, dict):
            return None
        d = m.get("dps") or (m.get("data") or {}).get("dps")
        if not d:
            return None
        return (m.get("cid") or (m.get("data") or {}).get("cid"), d)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._p.close()


def request(
    link: Any,
    dev_id: str,
    node_id: str,
    frame: bytes,
    accept: Callable[[dict[str, Any]], Any],
    duration: float = 12,
    attempts: int = 2,
) -> Any:
    """Send a frame through the tunnel and wait for the response `accept` recognises (None else)."""
    for _ in range(attempts):
        link.send(dev_id, node_id, frame)
        t0 = time.time()
        while time.time() - t0 < duration:
            r = link.receive()
            if not r or r[0] != node_id or "199" not in r[1]:
                continue
            try:
                f = decode_frame(base64.b64decode(r[1]["199"]))
            except (ValueError, TypeError):
                f = None
            if f and f["cluster"] == CLUSTER_THERMOSTAT:
                v = accept(f)
                if v is not None:
                    return v
    return None


def read_attributes(
    link: Any,
    dev_id: str,
    node_id: str,
    table: list[tuple[int, str]],
    manufacturer: bool,
    **kw: Any,
) -> dict[str, Any] | None:
    ids = [a for a, _ in table]

    def accept(f: dict[str, Any]) -> Any:
        if f["specific"] or f["command"] != 0x01 or bool(f["manufacturer"]) != manufacturer:
            return None
        v = decode_attributes(f["payload"])
        return v if ids[0] in v else None

    v = request(link, dev_id, node_id, frame_read_attributes(ids, manufacturer), accept, **kw)
    return None if v is None else name_attributes(v, table)


def read_standard(link: Any, dev_id: str, node_id: str, **kw: Any) -> dict[str, Any] | None:
    """A head's standard attributes (``ATTRS_STD``, one frame), named. None if no response.

    The same single request as ``read_preheat`` / ``read_programming_mode``: it also carries the
    setpoint change source (0x0030) and the setpoint bounds, at no extra traffic.
    """
    return read_attributes(link, dev_id, node_id, ATTRS_STD, False, **kw)


def read_settings(link: Any, dev_id: str, node_id: str, **kw: Any) -> dict[str, Any] | None:
    """A head's Danfoss (manufacturer) attributes, named: ``ATTRS_DANFOSS_A`` then ``_B``.

    Two frames for a head that answers. None if the first one gets no response: the second is
    then not sent, so a head that does not answer costs one request, not two. A head that answers
    the first only returns what it gave.
    """
    out = read_attributes(link, dev_id, node_id, ATTRS_DANFOSS_A, True, **kw)
    if out is None:
        return None
    out.update(read_attributes(link, dev_id, node_id, ATTRS_DANFOSS_B, True, **kw) or {})
    return out


def read_preheat(link: Any, dev_id: str, node_id: str, **kw: Any) -> bool | None:
    """A head's real preheat (standard attribute 0x0025, bit 1). None if no response."""
    a = read_attributes(link, dev_id, node_id, ATTRS_STD, False, **kw)
    return None if a is None else a.get("preheat")


def read_day(
    link: Any, dev_id: str, node_id: str, day: str, **kw: Any
) -> list[dict[str, Any]] | None:
    def accept(f: dict[str, Any]) -> Any:
        if not f["specific"] or f["command"] != 0x00 or f["manufacturer"] or len(f["payload"]) < 3:
            return None
        bit, tr = decode_schedule(f["payload"])
        return tr if bit == DAY_BIT[day] else None

    return request(link, dev_id, node_id, frame_read_schedule(day), accept, **kw)


def read_schedule(link: Any, dev_id: str, node_id: str) -> dict[str, list[dict[str, Any]]]:
    """A head's 7-day weekly schedule (through the tunnel)."""
    sched: dict[str, list[dict[str, Any]]] = {}
    for d in WEEKDAYS:
        tr = read_day(link, dev_id, node_id, d)
        if tr is not None:
            sched[d] = tr
    return sched


def read_programming_mode(link: Any, dev_id: str, node_id: str, **kw: Any) -> int | None:
    """Raw 0x0025 value (bit 0 schedule, bit 1 preheat). None without a response."""
    a = read_attributes(link, dev_id, node_id, ATTRS_STD, False, **kw)
    return None if a is None else a.get("programming_mode")


def new_preheat_mode(current_mode: int, active: bool) -> int:
    """New 0x0025 value: (de)activate the preheat bit (0x02), schedule bit preserved."""
    return (current_mode | 0x02) if active else (current_mode & ~0x02)


def write_attributes(
    link: Any,
    dev_id: str,
    node_id: str,
    records: list[tuple[int, int, int]],
    manufacturer: bool,
) -> int | None:
    """Write attributes through the tunnel. Returns the 1st status (0 = accepted), else None."""

    def accept(f: dict[str, Any]) -> Any:
        if f["specific"] or f["command"] != 0x04 or bool(f["manufacturer"]) != manufacturer:
            return None
        return f["payload"][0] if f["payload"] else None

    frame = frame_write_attributes(records, manufacturer)
    return request(link, dev_id, node_id, frame, accept, duration=10, attempts=1)


def write_preheat(
    link: Any, dev_id: str, node_id: str, active: bool, current_mode: int
) -> int | None:
    """Enable/disable the real preheat (0x0025 bit 1), schedule bit preserved.

    Attribute 0x0025 "Programming Operation Mode" of the thermostat cluster is a bitmap8: it must be
    written with ZCL type 0x18 (and not 0x30 enum8, which the head rejects). Type validated by the
    reference MCP connector (real observation of 2026-11-15).
    """
    new = new_preheat_mode(current_mode, active)
    return write_attributes(link, dev_id, node_id, [(0x0025, 0x18, new)], manufacturer=False)


def write_day(
    link: Any, dev_id: str, node_id: str, day: str, transitions: list[dict[str, Any]]
) -> int | None:
    """SetWeeklySchedule (0x01) for ONE day through the tunnel. Returns the head's status (0 =
    accepted) in the default ZCL response (0x0B), None without a response. R2: the head then
    propagates the whole week. R1 is validated upstream (``check_transitions``, also in frame)."""

    def accept(f: dict[str, Any]) -> Any:
        c = f["payload"]
        return c[1] if f["command"] == 0x0B and len(c) >= 2 and c[0] == 0x01 else None

    frame = frame_write_schedule(day, transitions)
    return request(link, dev_id, node_id, frame, accept, duration=10, attempts=1)


def write_descale(link: Any, dev_id: str, node_id: str, day: str, hour: str) -> int | None:
    """Set the valve descaling (0x4010/0x4011, manufacturer). Status 0 = accepted."""
    return write_attributes(link, dev_id, node_id, descale_records(day, hour), manufacturer=True)


def write_setting(link: Any, dev_id: str, node_id: str, setting: str, value: Any) -> int | None:
    """Write ONE Danfoss setting of ``TUNNEL_SETTINGS`` (validated first, LocalError otherwise).
    Returns the head's status (0 = accepted), None without a response."""
    return write_attributes(
        link, dev_id, node_id, setting_records(setting, value), manufacturer=True
    )


# The valve exercise attributes alone, read just before rewriting one of the two.
ATTRS_VALVE_EXERCISE = [(0x4010, "descale_day"), (0x4011, "descale_time")]


def read_valve_exercise(link: Any, dev_id: str, node_id: str, **kw: Any) -> dict[str, Any] | None:
    """A head's valve exercise day and time (``descale_day`` / ``descale_time``). None if no
    response."""
    return read_attributes(link, dev_id, node_id, ATTRS_VALVE_EXERCISE, True, **kw)


def write_setpoint(link: Any, dev_id: str, node_id: str, celsius: float) -> int:
    """ "Now" setpoint override via DP 171 (hundredths). Fire-and-forget: returns the published
    value (verified by a later read-back). Outside the schedule: an override until the head's
    next change (not a durable setpoint, cf. contract R3/R5)."""
    val = setpoint_value(celsius)
    link.send_dp(dev_id, node_id, DP_SETPOINT, val)
    return val


def write_setting_dp(link: Any, dev_id: str, node_id: str, setting: str, value: Any) -> Any:
    """Write a room setting by its DP (``window_detection`` -> 116, ``child_lock`` -> 30).
    Fire-and-forget: returns the published value (type coerced per SETTINGS_DP)."""
    if setting not in SETTINGS_DP:
        raise LocalError(f"unknown DP setting: {setting} (known: {', '.join(SETTINGS_DP)})")
    dp, typ = SETTINGS_DP[setting]
    val = typ(value)
    link.send_dp(dev_id, node_id, dp, val)
    return val


def read_window_detection(link: Any, dev_id: str, node_id: str, **kw: Any) -> bool | None:
    """Read Danfoss attribute 0x4051 (open-window detection, bool). None without a response."""
    a = read_attributes(link, dev_id, node_id, [(0x4051, "window_detection")], True, **kw)
    return None if a is None else a.get("window_detection")


def read_head(link: Any, dev_id: str, node_id: str, schedule: bool = True) -> dict[str, Any]:
    """Full survey of a head through the tunnel: standard + Danfoss attributes (A and B) and, if
    ``schedule``, the transitions of the 7 days. ``unreachable`` if nothing responds. Best-effort:
    attribute survey that fails is noted in ``missing`` without interrupting the others."""
    e: dict[str, Any] = {"attributes": {}, "schedule": {}, "missing": []}
    t0, failures = time.time(), 0
    for name, table, man in (
        ("standard", ATTRS_STD, False),
        ("danfoss_a", ATTRS_DANFOSS_A, True),
        ("danfoss_b", ATTRS_DANFOSS_B, True),
    ):
        a = read_attributes(link, dev_id, node_id, table, man)
        if a is None:
            e["missing"].append(name)
            failures += 1
            if failures >= 2 and not e["attributes"]:
                break
        else:
            e["attributes"].update(a)
    if not e["attributes"]:
        e["unreachable"] = True
    elif schedule:
        for d in WEEKDAYS:
            tr = read_day(link, dev_id, node_id, d)
            if tr is None:
                e["missing"].append(d)
            else:
                e["schedule"][d] = tr
    e["duration_s"] = round(time.time() - t0)
    return e


# Upper bound of a full survey: a fixed margin plus a per-head allowance for the busiest gateway (a
# head that answers takes ~8 s, one that does not about 48 s: two attribute reads of two 12 s
# attempts). Past it the survey stops and the heads not read yet are reported ``not_read``.
SURVEY_BASE_TIMEOUT = 60.0
SURVEY_HEAD_TIMEOUT = 60.0


def _not_surveyed(error: str, **extra: Any) -> dict[str, Any]:
    return {"attributes": {}, "schedule": {}, "unreachable": True, "error": error[:80], **extra}


def survey(
    gateways: dict[str, GatewayKey],
    heads: list[Any],
    open_link: Callable[[GatewayKey], Any] = LocalLink,
    progress: Callable[[str, dict[str, Any]], None] | None = None,
    timeout: float | None = None,
) -> dict[str, dict[str, Any]]:
    """Robust survey of several heads, one gateway per thread (read-only, ~8 s per head).

    ``heads``: objects exposing ``dev_id``, ``node_id`` and ``gateway_id`` (e.g. ``manager.Head``).
    Result indexed by ``dev_id``. Best-effort: an unreachable gateway marks its heads unreachable,
    and each head is isolated from the others (no exception propagated).

    Every head of a known gateway is in the result. ``timeout`` (default: scaled on the busiest
    gateway) bounds the whole survey: past it, each worker gives up after the head it is on (a
    tunnel request is not interruptible) and the heads not read yet are returned as
    ``{"unreachable": True, "not_read": True}`` rather than left out. Nothing is written into the
    returned mapping afterwards. A head of an unknown gateway is ignored."""
    res: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()
    stop = threading.Event()
    by_gw: dict[str, list[Any]] = {}
    for h in heads:
        if h.gateway_id in gateways:
            by_gw.setdefault(h.gateway_id, []).append(h)

    def publish(dev_id: str, entry: dict[str, Any]) -> bool:
        with lock:
            if stop.is_set():
                return False
            res[dev_id] = entry
            return True

    def worker(gw_id: str, mine: list[Any]) -> None:
        try:
            link = open_link(gateways[gw_id])
        except Exception as ex:  # noqa: BLE001 - gateway unreachable
            for h in mine:
                publish(h.dev_id, _not_surveyed(str(ex)))
            return
        try:
            for h in mine:
                if stop.is_set():
                    return
                try:
                    e = read_head(link, h.dev_id, h.node_id)
                except Exception as ex:  # noqa: BLE001 - isolate each head
                    e = _not_surveyed(str(ex))
                if publish(h.dev_id, e) and progress:
                    progress(h.dev_id, e)
        finally:
            link.close()

    threads = [threading.Thread(target=worker, args=(g, m), daemon=True) for g, m in by_gw.items()]
    if timeout is None:
        busiest = max((len(m) for m in by_gw.values()), default=0)
        timeout = SURVEY_BASE_TIMEOUT + SURVEY_HEAD_TIMEOUT * busiest
    deadline = time.monotonic() + timeout
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    with lock:
        stop.set()  # timed out: late workers stop after their current head and publish nothing
        for mine in by_gw.values():
            for h in mine:
                res.setdefault(h.dev_id, _not_surveyed("survey timed out", not_read=True))
        return dict(res)


# --- summary and comparison (pure functions) --------------------------------------------------


def summarize_head(e: dict[str, Any]) -> dict[str, Any]:
    """Summary of a head survey: comfort slots per day, comfort/setback and attributes."""
    if not e.get("attributes"):
        # ``not_read``: the survey ran out of time before this head (it was not asked at all).
        return {"unreachable": True, **({"not_read": True} if e.get("not_read") else {})}
    p = e.get("schedule") or {}
    cs = [t["c"] for d in WEEKDAYS for t in p.get(d, [])]
    return {
        "slots": {
            d: [f"{x['start']}–{x['end']}" for x in comfort_slots(p.get(d))]
            for d in WEEKDAYS
            if d in p
        },
        "comfort": max(cs) if cs else None,
        "setback": min(cs) if cs else None,
        **e["attributes"],
        **({"missing": e["missing"]} if e.get("missing") else {}),
    }


# Settings tracked from one survey to the next (differences reported by ``compare``).
TRACKED_SETTINGS = (
    "max_setpoint",
    "min_setpoint",
    "programming_mode",
    "descale_day",
    "descale_time",
    "control_speed",
    "window_detection",
    "adaptation_done",
    "radiator_covered",
    "load_balancing",
    "window_open_external",
    "adaptation_setting",
)


def compare(
    reference: dict[str, dict[str, Any]], current: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Differences of a survey against a reference: schedules, tracked settings, unreachable heads.

    ``reference`` and ``current`` are indexed the same way (``dev_id`` or name)."""
    ra = {n: e for n, e in reference.items() if e.get("attributes")}
    rb = {n: e for n, e in current.items() if e.get("attributes")}
    sched = []
    for n in sorted(rb):
        if n in ra:
            for d in WEEKDAYS:
                x, y = ra[n]["schedule"].get(d), rb[n]["schedule"].get(d)
                if x != y and y is not None:
                    sched.append({"head": n, "day": d, "before": x, "after": y})
    settings = [
        {
            "head": n,
            "setting": k,
            "before": ra[n]["attributes"].get(k),
            "after": rb[n]["attributes"].get(k),
        }
        for n in sorted(rb)
        if n in ra
        for k in TRACKED_SETTINGS
        if ra[n]["attributes"].get(k) != rb[n]["attributes"].get(k)
    ]

    def heating(e: dict[str, Any], d: str) -> bool:
        return bool(comfort_slots(e["schedule"].get(d)))

    return {
        "surveyed": len(rb),
        "attempted": len(current),
        "unreachable": sorted(n for n, e in current.items() if not e.get("attributes")),
        "newly_unreachable": sorted(n for n in ra if n in current and n not in rb),
        "schedules_changed": sched,
        "settings_changed": settings,
        "heating_wednesday": sorted(n for n, e in rb.items() if heating(e, "wednesday")),
        "heating_weekend": sorted(
            n for n, e in rb.items() if heating(e, "saturday") or heating(e, "sunday")
        ),
    }
