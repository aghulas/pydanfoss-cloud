"""Decode a Danfoss Ally head's Tuya ``dps`` (path 1a) into a normalised model.

Input: a raw device from ``tuya.m.my.group.device.list`` (devId, name, category, productId,
nodeId, meshId, moduleMap, dps). The ``dps`` are indexed by DP NUMBER (string), per the official
product schema ``03wvrwpv`` (see danfoss-app-capture/dp_schema_officiel.md, source of truth).

⚠ To confirm on real hardware (v0.1): the exact scale of some DPs may depend on the firmware.
Setpoints and temperatures use the 10^-1 scale (tenths of °C). Any unknown DP stays in dps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# --- Fault table (DP 45 `fault` / DP 184 `sw_error_code`): Danfoss "SW error code" bit field
# (Zigbee attribute 0x4000, diagnostic cluster 0x0B05). Identical to ZHA and Zigbee2MQTT.
# The sibling MCP connector (danfoss-ally-mcp) imports this table and now surfaces these English
# strings too (coordinated with pydanfoss-cloud 0.2.0).
FAULT_BITS = {
    0: ("E1", "upper board sensor fault", "faulty head: replace it"),
    1: ("E2", "side board sensor fault", "faulty head: replace it"),
    2: ("E3", "internal memory fault", "faulty head: replace it"),
    3: ("E4", "unknown hardware error", "faulty head: replace it"),
    5: (
        "E6",
        "motor stuck",
        "restart the head (batteries), then remount it on the valve body",
    ),
    7: (
        "E8",
        "invalid internal communication",
        "restart; clears on its own in ~10 min if transient",
    ),
    9: (
        "E10",
        "invalid clock",
        "time to resync via the gateway (the schedule does not run without the time)",
    ),
    11: ("E12", "radio communication error", "check the gateway and the Zigbee range"),
    12: ("E13", "encoder stuck", "restart the head"),
    13: ("E14", "low battery", "change the batteries soon"),
    14: ("E15", "very low battery (critical)", "change the batteries now"),
}


def decode_fault(v: Any) -> list[dict[str, Any]]:
    """`fault` code (bit field, int or decimal string, or "E14") -> list of errors."""
    if isinstance(v, str) and v.strip().upper().startswith("E") and v.strip()[1:].isdigit():
        n = int(v.strip()[1:]) - 1
        value = 1 << n if 0 <= n < 16 else 0
    else:
        try:
            value = int(v)
        except (TypeError, ValueError):
            return [{"code": str(v), "label": "unrecognised code"}]
    errors = []
    for bit in range(16):
        if value >> bit & 1:
            if bit in FAULT_BITS:
                code, label, action = FAULT_BITS[bit]
                errors.append({"code": code, "bit": bit, "label": label, "action": action})
            else:
                errors.append(
                    {"code": f"bit {bit}", "bit": bit, "label": f"undocumented bit {bit}"}
                )
    return errors


# Setpoints per mode (DP -> readable key), tenths-of-°C scale.
_SETPOINT_DPS = {
    # DP 16 temp_set: NOT the active setpoint, it reports the comfort setpoint whatever the slot
    # (R-1). Kept under its DP name for information; the active one is ``DanfossDevice.setpoint``.
    "16": "temp_set",
    "118": "comfort",  # at_home_setting
    "119": "setback",  # leaving_home_setting
    "114": "manual",  # manual_mode_fast
    "120": "pause",  # pause_setting
    "121": "holiday",  # holiday_setting
}

# Current mode (DP 2) -> which per-mode setting is the ACTIVE setpoint. Fallback only, used when the
# payload lacks DP 171 (OccupiedSetpoint): DP 2 and the per-mode settings are always in the bulk
# device.list payload, so the active setpoint stays correct without the comfort/active confusion.
_MODE_ACTIVE_KEY = {
    "at_home": "comfort",
    "leaving_home": "setback",
    "manual": "manual",
    "pause": "pause",
    "holiday": "holiday",
    "holiday_sat": "holiday",
}


def _num(v: Any, divisor: float = 1) -> float | None:
    if v is None or v == "":
        return None
    try:
        return round(float(v) / divisor, 2)
    except (TypeError, ValueError):
        return None


def _bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "on", "yes", "open", "active"):
            return True
        if s in ("false", "0", "off", "no", "close", "closed", "inactive"):
            return False
    return None


_HOLIDAY_TEMP_KEYS = ("temp", "temperature", "tempSet", "holidayTemp", "setTemp", "value")


def holiday_running(detail: dict[str, Any] | None) -> bool | None:
    """Whether a ``vacation.detail`` record is a holiday *in progress* (contract §4.5).

    ``vacation.detail`` also returns *planned* holidays (measured 2026-11-19: a record with
    ``isRunning: false`` for a holiday two weeks ahead), so a non-empty record is not "active".
    ``isRunning`` tells them apart. Returns ``False`` for an empty record (no holiday), the parsed
    flag when present, and ``None`` (unknown) for a record without a readable ``isRunning`` rather
    than guessing.
    """
    if not isinstance(detail, dict) or not detail:
        return False
    return _bool(detail.get("isRunning"))


def holiday_temperature(detail: dict[str, Any] | None) -> float | None:
    """Best-effort holiday setpoint (°C) from a ``vacation.detail`` record.

    The holiday record's exact shape is cloud-defined (the set/exit routes are uncaptured, §4.5);
    this reads the first recognised numeric temperature field and applies the standard Danfoss
    tenths-of-°C scale when the raw value is >= 50 (every Danfoss temperature DP is tenths). Returns
    ``None`` when no recognised field is present (rather than fabricating a value). The caller also
    exposes the raw ``detail`` as attributes, so nothing is hidden.
    """
    if not isinstance(detail, dict):
        return None
    for key in _HOLIDAY_TEMP_KEYS:
        if key in detail:
            raw = _num(detail[key])
            if raw is None:
                continue
            return round(raw / 10, 1) if raw >= 50 else raw
    return None


def category_of(category: str | None) -> str:
    """Raw Tuya category -> readable category."""
    c = (category or "").lower()
    if c == "wk":
        return "head"
    if c == "kg":
        return "relay"
    if c in ("wsdcg", "mcs", "ms", "pir"):
        return "sensor"
    # Any ``wg*`` Tuya category is an Ally gateway (wg2 on real hardware); the prefix match is
    # defensive so a variant gateway code can never be mistaken for a detachable sub-device (a
    # gateway must never be unbound — it would drop all its heads at once).
    if c.startswith("wg") or c in ("zigbee", "gateway"):
        return "gateway"
    return "other"


@dataclass
class DanfossDevice:
    """Normalised model of a Danfoss Ally device (read-only)."""

    id: str
    name: str
    category: str
    product_id: str | None = None
    node_id: str | None = None  # Zigbee cid (link with the local bridge)
    mesh_id: str | None = None  # gateway
    online: bool = False
    temperature: float | None = None  # °C (DP 24; room sensor: DP 1)
    humidity: float | None = None  # % relative (room sensor DP 2); None on a head
    setpoint: float | None = None  # °C ACTIVE setpoint (DP 171, else mode setting, else DP 16)
    setpoints: dict[str, float] = field(default_factory=dict)
    mode: str | None = None  # DP 2
    state: str | None = None  # DP 3 work_state
    battery: float | None = None  # % (DP 34)
    valve_opening: float | None = None  # % (DP 170 pi_heating_demand)
    load_estimate: int | None = None  # raw Danfoss radiator load estimate (int16, DP 108 / 0x404A)
    heat_request: bool | None = None  # DP 162 heat_supply_request
    window_open: bool | None = None  # DP 25 window_state
    window_detection: bool | None = None  # DP 116 window_toggle
    child_lock: bool | None = None  # DP 30 child_lock
    max_setpoint: float | None = None  # °C (DP 18)
    min_setpoint: float | None = None  # °C (DP 27)
    schedule_active: bool | None = None  # DP 172 bit 0 (R6); None if DP absent
    preheat: bool | None = None  # DP 172 bit 1 (R6); None if DP absent
    mounting: bool | None = None  # DP 149 (mounting mode: not regulating)
    viewing_direction: bool | None = None  # DP 101 ViewingDirection (cloud's view)
    orientation: str | None = None  # DP 133 vertical_mounting: "Horizontal" | "Vertical" (cloud's)
    relay_on: bool | None = None  # DP 1 on a boiler relay (category "kg"); None otherwise
    faults: list[dict[str, Any]] = field(default_factory=list)
    dps: dict[str, Any] = field(default_factory=dict)

    @property
    def is_head(self) -> bool:
        return self.category == "head"

    @property
    def is_relay(self) -> bool:
        return self.category == "relay"

    @property
    def is_sensor(self) -> bool:
        """A Danfoss Ally room sensor (category ``wsdcg``): temperature + humidity, read only."""
        return self.category == "sensor"

    @property
    def inconsistent(self) -> bool:
        """Mode shows comfort/setback while the schedule is inactive in the head (R6)."""
        return self.mode in ("at_home", "leaving_home") and self.schedule_active is False

    @property
    def heating(self) -> bool | None:
        """Real heating state from ``work_state`` (DP 3): ``Heat`` -> True, ``NoHeat`` -> False.

        The head's own ``work_state`` is authoritative over a local temperature-vs-setpoint guess
        or a cracked-valve reading (real hardware R-1, 2026-11-18: ``NoHeat`` with the valve at 1 %
        while the room sat above its active setpoint). Falls back to the valve opening when DP 3 is
        absent (older firmware), and is ``None`` when neither is known.
        """
        if isinstance(self.state, str):
            s = self.state.strip().lower()
            if s in ("heat", "heating"):
                return True
            if s in ("noheat", "no_heat", "idle", "off"):
                return False
        if self.valve_opening is not None:
            return self.valve_opening > 0
        return None


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def decode_operation(dp172: Any) -> tuple[bool | None, bool | None]:
    """DP 172 (operation_mode, mirror of Zigbee attribute 0x0025) -> (schedule, preheat).

    bit 0 = schedule active, bit 1 = preheat; 0 = manual. (None, None) if the DP is absent.
    """
    v = _int(dp172)
    if v is None:
        return None, None
    return bool(v & 1), bool(v & 2)


def head_state(dev: DanfossDevice, room_id: str | None = None) -> dict[str, Any]:
    """Contract `HeadState` (§3.3) built from a decoded device.

    The status fields (battery, valve, window, lock, faults) are read-only and come straight from
    the already-decoded device (no extra cloud call): they let the UI (axis D) show per-head health.
    """
    return {
        "device_id": dev.id,
        "name": dev.name,
        "room_id": room_id,
        "online": dev.online,
        "mode_displayed": dev.mode,
        "schedule_active": dev.schedule_active,
        "preheat": dev.preheat,
        "mounting": bool(dev.mounting),
        "inconsistent": dev.inconsistent,
        "battery": dev.battery,
        "valve_opening": dev.valve_opening,
        "heat_request": dev.heat_request,
        "window_open": dev.window_open,
        "window_detection": dev.window_detection,
        "child_lock": dev.child_lock,
        "faults": list(dev.faults),
    }


def _online(device: dict[str, Any]) -> bool:
    zb = (device.get("moduleMap") or {}).get("zigbee") or {}
    if "isOnline" in zb:
        return bool(zb.get("isOnline"))
    return bool(device.get("isOnline", device.get("online", False)))


def _apply_dps(d: DanfossDevice, dps: dict[str, Any]) -> None:
    """Fill a device's DP-derived fields from its ``dps`` map (identity/topology untouched).

    Split out of ``decode_device`` so the same decoding is reused to re-derive the fast-changing
    state when fresher DPs are overlaid onto an already-known device (``apply_dps_overlay``).
    """
    if d.category == "sensor":
        # Danfoss Ally room sensor (014G2480, Tuya category ``wsdcg``, product ``mxzo5rhf``):
        # DP 1 ``temp_current`` (tenths of °C), DP 2 ``humidity_value`` (tenths of %), DP 4
        # ``battery_percentage`` (whole %). Read only; it has none of the head DPs. Observed on the
        # real install's device list (temp 17.5 °C, humidity 56.1 %, battery 79 %), 2026-11-21.
        d.temperature = _num(dps.get("1"), 10)
        d.humidity = _num(dps.get("2"), 10)
        d.battery = _num(dps.get("4"))
        return
    d.temperature = _num(dps.get("24"), 10)
    for dp, key in _SETPOINT_DPS.items():
        x = _num(dps.get(dp), 10)
        if x is not None:
            d.setpoints[key] = x
    d.mode = dps.get("2")
    # Active setpoint = the one of the current schedule slot. DP 171 (OccupiedSetpoint) tracks it,
    # in centi-°C (confirmed on real hardware and in the bulk device.list capture: raw 1400 = 14.0,
    # 2100 = 21.0), despite the official schema nominally listing it unscaled. When absent, derive
    # it from the current mode (DP 2) + the per-mode settings; DP 16 (temp_set) is the last resort
    # (it reports the comfort setpoint regardless of the slot, the R-1 bug).
    d.setpoint = _num(dps.get("171"), 100)
    if d.setpoint is None:
        d.setpoint = d.setpoints.get(_MODE_ACTIVE_KEY.get(d.mode or "", ""))
    if d.setpoint is None:
        d.setpoint = _num(dps.get("16"), 10)
    d.state = dps.get("3")
    d.battery = _num(dps.get("34"))
    d.valve_opening = _num(dps.get("170"))
    d.heat_request = _bool(dps.get("162"))
    d.child_lock = _bool(dps.get("30"))
    d.window_detection = _bool(dps.get("116"))
    d.max_setpoint = _num(dps.get("18"), 10)
    d.min_setpoint = _num(dps.get("27"), 10)
    d.schedule_active, d.preheat = decode_operation(dps.get("172"))
    d.mounting = _bool(dps.get("149"))
    d.viewing_direction = dps.get("101") if isinstance(dps.get("101"), bool) else None
    d.orientation = dps.get("133") if dps.get("133") in ("Horizontal", "Vertical") else None
    if d.category == "relay":  # DP 1 on a relay = on/off (on a head it is preheat)
        d.relay_on = _bool(dps.get("1"))
    ws = dps.get("25")
    if ws is not None and ws != "":
        d.window_open = _bool(ws)
    for dp in ("45", "184"):
        v = dps.get(dp)
        if v not in (None, "", 0, "0", [], "none"):
            d.faults.extend(decode_fault(v))


def decode_device(device: dict[str, Any]) -> DanfossDevice:
    """Raw device (``tuya.m.my.group.device.list``) -> normalised DanfossDevice."""
    dps: dict[str, Any] = {str(k): v for k, v in (device.get("dps") or {}).items()}
    dev_id: str = device.get("devId") or ""
    d = DanfossDevice(
        id=dev_id,
        name=(device.get("name") or "").strip(),
        category=category_of(device.get("category")),
        product_id=device.get("productId"),
        node_id=device.get("nodeId"),
        mesh_id=device.get("meshId"),
        online=_online(device),
        dps=dps,
    )
    _apply_dps(d, dps)
    return d


def apply_dps_overlay(base: DanfossDevice, fresh_dps: dict[str, Any]) -> DanfossDevice:
    """A copy of ``base`` with ``fresh_dps`` merged over its DPs and the DP fields re-derived.

    R-3 (freshness): the bulk ``device.list`` route lags for minutes after a command, while the
    per-device route already reflects it. This overlays the fresh per-device DPs (valve, active
    setpoint, work_state, mode...) onto the device the bulk poll produced, keeping its stable
    identity/topology and online flag. ``fresh_dps`` wins over the bulk DPs key by key; DPs absent
    from ``fresh_dps`` keep their bulk value. Returns a NEW device (the caller swaps it into the
    poll result), never mutating ``base``.
    """
    merged = {**base.dps, **{str(k): v for k, v in (fresh_dps or {}).items()}}
    d = DanfossDevice(
        id=base.id,
        name=base.name,
        category=base.category,
        product_id=base.product_id,
        node_id=base.node_id,
        mesh_id=base.mesh_id,
        online=base.online,
        dps=merged,
    )
    _apply_dps(d, merged)
    return d
