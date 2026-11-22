"""Shared v2.x Zigbee2MQTT <-> DanfossDevice contract codec (``docs/companion-contract.md``).

Used on both sides of the Mosquitto broker:

1. **Bridge side (`danfoss2mqtt`)**: ``encode_z2m_descriptor`` and ``encode_z2m_state`` serialize
   any ``DanfossDevice`` (radiator head ``014G2461`` / ``03wvrwpv``, room sensor ``014G2480`` /
   ``mxzo5rhf``, or boiler relay ``ypieax1h``) into the exact Zigbee2MQTT JSON dialect.
2. **Companion side (`ha-danfoss-ally` v2)**: ``decode_z2m_descriptor``, ``decode_z2m_state``, and
   ``encode_z2m_set_command`` normalize devices and states coming from **either** ``zigbee2mqtt`` or
   ``danfoss2mqtt`` into the same ``DanfossDevice`` model and build ``<prefix>/<name>/set``.
"""

from __future__ import annotations

import math
from typing import Any

from .decode import DanfossDevice

# Known Danfoss models across Zigbee2MQTT and Tuya path 1a.
HEAD_MODELS = frozenset({"014G2461", "03wvrwpv", "Ally eTRV"})
SENSOR_MODELS = frozenset({"014G2480", "mxzo5rhf"})
RELAY_MODELS = frozenset({"014G2400", "ypieax1h", "Ally Boiler Relay"})


def clean_friendly_name(name: str, fallback: str = "") -> str:
    """Return a topic-safe Zigbee2MQTT ``friendly_name``."""
    raw = (name or fallback or "").strip() or fallback
    for ch in ("/", "+", "#"):
        raw = raw.replace(ch, "_")
    return raw


def programming_mode_to_z2m(schedule_active: bool | None, preheat: bool | None) -> str | None:
    """Map ``(schedule_active, preheat)`` (DP 172 bits 0 & 1 / tunnel 0x0025) to Z2M's enum."""
    if schedule_active is None:
        return None
    if not schedule_active:
        return "setpoint"
    return "schedule_with_preheat" if preheat else "schedule"


def programming_mode_from_z2m(mode: Any) -> tuple[bool | None, bool | None]:
    """Inverse of ``programming_mode_to_z2m``: Z2M enum -> ``(schedule_active, preheat)``."""
    if not isinstance(mode, str):
        return None, None
    m = mode.strip().lower()
    if m == "setpoint":
        return False, False
    if m == "schedule":
        return True, False
    if m in ("schedule_with_preheat", "eco"):
        return True, True
    return None, None


# --- Bridge side: DanfossDevice -> Zigbee2MQTT JSON --------------------------------------------


def encode_z2m_descriptor(dev: DanfossDevice) -> dict[str, Any]:
    """Build a ``<prefix>/bridge/devices`` entry for ``dev`` (head, sensor, or boiler relay)."""
    base = dev.node_id or dev.id or ""
    ieee = base if base.startswith("0x") else f"0x{base}"
    if dev.is_sensor:
        model = "014G2480"
        desc = "Danfoss Ally temperature and humidity sensor"
    elif dev.is_relay:
        model = "014G2400"
        desc = "Danfoss Ally boiler relay"
    else:
        model = "014G2461"
        desc = "Danfoss Ally radiator thermostat"

    return {
        "friendly_name": clean_friendly_name(dev.name, dev.id),
        "ieee_address": ieee,
        "type": "EndDevice",
        "supported": True,
        "disabled": False,
        "definition": {
            "vendor": "Danfoss",
            "model": model,
            "description": desc,
        },
        "danfoss": {
            "dev_id": dev.id,
            "product_id": dev.product_id,
            "mesh_id": dev.mesh_id,
            "category": dev.category,
        },
    }


def encode_z2m_state(
    dev: DanfossDevice,
    local_attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize ``dev`` (+ optional tunnel attributes) to a ``<prefix>/<name>`` state dict."""
    if dev.is_sensor:
        out_sensor: dict[str, Any] = {}
        if dev.temperature is not None:
            out_sensor["temperature"] = dev.temperature
        if dev.humidity is not None:
            out_sensor["humidity"] = dev.humidity
        if dev.battery is not None:
            out_sensor["battery"] = dev.battery
        return out_sensor

    if dev.is_relay:
        if dev.relay_on is None:
            return {}
        return {
            "state": "ON" if dev.relay_on else "OFF",
            "relay_on": dev.relay_on,
        }

    out: dict[str, Any] = {"system_mode": "heat"}
    if dev.temperature is not None:
        out["local_temperature"] = dev.temperature
    if dev.setpoint is not None:
        out["occupied_heating_setpoint"] = dev.setpoint
    if dev.valve_opening is not None:
        out["pi_heating_demand"] = dev.valve_opening
    heating = dev.heating
    if heating is not None:
        out["running_state"] = "heat" if heating else "idle"
    if dev.battery is not None:
        out["battery"] = dev.battery
    if dev.window_open is not None:
        out["window_open"] = dev.window_open
    if dev.window_detection is not None:
        out["window_open_feature"] = dev.window_detection
    if dev.child_lock is not None:
        out["keypad_lockout"] = "lock" if dev.child_lock else "unlock"
    if dev.max_setpoint is not None:
        out["max_heat_setpoint_limit"] = dev.max_setpoint
    if dev.min_setpoint is not None:
        out["min_heat_setpoint_limit"] = dev.min_setpoint
    if dev.heat_request is not None:
        out["heat_required"] = dev.heat_request
    if dev.mounting is not None:
        out["mounted_mode_active"] = dev.mounting
    prog_mode = programming_mode_to_z2m(dev.schedule_active, dev.preheat)
    if prog_mode is not None:
        out["programming_operation_mode"] = prog_mode
    if dev.viewing_direction is not None:
        out["viewing_direction"] = dev.viewing_direction
    if dev.orientation is not None:
        out["thermostat_orientation"] = dev.orientation.strip().lower() == "vertical"

    if local_attrs:
        for src_key, z2m_key in (
            ("setpoint_source", "setpoint_change_source"),
            ("radiator_covered", "radiator_covered"),
            ("heat_available", "heat_available"),
            ("regulation_speed", "algorithm_scale_factor"),
            ("setpoint_offset", "regulation_setpoint_offset"),
            ("descale_day", "day_of_week"),
            ("descale_time", "trigger_time"),
            ("adaptation_run", "adaptation_run_status"),
            ("window_state", "window_open_internal"),
        ):
            if local_attrs.get(src_key) is not None:
                out[z2m_key] = local_attrs[src_key]

    ext: dict[str, Any] = {}
    if dev.setpoints:
        ext["setpoints"] = dict(dev.setpoints)
    if dev.mode is not None:
        ext["mode"] = dev.mode
    if dev.faults:
        ext["faults"] = list(dev.faults)
    if ext:
        out["danfoss"] = ext
    return out


# --- Companion side: Zigbee2MQTT / danfoss2mqtt JSON -> DanfossDevice --------------------------


def classify_z2m_descriptor(entry: dict[str, Any]) -> str | None:
    """Return ``"head"``, ``"sensor"``, or ``"relay"`` if ``entry`` is Danfoss, else ``None``."""
    danfoss_meta = entry.get("danfoss")
    if isinstance(danfoss_meta, dict) and danfoss_meta.get("category") in (
        "head",
        "sensor",
        "relay",
    ):
        return str(danfoss_meta["category"])

    defn = entry.get("definition") or {}
    vendor = str(defn.get("vendor") or entry.get("manufacturer") or "").strip().lower()
    model = str(defn.get("model") or entry.get("model_id") or "").strip()

    if model in HEAD_MODELS:
        return "head"
    if model in SENSOR_MODELS:
        return "sensor"
    if model in RELAY_MODELS:
        return "relay"
    if "danfoss" in vendor:
        desc = str(defn.get("description") or "").lower()
        if "sensor" in desc or "humidity" in desc:
            return "sensor"
        if "relay" in desc or "boiler" in desc:
            return "relay"
        return "head"
    return None


def decode_z2m_descriptor(entry: dict[str, Any]) -> DanfossDevice | None:
    """Build an initial ``DanfossDevice`` from a ``<prefix>/bridge/devices`` entry.

    Works identically for native ``zigbee2mqtt`` entries and ``danfoss2mqtt`` entries.
    Returns ``None`` for non-Danfoss devices (e.g. a Sonoff router or Aqara contact sensor on z2m).
    """
    category = classify_z2m_descriptor(entry)
    if category is None:
        return None

    raw_meta = entry.get("danfoss")
    danfoss_meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
    ieee = str(entry.get("ieee_address") or "").strip()
    friendly = str(entry.get("friendly_name") or ieee).strip()
    dev_id = str(danfoss_meta.get("dev_id") or ieee or friendly)
    defn = entry.get("definition") or {}
    product_id = (
        str(danfoss_meta.get("product_id") or defn.get("model") or entry.get("model_id") or "")
        or None
    )

    return DanfossDevice(
        id=dev_id,
        name=friendly,
        category=category,
        product_id=product_id,
        node_id=ieee.removeprefix("0x") if ieee else None,
        mesh_id=danfoss_meta.get("mesh_id"),
        online=not bool(entry.get("disabled", False)),
    )


def decode_z2m_state(
    dev: DanfossDevice,
    payload: dict[str, Any],
    *,
    online: bool | None = None,
) -> DanfossDevice:
    """Apply a ``<prefix>/<friendly_name>`` state JSON (from ``zigbee2mqtt`` or ``danfoss2mqtt``)
    onto ``dev`` and return the updated ``DanfossDevice``."""
    if online is not None:
        dev.online = online

    if dev.is_sensor:
        if "temperature" in payload and payload["temperature"] is not None:
            dev.temperature = float(payload["temperature"])
        if "humidity" in payload and payload["humidity"] is not None:
            dev.humidity = float(payload["humidity"])
        if "battery" in payload and payload["battery"] is not None:
            dev.battery = float(payload["battery"])
        return dev

    if dev.is_relay:
        if "relay_on" in payload and isinstance(payload["relay_on"], bool):
            dev.relay_on = payload["relay_on"]
        elif "state" in payload and isinstance(payload["state"], str):
            dev.relay_on = payload["state"].strip().upper() == "ON"
        return dev

    if "local_temperature" in payload and payload["local_temperature"] is not None:
        dev.temperature = float(payload["local_temperature"])
    if "occupied_heating_setpoint" in payload and payload["occupied_heating_setpoint"] is not None:
        dev.setpoint = float(payload["occupied_heating_setpoint"])
    elif (
        "occupied_heating_setpoint_scheduled" in payload
        and payload["occupied_heating_setpoint_scheduled"] is not None
    ):
        dev.setpoint = float(payload["occupied_heating_setpoint_scheduled"])
    if "pi_heating_demand" in payload and payload["pi_heating_demand"] is not None:
        dev.valve_opening = float(payload["pi_heating_demand"])
    if "running_state" in payload and isinstance(payload["running_state"], str):
        rs = payload["running_state"].strip().lower()
        dev.state = "Heat" if rs in ("heat", "heating") else "NoHeat"
    if "battery" in payload and payload["battery"] is not None:
        dev.battery = float(payload["battery"])

    if "window_open" in payload and isinstance(payload["window_open"], bool):
        dev.window_open = payload["window_open"]
    elif "window_open_internal" in payload:
        wi = payload["window_open_internal"]
        if isinstance(wi, bool):
            dev.window_open = wi
        elif isinstance(wi, str):
            dev.window_open = wi.strip().lower() in ("open", "external_open")

    if "window_open_feature" in payload and payload["window_open_feature"] is not None:
        dev.window_detection = bool(payload["window_open_feature"])

    if "keypad_lockout" in payload and payload["keypad_lockout"] is not None:
        kl = payload["keypad_lockout"]
        if isinstance(kl, bool):
            dev.child_lock = kl
        elif isinstance(kl, str):
            low = kl.strip().lower()
            dev.child_lock = low.startswith("lock") or low in ("true", "on", "1")

    if "max_heat_setpoint_limit" in payload and payload["max_heat_setpoint_limit"] is not None:
        dev.max_setpoint = float(payload["max_heat_setpoint_limit"])
    if "min_heat_setpoint_limit" in payload and payload["min_heat_setpoint_limit"] is not None:
        dev.min_setpoint = float(payload["min_heat_setpoint_limit"])
    if "heat_required" in payload and payload["heat_required"] is not None:
        dev.heat_request = bool(payload["heat_required"])
    if "mounted_mode_active" in payload and payload["mounted_mode_active"] is not None:
        dev.mounting = bool(payload["mounted_mode_active"])

    if "programming_operation_mode" in payload:
        sched, pre = programming_mode_from_z2m(payload["programming_operation_mode"])
        if sched is not None:
            dev.schedule_active = sched
            dev.preheat = pre

    if "viewing_direction" in payload and payload["viewing_direction"] is not None:
        vd = payload["viewing_direction"]
        if isinstance(vd, bool):
            dev.viewing_direction = vd
        elif isinstance(vd, str):
            dev.viewing_direction = vd.strip().lower() in (
                "flipped",
                "inverted",
                "reverse",
                "true",
                "on",
                "1",
            )
        else:
            dev.viewing_direction = bool(vd)
    if "thermostat_orientation" in payload and payload["thermostat_orientation"] is not None:
        to = payload["thermostat_orientation"]
        if isinstance(to, bool):
            dev.orientation = "Vertical" if to else "Horizontal"
        elif isinstance(to, str):
            low = to.strip().lower()
            if low in ("vertical", "true", "on", "1"):
                dev.orientation = "Vertical"
            elif low in ("horizontal", "false", "off", "0"):
                dev.orientation = "Horizontal"
        else:
            dev.orientation = "Vertical" if bool(to) else "Horizontal"

    ext = payload.get("danfoss")
    if isinstance(ext, dict):
        if isinstance(ext.get("setpoints"), dict):
            dev.setpoints = {
                str(k): float(v) for k, v in ext["setpoints"].items() if isinstance(v, (int, float))
            }
        if isinstance(ext.get("mode"), str):
            dev.mode = ext["mode"]
        if isinstance(ext.get("faults"), list):
            dev.faults = list(ext["faults"])

    return dev


# --- Companion -> Bridge command builder (<prefix>/<friendly_name>/set) ------------------------


def encode_z2m_set_command(
    *,
    setpoint: float | None = None,
    max_setpoint: float | None = None,
    min_setpoint: float | None = None,
    child_lock: bool | None = None,
    window_detection: bool | None = None,
    preheat: bool | None = None,
    external_sensor_temp_c: float | None = None,
    window_open_external: bool | None = None,
    load_room_mean: int | None = None,
) -> dict[str, Any]:
    """Build a ``<prefix>/<friendly_name>/set`` JSON command payload understood by both
    ``zigbee2mqtt`` and ``danfoss2mqtt``."""
    cmd: dict[str, Any] = {}
    if setpoint is not None:
        if not (5.0 <= setpoint <= 35.0) or not math.isclose(round(setpoint * 2), setpoint * 2):
            raise ValueError(f"setpoint {setpoint} out of range (5.0..35.0 °C, step 0.5)")
        cmd["occupied_heating_setpoint"] = float(setpoint)
    if max_setpoint is not None:
        cmd["max_heat_setpoint_limit"] = float(max_setpoint)
    if min_setpoint is not None:
        cmd["min_heat_setpoint_limit"] = float(min_setpoint)
    if child_lock is not None:
        cmd["keypad_lockout"] = "lock" if child_lock else "unlock"
    if window_detection is not None:
        cmd["window_open_feature"] = bool(window_detection)
    if preheat is not None:
        cmd["programming_operation_mode"] = "schedule_with_preheat" if preheat else "schedule"
    if external_sensor_temp_c is not None:
        # Z2M / Danfoss attribute 0x4015 is in 0.01 °C (-8000 = disabled).
        cmd["external_measured_room_sensor"] = int(round(external_sensor_temp_c * 100))
    if window_open_external is not None:
        cmd["window_open_external"] = bool(window_open_external)
    if load_room_mean is not None:
        cmd["load_room_mean"] = int(load_room_mean)
    if not cmd:
        raise ValueError("empty command payload")
    return cmd


__all__ = [
    "HEAD_MODELS",
    "RELAY_MODELS",
    "SENSOR_MODELS",
    "classify_z2m_descriptor",
    "clean_friendly_name",
    "decode_z2m_descriptor",
    "decode_z2m_state",
    "encode_z2m_descriptor",
    "encode_z2m_set_command",
    "encode_z2m_state",
    "programming_mode_from_z2m",
    "programming_mode_to_z2m",
]
