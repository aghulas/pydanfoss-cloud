"""Head settings the standalone bridge (``danfoss2mqtt``) can write: child lock, open-window
detection, setpoint bounds.

Every channel used here is already proven on real hardware (contract §4.2, 2026-11-19):

- a **grouped** head (member of a room): the room's group publish ``s.m.dev.group.dp.publish``
  carries DP 30 (child lock), 116 (window detection) and 18 (upper bound). It applies to **every
  head of the room**, not to the one head the ``/set`` was sent to. There is no group route for the
  lower bound (DP 27);
- an **isolated** head: a DP publish over Tuya cloud MQTT (``publishDps``), proven for 30, 116, 18
  and 27 (``writes.MQTT_WRITE_DPS``), confirmed by the gateway's echo or a per-device cloud read.

The bridge's own wiring of these channels is new code, so it is **closed by default**
(``HeadSettingsGate``) and opens per the supervised first-write procedure: on one designated test
head first (``trial_head``), then on every head once that trial passed.

Pure logic only (no I/O): the bridge daemon does the sends.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .client import CloudWriteError
from .const import WRITE_VERIFIED_PRODUCTS
from .decode import DanfossDevice
from .writes import mqtt_write_value

DP_CHILD_LOCK = 30
DP_WINDOW_DETECTION = 116
DP_UPPER_BOUND = 18
DP_LOWER_BOUND = 27
DP_PAUSE_SETPOINT = 120

# Zigbee2MQTT ``/set`` keys of the settings, by the channel that can carry them.
GROUP_SETTING_KEYS = frozenset(
    {"keypad_lockout", "child_lock", "window_open_feature", "max_heat_setpoint_limit"}
)
MQTT_SETTING_KEYS = GROUP_SETTING_KEYS | {"min_heat_setpoint_limit"}
SETTING_KEYS = MQTT_SETTING_KEYS


@dataclass(frozen=True)
class HeadSettingsGate:
    """Whether the bridge writes head settings, and on which heads.

    ``enabled`` False (the default): no setting is written, every setting key stays refused.
    ``trial_head`` set: only that head (by device id) can be written — the supervised first write.
    """

    enabled: bool = False
    trial_head: str | None = None

    def open_for(self, dev_id: str) -> bool:
        return self.enabled and (not self.trial_head or self.trial_head == dev_id)


def allowed_setting_keys(
    dev: DanfossDevice, *, gate: HeadSettingsGate, grouped: bool, mqtt_ready: bool
) -> frozenset[str]:
    """The setting keys ``dev`` accepts on ``/set`` right now. Empty when the gate is closed for it,
    when it is not a head of a verified model, or when no proven channel can reach it now (an
    isolated head while the cloud-MQTT link cannot publish to its gateway)."""
    if not dev.is_head or dev.product_id not in WRITE_VERIFIED_PRODUCTS:
        return frozenset()
    if not gate.open_for(dev.id):
        return frozenset()
    if grouped:
        return GROUP_SETTING_KEYS
    return MQTT_SETTING_KEYS if mqtt_ready else frozenset()


def _lock_value(key: str, raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        low = raw.strip().lower()
        if low in ("lock", "locked", "on", "true"):
            return True
        if low in ("unlock", "unlocked", "off", "false"):
            return False
    raise CloudWriteError(f"{key} expects lock / unlock, got {raw!r}")


def _tenths(key: str, raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise CloudWriteError(f"{key} expects a temperature in °C, got {raw!r}")
    return round(float(raw) * 10)


def setting_dps(payload: Mapping[str, Any], dev: DanfossDevice) -> dict[int, bool | int]:
    """Translate the setting keys of a ``/set`` payload into validated DPs.

    Raises ``CloudWriteError`` on a value of the wrong type, out of range or off the 0.5 °C step
    (the rules of ``writes.mqtt_write_value``, shared by both channels), on a child lock asked both
    on and off in one payload, or on a lower bound above the pause setpoint when that setpoint is
    unknown. A lower bound above the known pause setpoint carries the pause setpoint along, as the
    app does (the head refuses a pause setpoint below its lower bound).
    """
    dps: dict[int, bool | int] = {}
    locks = {
        _lock_value(key, payload[key]) for key in ("keypad_lockout", "child_lock") if key in payload
    }
    if len(locks) > 1:
        raise CloudWriteError("child lock asked both on and off in one payload")
    if locks:
        dps[DP_CHILD_LOCK] = locks.pop()
    if "window_open_feature" in payload:
        raw = payload["window_open_feature"]
        if not isinstance(raw, bool):
            raise CloudWriteError(f"window_open_feature expects a boolean, got {raw!r}")
        dps[DP_WINDOW_DETECTION] = raw
    if "max_heat_setpoint_limit" in payload:
        dps[DP_UPPER_BOUND] = _tenths("max_heat_setpoint_limit", payload["max_heat_setpoint_limit"])
    if "min_heat_setpoint_limit" in payload:
        lower = _tenths("min_heat_setpoint_limit", payload["min_heat_setpoint_limit"])
        dps[DP_LOWER_BOUND] = lower
        pause = (dev.setpoints or {}).get("pause")
        if pause is None:
            raise CloudWriteError("lower bound refused: the head's pause setpoint is unknown")
        if lower > round(float(pause) * 10):
            dps[DP_PAUSE_SETPOINT] = lower
    if (
        DP_UPPER_BOUND in dps
        and DP_LOWER_BOUND in dps
        and dps[DP_LOWER_BOUND] >= dps[DP_UPPER_BOUND]
    ):
        raise CloudWriteError("lower bound must stay below the upper bound")
    out: dict[int, bool | int] = {}
    for dp, value in dps.items():
        checked = mqtt_write_value(dp, value)
        if isinstance(checked, str):  # no string DP among the settings; keep the type exact
            raise CloudWriteError(f"DP {dp} has no string value among the head settings")
        out[dp] = checked
    return out
