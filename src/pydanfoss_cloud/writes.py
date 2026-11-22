"""Write safety: value validation and a model guard-rail (path 1a).

Different from the MCP: no prepare->confirm token here. Safety comes from VALIDATING the values
before sending and from entities being unavailable rather than lying (see CLAUDE.md).
"""

from __future__ import annotations

from .client import CloudWriteError
from .const import ORIENTATION_VALUES, WRITE_VERIFIED_PRODUCTS
from .decode import DanfossDevice


def write_allowed(dev: DanfossDevice) -> bool:
    """True if the write mechanism is wired for this product (model guard-rail)."""
    return bool(dev.product_id) and dev.product_id in WRITE_VERIFIED_PRODUCTS


# Cloud-MQTT isolated-head write path, one DP at a time (docs/architecture.md, "Rules for adding a
# route"; procedure: docs/first-write-procedure.md).
#
# - MQTT_WRITE_DPS: PROVEN. A supervised write on real hardware was read back from an independent
#   source. Open as soon as the cloud-MQTT option is on.
# - MQTT_CANDIDATE_DPS: IMPLEMENTED, NOT PROVEN. Closed by default; a candidate opens only when it
#   is explicitly listed for a supervised trial (the ``supervised`` argument below, fed by the
#   integration's ``cloud_mqtt_supervised_dps`` option). After its supervised write succeeds, a DP
#   moves from the second set to the first in a change of its own.
# All validated on real hardware (docs/first-write-procedure.md). On 2026-11-19: 30 child lock;
# 116 window detection, 18 upper bound and 27 lower bound, each read back through the local
# tunnel and in the app; 120 pause setpoint (also written with 27, in one frame, when the lower
# bound rises above it), read back in the app and the cloud only — no tunnel read exists for it.
# On 2026-11-19/20, on an isolated head following its schedule: 118 comfort setpoint, published
# with 16 (temp_set) as the app does, and 119 setback setpoint, each read back through the local
# tunnel (the head rewrites its own schedule transitions, no schedule rewrite needed) and in the
# app. DP 16 also sets the head's current setpoint at once; the head returns to its schedule's
# value by itself within five minutes (contract §4.2).
MQTT_WRITE_DPS = frozenset({30, 116, 18, 27, 120, 118, 16, 119})
# 1 preheat (see below), 101 viewing direction and 133 orientation (mounting vertical /
# horizontal), the three as the app publishes them for an isolated head (contract §4.2). None of
# their supervised trials has passed yet (101 and 133: docs/viewing-orientation-trial.md).
MQTT_CANDIDATE_DPS = frozenset({1, 101, 133})
# DPs whose write the gateway's echo and the cloud read CANNOT confirm: both may answer from the
# gateway's cache while the head has not changed. DP 1 (preheat) is the known case: over HTTP it
# only ever changed that cache; the head's real preheat is bit 1 of Zigbee attribute 0x0025. A
# write of such a DP is confirmed only by reading the head through the local tunnel, so a caller
# without a tunnel read must not send it (docs/cloud-mqtt-preheat-proposal.md). DP 133 is in the
# same case by caution: the head's orientation flag (Danfoss attribute 0x4014) is read through
# the tunnel, so that read, not the cloud's echo of the label, is what confirms it.
MQTT_TUNNEL_CONFIRMED_DPS = frozenset({1, 133})
# DPs whose write the gateway's ECHO cannot confirm, and that have no tunnel read: confirmed only
# by the per-device cloud route, read until it shows the value. DP 101 (viewing direction) lives
# in the user-interface cluster, which the tunnel does not read. The cloud's value is still the
# gateway's view: the trial's proof is the head's display (docs/viewing-orientation-trial.md).
MQTT_CLOUD_CONFIRMED_DPS = frozenset({101})
# Exact string values of the enum DPs of this path (the app's frame, product schema 03wvrwpv).
MQTT_ENUM_VALUES: dict[int, tuple[str, ...]] = {133: ORIENTATION_VALUES}

# Value rule of each DP this path knows: booleans, or tenths of °C within the product schema's
# range for that DP (03wvrwpv), on its 0.5 °C step.
_MQTT_BOOL_DPS = frozenset({30, 116, 1, 101})
MQTT_TENTHS_RANGE: dict[int, tuple[int, int]] = {
    18: (200, 350),  # upper_temp: 20-35 °C
    27: (50, 200),  # lower_temp: 5-20 °C
    120: (50, 350),  # pause_setting: 5-35 °C
    118: (50, 350),  # at_home_setting (comfort)
    16: (50, 350),  # temp_set, sent with 118
    119: (50, 350),  # leaving_home_setting (setback)
}
_TENTHS_STEP = 5


def mqtt_dp_open(dp: int, supervised: frozenset[int] = frozenset()) -> bool:
    """True if ``dp`` may be written over cloud MQTT: proven, or a candidate listed in
    ``supervised`` (a supervised trial). A DP that is neither proven nor a candidate is never open,
    whatever ``supervised`` says."""
    return dp in MQTT_WRITE_DPS or (dp in MQTT_CANDIDATE_DPS and dp in supervised)


def mqtt_write_value(
    dp: int, value: object, supervised: frozenset[int] = frozenset()
) -> bool | int | str:
    """Validate a value destined for the cloud-MQTT write path and return it as it must be sent.
    Raises CloudWriteError when the DP is not open on this path, or the value is not of the DP's
    type, range or step (a clear error, never a blind send)."""
    if not mqtt_dp_open(dp, supervised):
        if dp in MQTT_CANDIDATE_DPS:
            raise CloudWriteError(
                f"DP {dp} is not validated on the cloud-MQTT write path yet (closed until its "
                "supervised first write)"
            )
        raise CloudWriteError(f"DP {dp} is not enabled on the cloud-MQTT write path")
    if dp in _MQTT_BOOL_DPS:
        if not isinstance(value, bool):
            raise CloudWriteError(f"DP {dp} expects a boolean, got {type(value).__name__}")
        return value
    if dp in MQTT_ENUM_VALUES:
        allowed = MQTT_ENUM_VALUES[dp]
        # Exact string, case included: the app sends "Vertical" / "Horizontal", nothing else.
        if not isinstance(value, str) or value not in allowed:
            raise CloudWriteError(f"DP {dp} expects one of {', '.join(allowed)}, got {value!r}")
        return value
    if dp in MQTT_TENTHS_RANGE:
        low, high = MQTT_TENTHS_RANGE[dp]
        if isinstance(value, bool) or not isinstance(value, int):
            raise CloudWriteError(
                f"DP {dp} expects tenths of °C as an integer, got {type(value).__name__}"
            )
        if not low <= value <= high or value % _TENTHS_STEP:
            raise CloudWriteError(
                f"DP {dp} out of range: {value / 10} °C (expected {low / 10}-{high / 10} °C, "
                "0.5 °C step)"
            )
        return value
    raise CloudWriteError(f"DP {dp} has no value rule on the cloud-MQTT write path")


def encode_setpoint(celsius, low: float, high: float, step: float = 0.5) -> int:
    """Setpoint °C -> DP value (tenths), rounded to the step and clamped. Raises CloudWriteError."""
    try:
        c = float(celsius)
    except (TypeError, ValueError) as err:
        raise CloudWriteError(f"non-numeric setpoint: {celsius!r}") from err
    c = round(c / step) * step
    if not low <= c <= high:
        raise CloudWriteError(f"setpoint out of range: {c} °C (expected {low}-{high} °C)")
    return int(round(c * 10))
