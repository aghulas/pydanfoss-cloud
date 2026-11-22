"""Device/protocol constants of the library (independent of Home Assistant).

DP-code source of truth: danfoss-app-capture/dp_schema_officiel.md (product 03wvrwpv).
These constants belong to the reusable layer (future pydanfoss-cloud library); the HA integration
re-exports them from its own const.py.
"""

from __future__ import annotations

# DP codes (official names: see dp_schema_officiel.md). Known pitfalls:
#   DP 16  = temp_set (EFFECTIVE setpoint, tenths of °C) - read/written as the setpoint
#   DP 171 = OccupiedSetpoint (scale TO CONFIRM on real hardware)
#   DP 18  = upper_temp (MAX SETPOINT, not the setpoint)
#   DP 114 = manual-mode setpoint; DP 1 = preheat (cloud = gateway cache)
#   DP 30  = child lock; DP 116 = window detection; DP 162 = heat request
DP_TEMP_SET = 16
DP_SETPOINT = 171
DP_UPPER_TEMP = 18
DP_LOWER_TEMP = 27  # lower_temp (MIN SETPOINT)
DP_MANUAL_SETPOINT = 114  # manual_mode_fast
DP_AT_HOME_SETPOINT = 118  # at_home_setting ("comfort" period of the schedule)
DP_LEAVING_HOME_SETPOINT = 119  # leaving_home_setting ("setback" period)
DP_PAUSE_SETPOINT = 120  # pause_setting
DP_HOLIDAY_SETPOINT = 121  # holiday_setting
DP_PREHEAT = 1
DP_CHILD_LOCK = 30
DP_WINDOW_DETECT = 116
DP_HEAT_DEMAND = 162
DP_VIEWING_DIRECTION = 101  # ViewingDirection (bool): display orientation of the head
DP_ORIENTATION = 133  # vertical_mounting (enum "Horizontal" / "Vertical")
ORIENTATION_VALUES = ("Horizontal", "Vertical")  # exact strings of the app's frame

# Setpoint write: current mode (DP 2) -> DP of the setting to write.
# Proven on real hardware: writing DP16 temp_set DOES NOT HOLD on a head on schedule (the schedule
# rewrites it). The durable lever is the setpoint of the ACTIVE MODE's SETTING (temp_set follows).
MODE_SETPOINT_DP = {
    "at_home": DP_AT_HOME_SETPOINT,
    "leaving_home": DP_LEAVING_HOME_SETPOINT,
    "pause": DP_PAUSE_SETPOINT,
    "holiday": DP_HOLIDAY_SETPOINT,
    "holiday_sat": DP_HOLIDAY_SETPOINT,
    "manual": DP_MANUAL_SETPOINT,
}

# --- DURABLE setpoint write, the "real" path 1a: at ROOM level (group) ------------------------
# Corrects PR #8 (merged): writing the MODE DP on the HEAD (settings.change) DOES NOT HOLD either
# (verified on real hardware 2026-11-16, 0.1.0). The method the app actually uses (captures of
# 2026-11-15/16, _CONSIGNES_PIECE of the MCP connector, replayed and read back in the heads):
#   1. publish the setpoint on the room's GROUP via s.m.dev.group.dp.publish {dps, groupId};
#   2. THEN rewrite the room's schedule (one day is enough, the cloud pushes the whole week to the
#      heads with the setpoints of the moment), otherwise the heads keep the old value in their
#      scheduled transitions.
# Scale: tenths of °C, 0.5 step - same as encode_setpoint (round(°C*2)*5, 50..350).
#   comfort = DP 118 (at_home_setting)
#   setback = DP 109 AND DP 119 (both) - leaving_home_setting + the twin captured from the app
DP_COMFORT_SETPOINT = 118
DP_SETBACK_SETPOINT = (109, 119)

# Current mode (DP 2) -> ROOM setpoint DP(s) to publish on the group. Only comfort/setback are
# proven by capture; the other modes have no verified path-1a group method.
MODE_GROUP_SETPOINT_DP: dict[str, tuple[int, ...]] = {
    "at_home": (DP_COMFORT_SETPOINT,),
    "leaving_home": DP_SETBACK_SETPOINT,
}

# Tuya "week timer" resource types: 2 = room (group), 1 = isolated head.
RES_TYPE_GROUP = 2
RES_TYPE_DEVICE = 1

# The real preheat lever is the standard Zigbee attribute (local tunnel only).
ZB_ATTR_PROGRAMMING_MODE = 0x0025  # bit 0 schedule, bit 1 preheat

# Setpoint (write): bounds and step of the product schema (tenths of °C).
SETPOINT_MIN = 5.0
SETPOINT_MAX = 35.0
SETPOINT_STEP = 0.5

# Models / products on which writing is wired (to extend install by install).
WRITE_VERIFIED_MODELS = ("radiator thermostat", "icon2 rt")
WRITE_VERIFIED_PRODUCTS = ("03wvrwpv",)  # Danfoss Ally thermostatic head
