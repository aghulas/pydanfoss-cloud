"""Read-only room and heat-demand views for the room coordinator (``docs/z2m-cloud-parity.md`` §5).

Pure functions, no MQTT / Home Assistant / network. Build, from the members' **reported** state and
the coordinator's store, exactly the objects published on ``<base>/bridge/rooms`` and
``<base>/bridge/heat_demand``.

The one model of §5: the coordinator keeps a desired state per room, derives per-head writes from it
(later stages), and publishes **what the heads report, never what it asked**. So ``temperature``,
``setpoint``, ``heating`` … are aggregated from the devices; ``desired`` is echoed from the store;
``drift`` lists the heads whose reported value differs from the desired one.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .decode import DanfossDevice
from .room_store import CoordinatorHoliday, CoordinatorRoom, normalize_address
from .rooms import Room, aggregate_room

# Desired key -> DanfossDevice attribute that carries the head's REPORTED value for it.
_SIMPLE_DRIFT_ATTRS: dict[str, str] = {
    "setpoint": "setpoint",
    "max_setpoint": "max_setpoint",
    "min_setpoint": "min_setpoint",
    "child_lock": "child_lock",
    "window_detection": "window_detection",
}

# Heat-demand message validity: a consumer treats an older (or missing) message as "unknown"
# and applies its own safe default (``docs/z2m-cloud-parity.md`` §5.7).
DEFAULT_HEAT_DEMAND_VALID_S = 900


def _values_differ(desired: Any, reported: Any) -> bool:
    if reported is None:
        return False  # not reported yet -> not counted as drift
    if (
        isinstance(desired, (int, float))
        and not isinstance(desired, bool)
        and isinstance(reported, (int, float))
        and not isinstance(reported, bool)
    ):
        return abs(float(desired) - float(reported)) > 0.1
    return desired != reported


def member_drift_keys(room: CoordinatorRoom, dev: DanfossDevice) -> list[str]:
    """Return the desired keys for which ``dev``'s reported state differs from ``room.desired``."""
    if not room.desired:
        return []
    drifting: list[str] = []
    for key, attr in _SIMPLE_DRIFT_ATTRS.items():
        if key in room.desired and _values_differ(room.desired[key], getattr(dev, attr, None)):
            drifting.append(key)

    desired_mode = room.desired.get("mode")
    if isinstance(desired_mode, str) and desired_mode:
        norm_mode = desired_mode.strip().lower()
        if dev.schedule_active is not None:
            want_schedule = norm_mode == "schedule"
            if dev.schedule_active is not want_schedule:
                drifting.append("mode")
        elif dev.mode is not None and dev.mode.strip().lower() != norm_mode:
            drifting.append("mode")

    if (
        "preheat" in room.desired
        and str(room.desired.get("mode") or "schedule").strip().lower() == "schedule"
        and dev.preheat is not None
        and bool(room.desired["preheat"]) is not bool(dev.preheat)
    ):
        drifting.append("preheat")

    return drifting


def _drift_members(room: CoordinatorRoom, members: list[DanfossDevice]) -> list[str]:
    """Member addresses whose reported state differs from ``room.desired`` (empty if no desired)."""
    if not room.desired:
        return []
    by_norm = {normalize_address(d.id): d for d in members}
    drifting: list[str] = []
    for addr in room.members:
        dev = by_norm.get(normalize_address(addr))
        if dev is not None and member_drift_keys(room, dev):
            drifting.append(addr)
    return drifting


def build_room_view(
    room: CoordinatorRoom,
    members: list[DanfossDevice],
    sensor: DanfossDevice | None = None,
    *,
    reconcile_blocked: Sequence[str] = (),
) -> dict[str, Any]:
    """Build one ``bridge/rooms`` object for a room from its reported members (+ optional sensor).

    Shape matches what ``danfoss2mqtt`` already publishes (``group_id``, ``name``, ``home_id``,
    ``members``, ``temperature``, ``humidity``, ``setpoint``, ``setpoints``, ``mode``, ``heating``,
    ``schedule``) plus ``desired`` and ``drift`` (``docs/z2m-cloud-parity.md`` §5.1).
    """
    agg = aggregate_room(
        Room(
            group_id=room.room_id,
            name=room.name,
            pid=None,
            home_gid=room.home_id or "",
            members=room.members,
        ),
        members,
    )

    # Room temperature/humidity: the 014G2480 sensor when it is linked and reporting, else the
    # mean of the heads (companion-contract.md §3 / docs/z2m-cloud-parity.md §5.1).
    temperature = agg.temperature
    humidity: float | None = None
    if sensor is not None and sensor.online:
        if sensor.temperature is not None:
            temperature = sensor.temperature
        humidity = sensor.humidity

    return {
        "group_id": room.room_id,
        "name": room.name,
        "prefix": room.prefix,
        "home_id": room.home_id,
        "members": list(room.members),
        "sensor": room.sensor,
        "head_count": agg.head_count,
        "online": agg.online,
        "temperature": temperature,
        "humidity": humidity,
        "setpoint": agg.setpoint,
        "setpoints": dict(agg.setpoints),
        "mode": agg.mode,
        "heating": agg.heating,
        "min_setpoint": agg.min_setpoint,
        "max_setpoint": agg.max_setpoint,
        "schedule": room.schedule,
        "desired": dict(room.desired),
        "drift": _drift_members(room, members),
        "reconcile_blocked": list(reconcile_blocked),
    }


def build_holidays_view(
    holidays: Sequence[CoordinatorHoliday],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Serialize coordinator holidays into the retained ``bridge/holidays`` dict shape."""
    current = now or datetime.now().astimezone()
    out: dict[str, Any] = {}
    for hol in holidays:
        scheduled = False
        try:
            start_dt = datetime.fromisoformat(hol.start)
            end_dt = datetime.fromisoformat(hol.end)
            if start_dt.tzinfo is None and current.tzinfo is not None:
                start_dt = start_dt.replace(tzinfo=current.tzinfo)
            if end_dt.tzinfo is None and current.tzinfo is not None:
                end_dt = end_dt.replace(tzinfo=current.tzinfo)
            scheduled = current < end_dt and not hol.active and current < start_dt
        except ValueError:
            scheduled = False
        out[hol.holiday_id] = {
            "name": hol.name,
            "active": hol.active,
            "scheduled": scheduled,
            "detail": {
                "start": hol.start,
                "end": hol.end,
                "temperature": hol.temperature,
                "rooms": list(hol.room_ids),
            },
        }
    return out


def _room_calls_for_heat(members: list[DanfossDevice]) -> tuple[bool, float]:
    """Whether a room calls for heat, and its highest valve opening, from online members."""
    calling = False
    max_valve = 0.0
    for dev in members:
        if not dev.online:
            continue
        if dev.heat_request or dev.heating:
            calling = True
        if dev.valve_opening is not None:
            max_valve = max(max_valve, float(dev.valve_opening))
    return calling, max_valve


def build_heat_demand(
    room_members: Mapping[str, list[DanfossDevice]],
    room_names: Mapping[str, str] | None = None,
    *,
    valid_for_s: int = DEFAULT_HEAT_DEMAND_VALID_S,
    now: float | None = None,
) -> dict[str, Any]:
    """Aggregate per-room heat demand into the ``bridge/heat_demand`` object (§5.7).

    ``room_members`` maps room_id -> its reported member devices. A room calls for heat when any
    online member reports ``heat_required`` or an open valve. The message carries a timestamp and a
    validity window so a consumer can treat a stale or missing message as "unknown".
    """
    names = room_names or {}
    rooms_calling: list[dict[str, Any]] = []
    overall_max = 0.0
    for room_id, members in room_members.items():
        calling, max_valve = _room_calls_for_heat(members)
        overall_max = max(overall_max, max_valve)
        if calling:
            rooms_calling.append(
                {"group_id": room_id, "name": names.get(room_id, room_id), "valve": max_valve}
            )
    return {
        "calling": bool(rooms_calling),
        "rooms_calling": rooms_calling,
        "max_valve": overall_max,
        "ts": int(now if now is not None else time.time()),
        "valid_for_s": valid_for_s,
    }
