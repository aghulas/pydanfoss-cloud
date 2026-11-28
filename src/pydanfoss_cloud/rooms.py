"""Rooms (Tuya groups): model and aggregation of the member heads' state.

Pure functions and dataclasses, with no Home Assistant or network dependency (reusable layer).
Danfoss drives a multi-head room through its GROUP: schedule and setpoints are shared there. So we
expose one thermostat per ROOM whose state aggregates that of the member heads.

Aggregation used:
- temperature: mean of the **online** heads (offline ones would freeze a misleading value);
- setpoints (active, comfort/setback) and mode: those of an online reference head - heads in the
  same group are synchronised, so any online member is authoritative;
- heating: true if **at least one** head opens its valve;
- online: true if at least one head in the group is online.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from .decode import DanfossDevice


@dataclass(frozen=True)
class Room:
    """A room = one Tuya group (resType 2) and the list of its member heads."""

    group_id: str
    name: str
    pid: str | None
    home_gid: str
    members: tuple[str, ...]


@dataclass
class RoomState:
    """Aggregated state of a room for the room thermostat (read-only)."""

    group_id: str
    name: str
    pid: str | None
    head_count: int
    online: bool = False
    temperature: float | None = None
    setpoint: float | None = None  # active setpoint (°C)
    setpoints: dict[str, float] = field(default_factory=dict)
    mode: str | None = None
    heating: bool | None = None
    min_setpoint: float | None = None
    max_setpoint: float | None = None
    # Identity carried through for the ``bridge/rooms`` contract (companion-contract.md §3): the
    # home the group belongs to and its member head ids, which the Companion matches against its
    # own device map to resolve the room's heads.
    home_gid: str | None = None
    members: tuple[str, ...] = ()
    # The room sensor (014G2480) the account places in this group's Tuya room, or None: see
    # ``attach_room_sensors``. Published as ``sensor`` on ``bridge/rooms``.
    sensor: str | None = None


def attach_room_sensors(
    rooms: list[RoomState],
    devices: list[DanfossDevice],
    device_rooms: dict[str, str],
) -> tuple[list[RoomState], int]:
    """Set each group's ``sensor`` from the account's device -> Tuya room map (``device_rooms``).

    Proven links only, never a name match:

    - a group's Tuya room is the single ``roomId`` shared by ALL its member heads that carry one;
      no head with a room, or heads in different rooms, gives the group no room (and no sensor);
    - the group's sensor is the room sensor in that room, only when exactly ONE sensor is there.

    Returns the rooms (new objects, the input is not changed) and the number of groups whose heads
    disagree on their room, for the caller to log.
    """
    sensors_by_room: dict[str, list[str]] = {}
    for dev in devices:
        room = device_rooms.get(dev.id)
        if dev.is_sensor and room:
            sensors_by_room.setdefault(room, []).append(dev.id)
    out: list[RoomState] = []
    conflicts = 0
    for state in rooms:
        head_rooms = {device_rooms[m] for m in state.members if m in device_rooms}
        if len(head_rooms) > 1:
            conflicts += 1
        candidates = sensors_by_room.get(next(iter(head_rooms)), []) if len(head_rooms) == 1 else []
        out.append(replace(state, sensor=candidates[0] if len(candidates) == 1 else None))
    return out, conflicts


def aggregate_room(room: Room, members: list[DanfossDevice]) -> RoomState:
    """Build the aggregated state of a room from its present member heads."""
    online = [m for m in members if m.online]
    temps = [m.temperature for m in online if m.temperature is not None]
    temperature = round(sum(temps) / len(temps), 1) if temps else None
    ref = online[0] if online else (members[0] if members else None)
    # Heating = the heads' own work_state (DP 3), not a valve guess: a room is heating if any online
    # head calls for heat. None when no online head reports a known state.
    heats = [m.heating for m in online]
    heating = (any(heats) if any(h is not None for h in heats) else None) if online else None
    return RoomState(
        group_id=room.group_id,
        name=room.name,
        pid=room.pid,
        head_count=len(room.members),
        online=bool(online),
        temperature=temperature,
        setpoint=ref.setpoint if ref else None,
        setpoints=dict(ref.setpoints) if ref else {},
        mode=ref.mode if ref else None,
        heating=heating,
        min_setpoint=ref.min_setpoint if ref else None,
        max_setpoint=ref.max_setpoint if ref else None,
        home_gid=room.home_gid,
        members=room.members,
    )
