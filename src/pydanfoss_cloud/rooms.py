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

from dataclasses import dataclass, field

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
    )
