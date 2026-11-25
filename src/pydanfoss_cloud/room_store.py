"""Room store of the standalone room coordinator (``docs/z2m-cloud-parity.md`` §5.1, §6).

The coordinator owns heads that live on a Zigbee2MQTT coordinator with **no Danfoss gateway and no
cloud** (``docs/z2m-cloud-parity.md`` §6). A Zigbee2MQTT room has no server-side existence, so the
coordinator keeps its own definition of rooms here: one JSON file in the add-on's data folder.

Pure data layer — no MQTT, no Home Assistant, no network. The coordinator daemon
(:mod:`pydanfoss_cloud.room_coordinator`) loads this store, aggregates the members' **reported**
state against it, and mutates it on ``<base>/bridge/request/room/*``.

Stage 1 (read-only coordinator) scope: the store is the only thing a ``room/*`` request changes —
a **room definition**, never a head. No ``/set`` is ever published from a request. The
``desired`` and ``schedule`` fields exist in the model (they are what later write stages fan out),
but nothing in Stage 1 drives a head from them; they only feed the read-only ``desired`` / ``drift``
view (``docs/z2m-cloud-parity.md`` §5, "publishes what the heads report, never what it asked").

Invariant (§5.1 "Single owner per head"): a head belongs to **at most one** room. The mutators
refuse a member already owned by another room.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

STORE_VERSION = 1


class RoomStoreError(ValueError):
    """A ``room/*`` request that cannot be applied (unknown room, duplicate member, bad input)."""


def normalize_address(address: str) -> str:
    """Canonical form of a Zigbee address for comparison (lowercase, no ``0x`` prefix).

    Members are kept verbatim as the bridge publishes them (usually ``0x...``); matching a member
    to a reported device is done on this normalized form so ``0xABCD`` and ``abcd`` are one head.
    """
    return address.strip().lower().removeprefix("0x")


@dataclass(frozen=True)
class CoordinatorRoom:
    """One coordinator-defined room: a set of heads on one bridge prefix, plus an optional sensor.

    - ``members`` / ``sensor`` are Zigbee addresses as the bridge publishes them (``ieee_address``).
    - ``desired`` is the room's wanted state (setpoint, mode, room-wide settings). Empty in Stage 1
      until a write stage fills it; read only for the ``desired`` / ``drift`` view meanwhile.
    - ``schedule`` is the room's weekly schedule in the coordinator's own shape (filled by Stage 4).
    """

    room_id: str
    name: str
    prefix: str
    home_id: str | None = None
    members: tuple[str, ...] = ()
    sensor: str | None = None
    desired: dict[str, Any] = field(default_factory=dict)
    schedule: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "room_id": self.room_id,
            "name": self.name,
            "prefix": self.prefix,
            "home_id": self.home_id,
            "members": list(self.members),
            "sensor": self.sensor,
            "desired": dict(self.desired),
            "schedule": self.schedule,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CoordinatorRoom:
        room_id = str(raw.get("room_id") or "").strip()
        name = str(raw.get("name") or room_id).strip()
        prefix = str(raw.get("prefix") or "").strip().rstrip("/")
        if not room_id or not prefix:
            raise RoomStoreError("a stored room needs a room_id and a prefix")
        members = tuple(str(m).strip() for m in (raw.get("members") or []) if str(m).strip())
        sensor_raw = raw.get("sensor")
        sensor = str(sensor_raw).strip() or None if sensor_raw else None
        desired = dict(raw["desired"]) if isinstance(raw.get("desired"), Mapping) else {}
        schedule = raw.get("schedule") if isinstance(raw.get("schedule"), Mapping) else None
        return cls(
            room_id=room_id,
            name=name,
            prefix=prefix,
            home_id=(str(raw["home_id"]).strip() or None) if raw.get("home_id") else None,
            members=members,
            sensor=sensor,
            desired=desired,
            schedule=dict(schedule) if schedule is not None else None,
        )


def _new_room_id() -> str:
    return f"room-{secrets.token_hex(4)}"


def _new_holiday_id() -> str:
    return f"holiday-{secrets.token_hex(4)}"


@dataclass(frozen=True)
class CoordinatorHoliday:
    """One coordinator-managed holiday entry (``docs/z2m-cloud-parity.md`` §5.4, Stage 7).

    - ``start`` and ``end`` are ISO-8601 datetime strings.
    - ``temperature`` is the held away setpoint (°C).
    - ``room_ids`` lists the targeted coordinator rooms.
    - ``active`` records whether the coordinator has entered the holiday period (persisted so a
      missed end edge while stopped is reconciled immediately on restart).
    - ``saved_modes`` records each room's mode before the holiday started.
    """

    holiday_id: str
    start: str
    end: str
    temperature: float
    room_ids: tuple[str, ...] = ()
    name: str = "Holiday"
    active: bool = False
    saved_modes: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "holiday_id": self.holiday_id,
            "name": self.name,
            "start": self.start,
            "end": self.end,
            "temperature": self.temperature,
            "room_ids": list(self.room_ids),
            "active": self.active,
            "saved_modes": dict(self.saved_modes),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CoordinatorHoliday:
        hid = str(raw.get("holiday_id") or "").strip()
        start = str(raw.get("start") or "").strip()
        end = str(raw.get("end") or "").strip()
        if not hid or not start or not end:
            raise RoomStoreError("a stored holiday needs holiday_id, start and end")
        try:
            temp = float(raw["temperature"])
        except (KeyError, TypeError, ValueError) as err:
            raise RoomStoreError(f"holiday {hid!r} needs a numeric temperature") from err
        room_ids = tuple(str(r).strip() for r in (raw.get("room_ids") or []) if str(r).strip())
        saved = raw.get("saved_modes")
        saved_modes = (
            {str(k): str(v) for k, v in saved.items()} if isinstance(saved, Mapping) else {}
        )
        return cls(
            holiday_id=hid,
            name=str(raw.get("name") or "Holiday").strip() or "Holiday",
            start=start,
            end=end,
            temperature=temp,
            room_ids=room_ids,
            active=bool(raw.get("active", False)),
            saved_modes=saved_modes,
        )


class RoomStore:
    """In-memory room & holiday map backed by a JSON file. All mutators persist on success.

    Not thread-safe by itself: the coordinator calls it under its lock.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self._path: Path | None = Path(path) if path is not None else None
        self._rooms: dict[str, CoordinatorRoom] = {}
        self._holidays: dict[str, CoordinatorHoliday] = {}

    # --- persistence ---------------------------------------------------------------------------

    def load(self) -> RoomStore:
        """Load from disk if the file exists; an absent or empty file is an empty store."""
        self._rooms = {}
        self._holidays = {}
        if self._path is None or not self._path.exists():
            return self
        raw = self._path.read_text(encoding="utf-8").strip()
        if not raw:
            return self
        data = json.loads(raw)
        rooms = data.get("rooms", []) if isinstance(data, Mapping) else []
        for entry in rooms:
            if isinstance(entry, Mapping):
                room = CoordinatorRoom.from_dict(entry)
                self._rooms[room.room_id] = room
        holidays = data.get("holidays", []) if isinstance(data, Mapping) else []
        for entry in holidays:
            if isinstance(entry, Mapping):
                hol = CoordinatorHoliday.from_dict(entry)
                self._holidays[hol.holiday_id] = hol
        return self

    def save(self) -> None:
        """Atomically write the store to disk (no-op when no path was configured)."""
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "version": STORE_VERSION,
            "rooms": [r.to_dict() for r in self._rooms.values()],
        }
        if self._holidays:
            payload["holidays"] = [h.to_dict() for h in self._holidays.values()]
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)

    # --- reads ---------------------------------------------------------------------------------

    def rooms(self) -> list[CoordinatorRoom]:
        return list(self._rooms.values())

    def get(self, room_id: str) -> CoordinatorRoom:
        try:
            return self._rooms[room_id]
        except KeyError:
            raise RoomStoreError(f"unknown room {room_id!r}") from None

    def __len__(self) -> int:
        return len(self._rooms)

    # --- mutators (persist on success) ---------------------------------------------------------

    def create(
        self,
        name: str,
        prefix: str,
        *,
        members: Iterable[str] = (),
        sensor: str | None = None,
        home_id: str | None = None,
        room_id: str | None = None,
    ) -> CoordinatorRoom:
        name = (name or "").strip()
        prefix = (prefix or "").strip().rstrip("/")
        if not name:
            raise RoomStoreError("a room needs a name")
        if not prefix:
            raise RoomStoreError("a room needs a bridge prefix")
        rid = (room_id or "").strip() or _new_room_id()
        if rid in self._rooms:
            raise RoomStoreError(f"room {rid!r} already exists")
        member_tuple = self._clean_members(members)
        self._check_owner_free(member_tuple, owner=None)
        room = CoordinatorRoom(
            room_id=rid,
            name=name,
            prefix=prefix,
            home_id=(home_id or None),
            members=member_tuple,
            sensor=(sensor or None),
        )
        self._rooms[rid] = room
        self.save()
        return room

    def rename(self, room_id: str, name: str) -> CoordinatorRoom:
        name = (name or "").strip()
        if not name:
            raise RoomStoreError("a room needs a name")
        room = replace(self.get(room_id), name=name)
        self._rooms[room_id] = room
        self.save()
        return room

    def set_members(self, room_id: str, members: Iterable[str]) -> CoordinatorRoom:
        room = self.get(room_id)
        member_tuple = self._clean_members(members)
        self._check_owner_free(member_tuple, owner=room_id)
        room = replace(room, members=member_tuple)
        self._rooms[room_id] = room
        self.save()
        return room

    def set_sensor(self, room_id: str, sensor: str | None) -> CoordinatorRoom:
        room = replace(self.get(room_id), sensor=(sensor or None))
        self._rooms[room_id] = room
        self.save()
        return room

    def update_desired(
        self,
        room_id: str,
        changes: Mapping[str, Any],
        *,
        remove_keys: Iterable[str] = (),
    ) -> CoordinatorRoom:
        """Merge ``changes`` (and drop ``remove_keys``) on ``room.desired`` and persist (§5).

        The desired state is what a write stage *asked* for; the ``drift`` view compares it to what
        the heads **report**. Seeding it does not touch a head — it is a definition update only.
        """
        room = self.get(room_id)
        desired = dict(room.desired)
        for key in remove_keys:
            desired.pop(key, None)
        desired.update(changes)
        room = replace(room, desired=desired)
        self._rooms[room_id] = room
        self.save()
        return room

    def set_schedule(self, room_id: str, schedule: Mapping[str, Any] | None) -> CoordinatorRoom:
        """Persist a room's weekly schedule definition and return the updated room."""
        room = replace(self.get(room_id), schedule=dict(schedule) if schedule is not None else None)
        self._rooms[room_id] = room
        self.save()
        return room

    def delete(self, room_id: str) -> None:
        self.get(room_id)  # raise if unknown
        del self._rooms[room_id]
        self.save()

    # --- holidays (Stage 7) --------------------------------------------------------------------

    def holidays(self) -> list[CoordinatorHoliday]:
        return list(self._holidays.values())

    def get_holiday(self, holiday_id: str) -> CoordinatorHoliday:
        try:
            return self._holidays[holiday_id]
        except KeyError:
            raise RoomStoreError(f"unknown holiday {holiday_id!r}") from None

    def upsert_holiday(
        self,
        *,
        start: str,
        end: str,
        temperature: float,
        room_ids: Iterable[str],
        name: str = "Holiday",
        holiday_id: str | None = None,
        active: bool | None = None,
        saved_modes: Mapping[str, str] | None = None,
    ) -> CoordinatorHoliday:
        hid = (holiday_id or "").strip() or _new_holiday_id()
        existing = self._holidays.get(hid)
        rooms = tuple(str(r).strip() for r in room_ids if str(r).strip())
        if not rooms:
            raise RoomStoreError("a holiday requires at least one room_id")
        for rid in rooms:
            self.get(rid)  # verify every room exists
        hol = CoordinatorHoliday(
            holiday_id=hid,
            name=(name or "Holiday").strip() or "Holiday",
            start=start,
            end=end,
            temperature=float(temperature),
            room_ids=rooms,
            active=existing.active if active is None and existing is not None else bool(active),
            saved_modes=(
                dict(existing.saved_modes)
                if saved_modes is None and existing is not None
                else dict(saved_modes or {})
            ),
        )
        self._holidays[hid] = hol
        self.save()
        return hol

    def update_holiday_state(
        self,
        holiday_id: str,
        *,
        active: bool,
        saved_modes: Mapping[str, str] | None = None,
    ) -> CoordinatorHoliday:
        existing = self.get_holiday(holiday_id)
        hol = replace(
            existing,
            active=active,
            saved_modes=(
                dict(saved_modes) if saved_modes is not None else dict(existing.saved_modes)
            ),
        )
        self._holidays[holiday_id] = hol
        self.save()
        return hol

    def delete_holiday(self, holiday_id: str) -> None:
        self.get_holiday(holiday_id)  # raise if unknown
        del self._holidays[holiday_id]
        self.save()

    # --- helpers -------------------------------------------------------------------------------

    @staticmethod
    def _clean_members(members: Iterable[str]) -> tuple[str, ...]:
        seen: dict[str, str] = {}
        for raw in members:
            addr = str(raw).strip()
            if not addr:
                continue
            seen.setdefault(normalize_address(addr), addr)
        return tuple(seen.values())

    def _check_owner_free(self, members: tuple[str, ...], *, owner: str | None) -> None:
        """Enforce §5.1: no member may already belong to a different room."""
        wanted = {normalize_address(m) for m in members}
        for room in self._rooms.values():
            if room.room_id == owner:
                continue
            clash = wanted & {normalize_address(m) for m in room.members}
            if clash:
                raise RoomStoreError(
                    f"head(s) {sorted(clash)} already belong to room {room.room_id!r}"
                )
