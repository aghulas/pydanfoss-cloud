"""Room of a cloud room sensor (``014G2480``), read and written by the bridge (``danfoss2mqtt``).

In the Danfoss Ally app a room sensor belongs to one Tuya *room* (type 4) or to none, and a sensor
in a room is meant to become the reference measurement of that room's heads. The app's gestures
were captured on 2026-11-28 (Android emulator, request hook):

- put a sensor in a room: ``thing.m.custom.wkf.device.relation.save`` v1.0
  ``{bizId: <sensor devId>, bizType: 6, parentId: <room id>, parentType: 4}``;
- take it out of its room: ``thing.m.custom.wkf.device.relation.delete`` v1.0, same keys.

The SDK signs ``thing.m.*`` routes as ``smartlife.m.*`` (as for the other room writes), so the
client sends ``smartlife.m.custom.wkf.device.relation.save`` / ``.delete``. After each gesture the
app re-reads ``m.life.my.group.device.sort.list`` v2.1, the device -> room map the bridge already
reads (``CloudClient.async_device_rooms``): that read is the bridge's read-back.

Putting a sensor in a room changes what the room's heads regulate on, so the bridge's write is
**closed by default** (``SensorRoomsGate``) and opens per the supervised first-write procedure: one
designated sensor first (``trial_sensor``), then every sensor once that trial passed.

``/set`` contract (``<base>/<sensor>/set``)::

    {"sensor_room": {"room_id": "<Tuya room id from bridge/sensor_rooms>"}}  # put in that room
    {"sensor_room": {"room_id": null}}  # take it out of its room

Moving a sensor from one room to another takes two messages (out, then in), as in the app; a
room that already holds another sensor is refused.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .client import CloudClient, CloudError, CloudWriteError
from .decode import DanfossDevice

_LOGGER = logging.getLogger(__name__)

SENSOR_ROOM_KEY = "sensor_room"

# A home's room list is account configuration (rooms are created by hand in the app, rarely): one
# ``room.map.get`` request per home, at most this often.
SENSOR_ROOMS_TTL_SECONDS = 3600.0

# Read-back of a ``sensor_room`` write: the home's device -> room map is re-read up to this many
# times, this far apart, until the sensor shows the asked room.
SENSOR_CONFIRM_ATTEMPTS = 3
SENSOR_CONFIRM_DELAY_S = 5.0

ACTION_SAVE = "save"
ACTION_DELETE = "delete"


@dataclass(frozen=True)
class SensorRoomsGate:
    """Whether the bridge writes a sensor's room, and for which sensors.

    ``enabled`` False (the default): nothing is written, ``sensor_room`` stays refused.
    ``trial_sensor`` set: only that sensor (by device id) can be written — the supervised first
    write.
    """

    enabled: bool = False
    trial_sensor: str | None = None

    def open_for(self, dev_id: str) -> bool:
        return self.enabled and (not self.trial_sensor or self.trial_sensor == dev_id)


@dataclass(frozen=True)
class SensorRoomRequest:
    """A validated ``sensor_room`` request: ``room_id`` to put the sensor in, None: take it out."""

    room_id: str | None


def parse_sensor_room(raw: Any) -> SensorRoomRequest:
    """Validate the ``sensor_room`` value of a ``/set`` payload.

    Raises ``CloudWriteError`` unless it is ``{"room_id": <non-empty str or int>}`` or
    ``{"room_id": null}`` (a missing key, a boolean or an empty id is refused: never guessed).
    """
    if not isinstance(raw, Mapping):
        raise CloudWriteError(f"sensor_room expects an object, got {type(raw).__name__}")
    if "room_id" not in raw:
        raise CloudWriteError("sensor_room.room_id is required (null takes the sensor out)")
    room_id = raw["room_id"]
    if room_id is None:
        return SensorRoomRequest(room_id=None)
    if isinstance(room_id, bool) or not isinstance(room_id, str | int):
        raise CloudWriteError("sensor_room.room_id must be a room id or null")
    text = str(room_id).strip()
    if not text or text == "-1":
        raise CloudWriteError("sensor_room.room_id must be a room id or null")
    return SensorRoomRequest(room_id=text)


def check_sensor_room(
    request: SensorRoomRequest,
    sensor_id: str,
    device_rooms: Mapping[str, str],
    home_rooms: Iterable[Mapping[str, Any]],
    sensor_ids: Iterable[str],
) -> tuple[str, str] | None:
    """Check ``request`` against FRESH reads of the sensor's home.

    ``device_rooms`` is the home's {devId: room_id} map (``async_device_rooms``, devices in no room
    left out), ``home_rooms`` its rooms (``async_list_rooms``), ``sensor_ids`` every room sensor of
    the fleet. Returns ``(action, room_id)`` — ``("save", <asked room>)`` or ``("delete",
    <current room>)`` — or ``None`` when the sensor is already as asked (nothing to send). Raises
    ``CloudWriteError`` when the room is not one of the home's rooms, when the sensor already sits
    in another room (take it out first, as in the app), or when the room already holds another
    sensor (the heads would have two references).
    """
    current = device_rooms.get(sensor_id)
    if request.room_id is None:
        return None if current is None else (ACTION_DELETE, current)
    if not any(str(r.get("room_id")) == request.room_id for r in home_rooms):
        raise CloudWriteError("room is not one of the sensor's home rooms")
    if current == request.room_id:
        return None
    if current is not None:
        raise CloudWriteError("the sensor is in another room: take it out of that room first")
    others = {s for s in sensor_ids if s != sensor_id}
    if any(device_rooms.get(s) == request.room_id for s in others):
        raise CloudWriteError("that room already holds another room sensor")
    return (ACTION_SAVE, request.room_id)


def sensor_room_confirmed(
    request: SensorRoomRequest, sensor_id: str, device_rooms: Mapping[str, str]
) -> bool:
    """True when a read of the home's device -> room map shows the sensor as asked."""
    return device_rooms.get(sensor_id) == request.room_id


def sensor_room_entry(
    home_id: str, room_id: str | None, rooms: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """One sensor's value on ``bridge/sensor_rooms``."""
    return {
        "home_id": str(home_id),
        "room_id": room_id,
        "rooms": [{"room_id": str(r.get("room_id")), "name": r.get("name")} for r in rooms],
    }


class SensorRooms:
    """Per home, the Tuya rooms a sensor can be put in (``async_list_rooms``), read sparingly.

    One request per home that holds a room sensor, at most every ``SENSOR_ROOMS_TTL_SECONDS`` (or
    at once when such a home appears). A home whose read fails keeps its previous rooms and is
    retried on the next cycle; a home with no sensor any more is dropped. ``refresh`` re-reads one
    home at once (around a ``sensor_room`` write), outside the TTL.
    """

    def __init__(self, ttl: float = SENSOR_ROOMS_TTL_SECONDS) -> None:
        self._ttl = ttl
        self._rooms: dict[str, list[dict[str, Any]]] = {}
        self._read_at: dict[str, float] = {}

    async def refresh(self, client: CloudClient, home_id: str) -> list[dict[str, Any]]:
        """Read one home's rooms now and keep them. Raises ``CloudError`` on a failed read."""
        rooms = await client.async_list_rooms(home_id)
        clean = [
            {"room_id": str(r.get("room_id")), "name": r.get("name")}
            for r in rooms
            if isinstance(r, Mapping) and r.get("room_id") is not None
        ]
        self._rooms[home_id] = clean
        self._read_at[home_id] = time.monotonic()
        return list(clean)

    def rooms_of(self, home_id: str) -> list[dict[str, Any]] | None:
        rooms = self._rooms.get(home_id)
        return None if rooms is None else list(rooms)

    async def read(
        self,
        client: CloudClient,
        devices: list[DanfossDevice],
        device_rooms: Mapping[str, str],
    ) -> dict[str, dict[str, Any]] | None:
        """``bridge/sensor_rooms`` payload, or ``None`` when nothing is known yet.

        Keyed by sensor id: ``{"home_id", "room_id" (None = in no room), "rooms"}``. A sensor whose
        home is unknown, or whose home's rooms were never read, is left out. ``None`` leaves the
        topic untouched; a fleet without any room sensor yields ``{}``.
        """
        raw_home = getattr(client, "device_home", None)
        device_home = raw_home if isinstance(raw_home, Mapping) else {}
        sensors = [d for d in devices if d.is_sensor]
        homes = {str(device_home[s.id]) for s in sensors if s.id in device_home}
        for gone in set(self._rooms) - homes:
            self._rooms.pop(gone, None)
            self._read_at.pop(gone, None)
        now = time.monotonic()
        for home_id in sorted(homes):
            last = self._read_at.get(home_id)
            if last is not None and now - last < self._ttl:
                continue
            try:
                await self.refresh(client, home_id)
            except CloudError:
                _LOGGER.warning("danfoss2mqtt: could not read a home's rooms this cycle")
        out: dict[str, dict[str, Any]] = {}
        for sensor in sensors:
            sensor_home = device_home.get(sensor.id)
            rooms = self._rooms.get(str(sensor_home)) if sensor_home is not None else None
            if rooms is None:
                continue
            out[sensor.id] = sensor_room_entry(str(sensor_home), device_rooms.get(sensor.id), rooms)
        if sensors and not out:
            return None
        return out


async def write_sensor_room(
    client: CloudClient,
    gate: SensorRoomsGate,
    cache: SensorRooms,
    dev: DanfossDevice,
    raw: Any,
    sensor_ids: Iterable[str],
) -> tuple[str, dict[str, str]] | None:
    """Put room sensor ``dev`` in a room, or take it out (``sensor_room`` on its ``/set``).

    Validated against FRESH reads of the sensor's home (its rooms and its device -> room map),
    written with one ``relation.save`` / ``relation.delete``, then confirmed by re-reading the
    device -> room map. Never optimistic. Returns ``(home_id, device_rooms)`` — the last map read,
    for the caller to republish at once — or ``None`` when refused before any read.
    """
    if not dev.is_sensor:
        _LOGGER.warning("danfoss2mqtt: sensor_room refused for %s: not a room sensor", dev.id)
        return None
    if not gate.open_for(dev.id):
        _LOGGER.warning(
            "danfoss2mqtt: sensor_room refused for sensor %s: sensor room writes are closed "
            "(add-on option sensor_rooms_writes / sensor_rooms_trial_sensor)",
            dev.id,
        )
        return None
    try:
        request = parse_sensor_room(raw)
    except CloudWriteError as err:
        _LOGGER.warning("danfoss2mqtt: sensor_room refused for sensor %s: %s", dev.id, err)
        return None
    raw_home = getattr(client, "device_home", None)
    home_id = raw_home.get(dev.id) if isinstance(raw_home, Mapping) else None
    if home_id is None:
        _LOGGER.warning(
            "danfoss2mqtt: sensor_room refused for sensor %s: its home is not known", dev.id
        )
        return None
    home_id = str(home_id)
    try:
        rooms = await cache.refresh(client, home_id)
        device_rooms = await client.async_device_rooms(home_id)
    except CloudError as err:
        _LOGGER.warning(
            "danfoss2mqtt: sensor_room not sent for sensor %s: its home could not be read: %s",
            dev.id,
            err,
        )
        return None
    try:
        decision = check_sensor_room(request, dev.id, device_rooms, rooms, sensor_ids)
    except CloudWriteError as err:
        _LOGGER.warning("danfoss2mqtt: sensor_room refused for sensor %s: %s", dev.id, err)
        return home_id, device_rooms
    if decision is None:
        _LOGGER.info("danfoss2mqtt: sensor %s already as asked; nothing sent", dev.id)
        return home_id, device_rooms
    action, room_id = decision
    try:
        if action == ACTION_SAVE:
            await client.async_sensor_room_save(dev.id, room_id)
        else:
            await client.async_sensor_room_delete(dev.id, room_id)
    except CloudError as err:
        _LOGGER.warning(
            "danfoss2mqtt: sensor_room write failed for sensor %s, room %s: %s",
            dev.id,
            room_id,
            err,
        )
    for attempt in range(SENSOR_CONFIRM_ATTEMPTS):
        if attempt:
            await asyncio.sleep(SENSOR_CONFIRM_DELAY_S)
        try:
            device_rooms = await client.async_device_rooms(home_id)
        except CloudError:
            continue
        if sensor_room_confirmed(request, dev.id, device_rooms):
            _LOGGER.info(
                "danfoss2mqtt: sensor %s %s room %s (confirmed by a read of the device rooms)",
                dev.id,
                "put in" if action == ACTION_SAVE else "taken out of",
                room_id,
            )
            return home_id, device_rooms
    _LOGGER.warning(
        "danfoss2mqtt: sensor_room on sensor %s, room %s NOT confirmed: the device rooms still "
        "show the previous state (published as read)",
        dev.id,
        room_id,
    )
    return home_id, device_rooms
