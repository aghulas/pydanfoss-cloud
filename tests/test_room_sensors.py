"""Room sensors on ``bridge/rooms``: the device -> Tuya room read, its cache and the attachment.

Synthetic data only (no real device, room or home id).
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydanfoss_cloud import bridge_daemon
from pydanfoss_cloud.bridge_daemon import DeviceRooms, StandaloneDanfoss2MqttDaemon
from pydanfoss_cloud.client import CloudClient, CloudError
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.rooms import RoomState, attach_room_sensors, isolated_head_sensors


def _head(dev_id: str) -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=dev_id, category="head", online=True)


def _sensor(dev_id: str) -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=dev_id, category="sensor", online=True, temperature=19.5)


def _group(group_id: str, *members: str) -> RoomState:
    return RoomState(
        group_id=group_id,
        name=f"Room {group_id}",
        pid=None,
        head_count=len(members),
        home_gid="home1",
        members=members,
    )


def _client() -> CloudClient:
    return CloudClient(
        username="u",
        password="p",
        country_code="33",
        sign_key="sk",
        device_id="d" * 44,
        ch_key="ck",
    )


# --- client read --------------------------------------------------------------------------------


async def test_async_device_rooms_keeps_devices_in_a_room() -> None:
    client = _client()
    client._request = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"bizId": "head_a", "bizType": 6, "roomId": 401, "displayOrder": 1},
            {"bizId": "sens_a", "bizType": 6, "roomId": "401", "displayOrder": 2},
            {"bizId": "sens_b", "bizType": 6, "roomId": -1, "displayOrder": -1},  # in no room
            {"bizId": "head_c", "bizType": 6},  # no roomId
            {"bizId": "group_x", "bizType": 5, "roomId": 401},  # not a device
            "junk",
        ]
    )
    assert await client.async_device_rooms("home1") == {"head_a": "401", "sens_a": "401"}
    client._request.assert_awaited_once_with(
        "m.life.my.group.device.sort.list", "2.1", {"gid": "home1"}
    )


async def test_async_device_rooms_tolerates_a_non_list_answer() -> None:
    client = _client()
    client._request = AsyncMock(return_value={"unexpected": True})  # type: ignore[method-assign]
    assert await client.async_device_rooms("home1") == {}


# --- attachment rules ---------------------------------------------------------------------------


def test_attach_sensor_from_the_heads_room() -> None:
    devices = [_head("h1"), _head("h2"), _sensor("s1"), _head("h3"), _sensor("s9")]
    device_rooms = {"h1": "401", "h2": "401", "s1": "401", "h3": "402", "s9": "499"}
    rooms, conflicts = attach_room_sensors(
        [_group("g1", "h1", "h2"), _group("g2", "h3")], devices, device_rooms
    )
    assert [r.sensor for r in rooms] == ["s1", None]  # room 402 has no sensor
    assert conflicts == 0


def test_heads_in_different_rooms_give_no_sensor() -> None:
    devices = [_head("h1"), _head("h2"), _sensor("s1")]
    rooms, conflicts = attach_room_sensors(
        [_group("g1", "h1", "h2")], devices, {"h1": "401", "h2": "402", "s1": "401"}
    )
    assert rooms[0].sensor is None
    assert conflicts == 1


def test_heads_without_room_are_ignored_but_one_is_needed() -> None:
    devices = [_head("h1"), _head("h2"), _sensor("s1")]
    # h2 carries no room: the group's room is h1's.
    rooms, _ = attach_room_sensors([_group("g1", "h1", "h2")], devices, {"h1": "401", "s1": "401"})
    assert rooms[0].sensor == "s1"
    # No head with a room: no room, no sensor.
    rooms, conflicts = attach_room_sensors([_group("g1", "h1", "h2")], devices, {"s1": "401"})
    assert rooms[0].sensor is None
    assert conflicts == 0


def test_two_sensors_in_the_room_give_no_sensor() -> None:
    devices = [_head("h1"), _sensor("s1"), _sensor("s2")]
    rooms, _ = attach_room_sensors(
        [_group("g1", "h1")], devices, {"h1": "401", "s1": "401", "s2": "401"}
    )
    assert rooms[0].sensor is None


def test_no_name_fallback() -> None:
    # Same name as the group, but no room link: never attached.
    devices = [_head("h1"), DanfossDevice(id="s1", name="Room g1", category="sensor")]
    rooms, _ = attach_room_sensors([_group("g1", "h1")], devices, {"h1": "401"})
    assert rooms[0].sensor is None


def test_attach_does_not_change_its_input() -> None:
    group = _group("g1", "h1")
    attach_room_sensors([group], [_head("h1"), _sensor("s1")], {"h1": "401", "s1": "401"})
    assert group.sensor is None


# --- cache --------------------------------------------------------------------------------------


async def test_device_rooms_read_sparingly_and_keep_last_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [1000.0]
    monkeypatch.setattr(bridge_daemon.time, "monotonic", lambda: now[0])
    maps: dict[str, dict[str, str]] = {"home1": {"h1": "401"}, "home2": {"h9": "901"}}
    client = SimpleNamespace(
        device_list_homes={"home1": 3, "home2": 1},
        async_device_rooms=AsyncMock(side_effect=lambda gid: dict(maps[gid])),
    )
    cache = DeviceRooms(ttl=3600.0)
    expected = {"h1": "401", "h9": "901"}
    assert await cache.read(client) == expected  # type: ignore[arg-type]
    assert client.async_device_rooms.await_count == 2
    # Within the TTL: no new request.
    now[0] += 600
    assert await cache.read(client) == expected  # type: ignore[arg-type]
    assert client.async_device_rooms.await_count == 2
    # After the TTL a failed read keeps the last known map and is retried next cycle.
    now[0] += 3600
    client.async_device_rooms.side_effect = CloudError("boom")
    assert await cache.read(client) == expected  # type: ignore[arg-type]
    assert await cache.read(client) == expected  # type: ignore[arg-type]
    assert client.async_device_rooms.await_count == 6
    # A home gone from the account is dropped.
    client.device_list_homes = {"home1": 3}
    assert await cache.read(client) == {"h1": "401"}  # type: ignore[arg-type]


async def test_device_rooms_empty_without_homes() -> None:
    client = SimpleNamespace(device_list_homes={}, async_device_rooms=AsyncMock())
    assert await DeviceRooms().read(client) == {}  # type: ignore[arg-type]
    client.async_device_rooms.assert_not_awaited()


def test_conflicts_logged_once_without_ids(caplog: pytest.LogCaptureFixture) -> None:
    cache = DeviceRooms()
    devices = [_head("h1"), _head("h2")]
    groups = [_group("g1", "h1", "h2")]
    split = {"h1": "401", "h2": "402"}
    with caplog.at_level(logging.WARNING, logger=bridge_daemon._LOGGER.name):
        cache.attach(groups, devices, split)
        cache.attach(groups, devices, split)
    msgs = [r.getMessage() for r in caplog.records if "different Tuya rooms" in r.getMessage()]
    assert len(msgs) == 1
    assert "h1" not in msgs[0] and "401" not in msgs[0] and "g1" not in msgs[0]


# --- bridge/rooms payload -----------------------------------------------------------------------


class _MemTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: Any) -> None:
        pass


def test_bridge_rooms_carries_sensor() -> None:
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport, base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    rooms, _ = attach_room_sensors(
        [_group("g1", "h1"), _group("g2", "h2")],
        [_head("h1"), _head("h2"), _sensor("s1")],
        {"h1": "401", "h2": "402", "s1": "401"},
    )
    daemon.publish_fleet([_head("h1"), _head("h2"), _sensor("s1")], rooms=rooms)
    pub = [p for t, p, _r in transport.published if t == "danfoss2mqtt/bridge/rooms"]
    payload = json.loads(pub[-1])
    assert [r["sensor"] for r in payload] == ["s1", None]


# --- isolated heads (no schedule group) -----------------------------------------------------------


def test_isolated_head_gets_the_sensor_of_its_room() -> None:
    devices = [_head("h1"), _head("h2"), _sensor("s1"), _head("h9"), _sensor("s9")]
    device_rooms = {"h1": "401", "h2": "401", "s1": "401", "h9": "409", "s9": "409"}
    groups = [_group("g1", "h1", "h2")]
    assert isolated_head_sensors(groups, devices, device_rooms) == {"h9": "s9"}


def test_isolated_head_ambiguous_cases_give_no_link() -> None:
    # Two isolated heads in one room.
    devices = [_head("h8"), _head("h9"), _sensor("s9")]
    assert isolated_head_sensors([], devices, {"h8": "409", "h9": "409", "s9": "409"}) == {}
    # Two sensors in the room.
    devices = [_head("h9"), _sensor("s8"), _sensor("s9")]
    assert isolated_head_sensors([], devices, {"h9": "409", "s8": "409", "s9": "409"}) == {}
    # A grouped head shares the room: the room is the group's, not the isolated head's.
    devices = [_head("h1"), _head("h9"), _sensor("s9")]
    groups = [_group("g1", "h1")]
    assert isolated_head_sensors(groups, devices, {"h1": "409", "h9": "409", "s9": "409"}) == {}
    # No room for the head, or no sensor in its room.
    devices = [_head("h9"), _sensor("s9")]
    assert isolated_head_sensors([], devices, {"s9": "409"}) == {}
    assert isolated_head_sensors([], devices, {"h9": "409", "s9": "410"}) == {}


def test_isolated_head_no_name_fallback() -> None:
    devices = [
        DanfossDevice(id="h9", name="Office", category="head"),
        DanfossDevice(id="s9", name="Office", category="sensor"),
    ]
    assert isolated_head_sensors([], devices, {"h9": "409"}) == {}


def _descriptors(transport: _MemTransport) -> dict[str, dict[str, Any]]:
    pub = [p for t, p, _r in transport.published if t == "danfoss2mqtt/bridge/devices"]
    return {d["danfoss"]["dev_id"]: d for d in json.loads(pub[-1])}


def test_bridge_devices_carries_room_sensor_on_isolated_head() -> None:
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport, base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    devices = [_head("h1"), _head("h9"), _sensor("s9")]
    groups = [_group("g1", "h1")]
    daemon.publish_fleet(devices, rooms=groups, head_sensors={"h9": "s9"})
    descs = _descriptors(transport)
    assert descs["h9"]["danfoss"]["room_sensor"] == "s9"
    assert "room_sensor" not in descs["h1"]["danfoss"]
    assert "room_sensor" not in descs["s9"]["danfoss"]
    # ``None`` keeps the last known map; a head that joined a group loses the link.
    daemon.publish_fleet(devices, rooms=[_group("g1", "h1", "h9")])
    assert "room_sensor" not in _descriptors(transport)["h9"]["danfoss"]
    daemon.publish_fleet(devices, rooms=groups)
    assert _descriptors(transport)["h9"]["danfoss"]["room_sensor"] == "s9"
    # An empty map clears it.
    daemon.publish_fleet(devices, rooms=groups, head_sensors={})
    assert "room_sensor" not in _descriptors(transport)["h9"]["danfoss"]
