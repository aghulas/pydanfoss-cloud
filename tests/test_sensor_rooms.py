"""Room of a cloud room sensor written by the bridge (``sensor_room``). Synthetic data only."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydanfoss_cloud import bridge_daemon, sensor_rooms
from pydanfoss_cloud.bridge_daemon import (
    BridgeConfig,
    DeviceRooms,
    StandaloneDanfoss2MqttDaemon,
    async_run_bridge,
)
from pydanfoss_cloud.client import CloudClient, CloudError, CloudWriteError
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.sensor_rooms import (
    SensorRoomRequest,
    SensorRooms,
    SensorRoomsGate,
    check_sensor_room,
    parse_sensor_room,
    sensor_room_confirmed,
    write_sensor_room,
)

OPEN = SensorRoomsGate(enabled=True)
HOME = "900"
ROOMS = [{"room_id": "71", "name": "Salle A"}, {"room_id": "72", "name": "Salle B"}]


def _sensor(dev_id: str = "sensor_1", name: str = "Sonde") -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=name, category="sensor", online=True)


def _head(dev_id: str = "dev_1") -> DanfossDevice:
    return DanfossDevice(id=dev_id, name="Salle 101", category="head", product_id="03wvrwpv")


class _MemTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self.subs: dict[str, Any] = {}

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: Any) -> None:
        self.subs[topic] = callback


def _descriptor(transport: _MemTransport, dev_id: str) -> dict[str, Any]:
    raw = [p for t, p, _ in transport.published if t == "danfoss2mqtt/bridge/devices"][-1]
    return next(d for d in json.loads(raw) if d["danfoss"]["dev_id"] == dev_id)


def _topic(transport: _MemTransport, name: str) -> list[Any]:
    return [json.loads(p) for t, p, _ in transport.published if t == f"danfoss2mqtt/bridge/{name}"]


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sensor_rooms, "SENSOR_CONFIRM_DELAY_S", 0.0)


# --- pure logic --------------------------------------------------------------------------------


def test_gate_closed_by_default_and_trial_confines_to_one_sensor() -> None:
    assert SensorRoomsGate().open_for("sensor_1") is False
    trial = SensorRoomsGate(enabled=True, trial_sensor="sensor_1")
    assert trial.open_for("sensor_1") is True
    assert trial.open_for("sensor_2") is False
    assert OPEN.open_for("sensor_2") is True


def test_parse_sensor_room_accepts_a_room_id_or_null_only() -> None:
    assert parse_sensor_room({"room_id": "71"}) == SensorRoomRequest("71")
    assert parse_sensor_room({"room_id": 71}) == SensorRoomRequest("71")
    assert parse_sensor_room({"room_id": " 71 "}) == SensorRoomRequest("71")
    assert parse_sensor_room({"room_id": None}) == SensorRoomRequest(None)
    for bad in ({}, {"room_id": ""}, {"room_id": True}, {"room_id": -1}, {"room_id": [71]}, "71"):
        with pytest.raises(CloudWriteError):
            parse_sensor_room(bad)


def test_check_sensor_room_against_the_fresh_reads() -> None:
    sensors = ["sensor_1", "sensor_2"]
    put = SensorRoomRequest("71")
    out = SensorRoomRequest(None)
    # Roomless sensor -> save into a listed room.
    assert check_sensor_room(put, "sensor_1", {}, ROOMS, sensors) == ("save", "71")
    # Already there, or already out: nothing to send.
    assert check_sensor_room(put, "sensor_1", {"sensor_1": "71"}, ROOMS, sensors) is None
    assert check_sensor_room(out, "sensor_1", {}, ROOMS, sensors) is None
    # Take out of the room it is in now.
    assert check_sensor_room(out, "sensor_1", {"sensor_1": "72"}, ROOMS, sensors) == (
        "delete",
        "72",
    )
    # A head in the room is fine; another sensor is not.
    assert check_sensor_room(put, "sensor_1", {"dev_1": "71"}, ROOMS, sensors) == ("save", "71")
    with pytest.raises(CloudWriteError, match="another room sensor"):
        check_sensor_room(put, "sensor_1", {"sensor_2": "71"}, ROOMS, sensors)
    with pytest.raises(CloudWriteError, match="not one of"):
        check_sensor_room(SensorRoomRequest("99"), "sensor_1", {}, ROOMS, sensors)
    with pytest.raises(CloudWriteError, match="take it out"):
        check_sensor_room(put, "sensor_1", {"sensor_1": "72"}, ROOMS, sensors)


def test_sensor_room_confirmed() -> None:
    assert sensor_room_confirmed(SensorRoomRequest("71"), "s", {"s": "71"})
    assert not sensor_room_confirmed(SensorRoomRequest("71"), "s", {})
    assert sensor_room_confirmed(SensorRoomRequest(None), "s", {"t": "71"})
    assert not sensor_room_confirmed(SensorRoomRequest(None), "s", {"s": "71"})


async def test_client_sends_the_captured_routes() -> None:
    client = CloudClient.__new__(CloudClient)
    client._request = AsyncMock(return_value=True)  # type: ignore[method-assign]
    await client.async_sensor_room_save("sensor_1", "71")
    await client.async_sensor_room_delete("sensor_1", "71")
    body = {"bizId": "sensor_1", "bizType": 6, "parentId": "71", "parentType": 4}
    assert client._request.await_args_list[0].args == (
        "smartlife.m.custom.wkf.device.relation.save",
        "1.0",
        body,
    )
    assert client._request.await_args_list[1].args == (
        "smartlife.m.custom.wkf.device.relation.delete",
        "1.0",
        body,
    )


# --- SensorRooms (bridge/sensor_rooms) ---------------------------------------------------------


def _client(
    device_reads: list[Any] | None = None,
    room_reads: list[Any] | None = None,
    home: dict[str, str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        device_home={"sensor_1": HOME, "sensor_2": HOME} if home is None else home,
        async_list_rooms=AsyncMock(side_effect=room_reads or [ROOMS] * 5),
        async_device_rooms=AsyncMock(side_effect=device_reads or []),
        async_sensor_room_save=AsyncMock(return_value=True),
        async_sensor_room_delete=AsyncMock(return_value=True),
    )


async def test_sensor_rooms_payload_and_ttl() -> None:
    client = _client(room_reads=[ROOMS])
    cache = SensorRooms()
    devices = [_sensor(), _head()]
    payload = await cache.read(client, devices, {"sensor_1": "72"})  # type: ignore[arg-type]
    assert payload == {"sensor_1": {"home_id": HOME, "room_id": "72", "rooms": ROOMS}}
    # Within the TTL: no new room read; the sensor's room follows the map passed in.
    payload = await cache.read(client, devices, {})  # type: ignore[arg-type]
    assert payload == {"sensor_1": {"home_id": HOME, "room_id": None, "rooms": ROOMS}}
    assert client.async_list_rooms.await_count == 1


async def test_sensor_rooms_unknown_until_read_and_empty_without_sensors() -> None:
    client = _client(room_reads=[CloudError("boom")])
    cache = SensorRooms()
    assert await cache.read(client, [_sensor()], {}) is None  # type: ignore[arg-type]
    assert await cache.read(client, [_head()], {}) == {}  # type: ignore[arg-type]
    # A sensor whose home is unknown is left out.
    client = _client(home={})
    assert await cache.read(client, [_sensor()], {}) is None  # type: ignore[arg-type]


# --- write_sensor_room -------------------------------------------------------------------------


async def _write(
    client: SimpleNamespace,
    raw: Any,
    gate: SensorRoomsGate = OPEN,
    dev: DanfossDevice | None = None,
) -> Any:
    return await write_sensor_room(
        client,  # type: ignore[arg-type]
        gate,
        SensorRooms(),
        dev or _sensor(),
        raw,
        ["sensor_1", "sensor_2"],
    )


async def test_put_in_room_is_validated_written_and_read_back() -> None:
    client = _client(device_reads=[{}, {"sensor_1": "71"}])
    read = await _write(client, {"room_id": "71"})
    assert read == (HOME, {"sensor_1": "71"})
    client.async_sensor_room_save.assert_awaited_once_with("sensor_1", "71")
    client.async_sensor_room_delete.assert_not_awaited()
    assert client.async_device_rooms.await_count == 2  # fresh read, then the read-back
    client.async_list_rooms.assert_awaited_once_with(HOME)


async def test_take_out_uses_the_current_room() -> None:
    client = _client(device_reads=[{"sensor_1": "72"}, {}])
    assert await _write(client, {"room_id": None}) == (HOME, {})
    client.async_sensor_room_delete.assert_awaited_once_with("sensor_1", "72")
    client.async_sensor_room_save.assert_not_awaited()


async def test_unconfirmed_write_returns_what_was_read(caplog: pytest.LogCaptureFixture) -> None:
    client = _client(device_reads=[{}, CloudError("boom"), {}, {}])
    assert await _write(client, {"room_id": "71"}) == (HOME, {})  # never the asked state
    assert client.async_device_rooms.await_count == 1 + sensor_rooms.SENSOR_CONFIRM_ATTEMPTS
    assert "NOT confirmed" in caplog.text


async def test_failed_write_is_still_read_back() -> None:
    client = _client(device_reads=[{}, {}, {}, {}])
    client.async_sensor_room_save.side_effect = CloudError("refused")
    assert await _write(client, {"room_id": "71"}) == (HOME, {})
    assert client.async_device_rooms.await_count == 1 + sensor_rooms.SENSOR_CONFIRM_ATTEMPTS


@pytest.mark.parametrize(
    ("gate", "dev", "raw", "home"),
    [
        (SensorRoomsGate(), _sensor(), {"room_id": "71"}, None),  # gate closed
        (SensorRoomsGate(enabled=True, trial_sensor="other"), _sensor(), {"room_id": "71"}, None),
        (OPEN, _head(), {"room_id": "71"}, None),  # not a sensor
        (OPEN, _sensor(), {"room": "71"}, None),  # invalid value
        (OPEN, _sensor(), {"room_id": "71"}, {}),  # home unknown
    ],
)
async def test_refused_before_any_request(
    gate: SensorRoomsGate, dev: DanfossDevice, raw: Any, home: dict[str, str] | None
) -> None:
    client = _client(home=home)
    assert await _write(client, raw, gate, dev) is None
    client.async_list_rooms.assert_not_awaited()
    client.async_device_rooms.assert_not_awaited()
    client.async_sensor_room_save.assert_not_awaited()


@pytest.mark.parametrize(
    ("current", "raw"),
    [
        ({}, {"room_id": "99"}),  # not a room of the home
        ({"sensor_1": "72"}, {"room_id": "71"}),  # in another room
        ({"sensor_2": "71"}, {"room_id": "71"}),  # room holds another sensor
        ({"sensor_1": "71"}, {"room_id": "71"}),  # already there
        ({}, {"room_id": None}),  # already out
    ],
)
async def test_nothing_sent_after_the_fresh_read(current: dict[str, str], raw: Any) -> None:
    client = _client(device_reads=[current])
    assert await _write(client, raw) == (HOME, current)  # read: republished with the truth
    client.async_sensor_room_save.assert_not_awaited()
    client.async_sensor_room_delete.assert_not_awaited()


async def test_unreadable_home_sends_nothing() -> None:
    client = _client(device_reads=[CloudError("boom")])
    assert await _write(client, {"room_id": "71"}) is None
    client.async_sensor_room_save.assert_not_awaited()


def test_device_rooms_store_keeps_a_fresh_map() -> None:
    rooms = DeviceRooms()
    rooms.store(HOME, {"sensor_1": "71"})
    assert rooms.merged() == {"sensor_1": "71"}


# --- end to end through the bridge -------------------------------------------------------------


async def _run(
    config: BridgeConfig, payload: dict[str, Any], device_reads: list[Any]
) -> tuple[_MemTransport, SimpleNamespace]:
    transport = _MemTransport()
    client = SimpleNamespace(
        async_list_devices=AsyncMock(return_value=[_sensor()]),
        async_list_room_groups=AsyncMock(return_value=[]),
        async_list_holidays=AsyncMock(return_value=[]),
        device_list_homes={HOME: 1},
        device_home={"sensor_1": HOME},
        async_device_rooms=AsyncMock(side_effect=device_reads),
        async_list_rooms=AsyncMock(return_value=ROOMS),
        async_sensor_room_save=AsyncMock(return_value=True),
        async_sensor_room_delete=AsyncMock(return_value=True),
        async_close=AsyncMock(return_value=None),
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        async_run_bridge(
            config,
            transport=transport,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            stop_event=stop_event,
        )
    )
    await asyncio.sleep(0.05)
    transport.subs["danfoss2mqtt/+/set"]("danfoss2mqtt/Sonde/set", json.dumps(payload))
    await asyncio.sleep(0.05)
    stop_event.set()
    await task
    return transport, client


async def test_bridge_routes_sensor_room_and_republishes_at_once() -> None:
    config = BridgeConfig(poll_interval=30, sensor_rooms_writes=True)
    transport, client = await _run(
        config,
        {"sensor_room": {"room_id": "71"}, "temperature": 20},
        # poll read, fresh read, read-back, then the poll cycle the write wakes up
        [{}, {}, {"sensor_1": "71"}, {"sensor_1": "71"}],
    )
    client.async_sensor_room_save.assert_awaited_once_with("sensor_1", "71")
    published = _topic(transport, "sensor_rooms")
    assert published[0] == {"sensor_1": {"home_id": HOME, "room_id": None, "rooms": ROOMS}}
    assert {"sensor_1": {"home_id": HOME, "room_id": "71", "rooms": ROOMS}} in published
    assert _descriptor(transport, "sensor_1")["danfoss"]["set_keys"] == ["sensor_room"]


async def test_bridge_refuses_sensor_room_while_closed() -> None:
    transport, client = await _run(
        BridgeConfig(poll_interval=30), {"sensor_room": {"room_id": "71"}}, [{}]
    )
    client.async_sensor_room_save.assert_not_awaited()
    assert client.async_device_rooms.await_count == 1  # the poll read only
    assert _descriptor(transport, "sensor_1")["danfoss"]["set_keys"] == []


def test_options_open_the_sensor_gate_only_when_asked(tmp_path) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"base_topic": "danfoss2mqtt"}), encoding="utf-8")
    args = bridge_daemon.build_arg_parser().parse_args(["--options", str(options)])
    config = bridge_daemon.load_bridge_config(args)
    assert config.sensor_rooms_writes is False and config.sensor_rooms_trial_sensor is None
    options.write_text(
        json.dumps({"sensor_rooms_writes": True, "sensor_rooms_trial_sensor": " sensor_1 "}),
        encoding="utf-8",
    )
    config = bridge_daemon.load_bridge_config(args)
    assert config.sensor_rooms_writes is True and config.sensor_rooms_trial_sensor == "sensor_1"
    options.write_text(json.dumps({"sensor_rooms_writes": "nope"}), encoding="utf-8")
    assert bridge_daemon.load_bridge_config(args).sensor_rooms_writes is False


def test_daemon_advertises_sensor_room_only_when_open_and_known() -> None:
    daemon = StandaloneDanfoss2MqttDaemon(_MemTransport())  # type: ignore[arg-type]
    assert daemon.sensor_set_keys_for(_sensor()) == []
    daemon.sensor_rooms_gate = OPEN
    assert daemon.sensor_set_keys_for(_sensor()) == []  # entry not known yet
    daemon.publish_sensor_rooms({"sensor_1": {"home_id": HOME, "room_id": None, "rooms": ROOMS}})
    assert daemon.sensor_set_keys_for(_sensor()) == ["sensor_room"]
    assert daemon.sensor_set_keys_for(_sensor("sensor_2")) == []
