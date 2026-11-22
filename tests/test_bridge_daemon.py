"""Unit tests for Step 3: PahoMqttTransport, BridgeConfig, async_run_bridge & HA Add-on."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydanfoss_cloud.bridge_daemon import (
    _CLOUD_UNSUPPORTED_SET_KEYS,
    AVAIL_OFFLINE,
    AVAIL_ONLINE,
    BridgeConfig,
    PahoMqttTransport,
    StandaloneDanfoss2MqttDaemon,
    _build_holidays,
    _build_rooms,
    _group_setpoint_dps,
    _topic_matches,
    async_run_bridge,
    build_arg_parser,
    load_bridge_config,
    main,
)
from pydanfoss_cloud.client import CloudError, CloudWriteError
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.rooms import Room, RoomState


class _FakePahoClient:
    """In-memory paho-mqtt Client double."""

    def __init__(self, **kwargs: Any) -> None:
        self.init_kwargs = kwargs
        self.username: str | None = None
        self.password: str | None = None
        self.tls_called = False
        self.will: tuple[str, str, int, bool] | None = None
        self.reconnect_delay: tuple[int, int] | None = None
        self.connected_to: tuple[str, int, int] | None = None
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False
        self.published: list[tuple[str, str, int, bool]] = []
        self.subscribed: list[tuple[str, int]] = []
        self.on_connect: Any = None
        self.on_disconnect: Any = None
        self.on_message: Any = None

    def username_pw_set(self, username: str, password: str | None = None) -> None:
        self.username = username
        self.password = password

    def tls_set(self) -> None:
        self.tls_called = True

    def will_set(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> None:
        self.will = (topic, payload, qos, retain)

    def reconnect_delay_set(self, min_delay: int = 1, max_delay: int = 120) -> None:
        self.reconnect_delay = (min_delay, max_delay)

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self.connected_to = (host, port, keepalive)

    def loop_start(self) -> None:
        self.loop_started = True

    def loop_stop(self) -> None:
        self.loop_stopped = True

    def disconnect(self) -> None:
        self.disconnected = True

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> None:
        self.published.append((topic, payload, qos, retain))

    def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append((topic, qos))


def _fake_paho_module(holder: dict[str, _FakePahoClient]) -> Any:
    class _FakePaho:
        MQTTv311 = 4

        @staticmethod
        def Client(**kwargs: Any) -> _FakePahoClient:
            client = _FakePahoClient(**kwargs)
            holder["client"] = client
            return client

    return _FakePaho


def _head(dev_id: str = "dev_1", name: str = "Salle 101") -> DanfossDevice:
    return DanfossDevice(
        id=dev_id,
        name=name,
        category="head",
        product_id="03wvrwpv",
        online=True,
        temperature=20.5,
        setpoint=21.0,
        battery=90,
        valve_opening=45,
        mode="at_home",
    )


def test_topic_matches_wildcards() -> None:
    assert _topic_matches("danfoss2mqtt/+/set", "danfoss2mqtt/salle_101/set")
    assert not _topic_matches("danfoss2mqtt/+/set", "danfoss2mqtt/salle_101/state")
    assert not _topic_matches("danfoss2mqtt/+/set", "danfoss2mqtt/a/b/set")
    assert _topic_matches("danfoss2mqtt/#", "danfoss2mqtt/bridge/request/refresh")
    assert not _topic_matches("danfoss2mqtt/a/b", "danfoss2mqtt/a")


def test_paho_transport_lifecycle_lwt_auth_and_dispatch() -> None:
    holder: dict[str, _FakePahoClient] = {}
    transport = PahoMqttTransport(
        "mosquitto.local",
        8883,
        base_topic="danfoss2mqtt/",
        username="mqtt_user",
        password="secret",
        tls=True,
        client_id="bridge-test",
        paho_module=_fake_paho_module(holder),
    )

    with pytest.raises(RuntimeError, match="not connected"):
        transport.publish("danfoss2mqtt/test", "1")

    received: list[tuple[str, str]] = []

    def _bad_cb(_topic: str, _payload: str) -> None:
        raise ValueError("boom")

    transport.subscribe("danfoss2mqtt/+/set", lambda t, p: received.append((t, p)))
    transport.subscribe("danfoss2mqtt/+/set", _bad_cb)

    transport.connect(keepalive=30)
    client = holder["client"]
    assert client.username == "mqtt_user"
    assert client.password == "secret"
    assert client.tls_called is True
    assert client.will == ("danfoss2mqtt/bridge/state", AVAIL_OFFLINE, 1, True)
    assert client.reconnect_delay == (2, 60)
    assert client.connected_to == ("mosquitto.local", 8883, 30)
    assert client.loop_started is True

    # Refused connect does not mark connected
    client.on_connect(client, None, None, 5)
    assert transport.connected is False

    # Accepted connect marks connected and auto-subscribes registered topics
    client.on_connect(client, None, None, 0)
    assert transport.connected is True
    assert ("danfoss2mqtt/+/set", 1) in client.subscribed

    # Subscribing while already connected immediately subscribes on client
    transport.subscribe("danfoss2mqtt/bridge/request/+", lambda t, p: received.append((t, p)))
    assert ("danfoss2mqtt/bridge/request/+", 1) in client.subscribed

    # Publish retained vs non-retained
    transport.publish("danfoss2mqtt/bridge/state", AVAIL_ONLINE, retain=True)
    transport.publish("danfoss2mqtt/bridge/response/refresh", "{}", retain=False)
    assert ("danfoss2mqtt/bridge/state", AVAIL_ONLINE, 1, True) in client.published
    assert ("danfoss2mqtt/bridge/response/refresh", "{}", 0, False) in client.published

    # Incoming message dispatch (bytes + callback error swallowed)
    client.on_message(
        client,
        None,
        SimpleNamespace(topic="danfoss2mqtt/salle_101/set", payload=b'{"child_lock":"LOCK"}'),
    )
    assert received == [("danfoss2mqtt/salle_101/set", '{"child_lock":"LOCK"}')]

    # Unexpected disconnect + clean disconnect (idempotent)
    client.on_disconnect(client, None, 7)
    assert transport.connected is False
    transport.disconnect()
    assert client.loop_stopped is True
    assert client.disconnected is True
    transport.disconnect()  # safe no-op when already disconnected


def test_load_bridge_config_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    opts = tmp_path / "options.json"
    opts.write_text(
        json.dumps(
            {
                "mqtt_host": "core-mosquitto",
                "mqtt_port": 1883,
                "base_topic": "danfoss_school",
                "username": "user@example.test",
                "password": "pw",
                "sign_key": "sk",
                "device_id": "did",
                "ch_key": "ck",
                "poll_interval": 45,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DANFOSS_MQTT_HOST", "env-mosquitto")
    monkeypatch.setenv("DANFOSS_MQTT_USERNAME", "env-user")

    args = build_arg_parser().parse_args(
        ["--options", str(opts), "--mqtt-port", "1884", "--dry-run"]
    )
    cfg = load_bridge_config(args)
    assert cfg.mqtt_host == "core-mosquitto"  # options.json beats env
    assert cfg.mqtt_port == 1884  # CLI beats options.json
    assert cfg.mqtt_username == "env-user"  # fallback to env
    assert cfg.base_topic == "danfoss_school"
    assert cfg.poll_interval == 45
    assert cfg.has_cloud_credentials is True

    # main --dry-run exits 0 cleanly
    assert main(["--options", str(opts), "--dry-run"]) == 0
    # main without cloud credentials also exits 0 cleanly
    assert main(["--mqtt-host", "localhost"]) == 0


async def test_async_run_bridge_poll_set_refresh_and_shutdown() -> None:
    published: list[tuple[str, str, bool]] = []
    subs: dict[str, Any] = {}

    class _MemTransport:
        def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
            published.append((topic, payload, retain))

        def subscribe(self, topic: str, callback: Any) -> None:
            subs[topic] = callback

    fake_client = SimpleNamespace(
        async_list_devices=AsyncMock(return_value=[_head()]),
        async_list_room_groups=AsyncMock(
            return_value=[
                Room(
                    group_id="grp1",
                    name="Salle 101",
                    pid="pidX",
                    home_gid="home9",
                    members=("dev_1",),
                )
            ]
        ),
        async_list_holidays=AsyncMock(
            return_value=[{"gid": "home9", "name": "Maison", "active": False, "scheduled": False}]
        ),
        async_set_room_setpoint=AsyncMock(return_value="grp1"),
        async_close=AsyncMock(return_value=None),
    )

    stop_event = asyncio.Event()
    cfg = BridgeConfig(base_topic="danfoss2mqtt", poll_interval=30)

    task = asyncio.create_task(
        async_run_bridge(
            cfg,
            transport=_MemTransport(),
            client=fake_client,  # type: ignore[arg-type]
            stop_event=stop_event,
        )
    )
    await asyncio.sleep(0.05)

    # First poll published bridge/state=online, bridge/devices, and Salle 101 state
    assert ("danfoss2mqtt/bridge/state", AVAIL_ONLINE, True) in published
    assert any(t == "danfoss2mqtt/bridge/devices" for t, _, _ in published)

    # Fix #2: bridge/rooms is published retained, carrying home_id + members (contract §3)
    rooms_pub = [p for t, p, retain in published if t == "danfoss2mqtt/bridge/rooms" and retain]
    assert rooms_pub, "bridge/rooms must be published"
    rooms_json = json.loads(rooms_pub[-1])
    assert rooms_json == [
        {
            "group_id": "grp1",
            "name": "Salle 101",
            "pid": "pidX",
            "home_id": "home9",
            "members": ["dev_1"],
            "head_count": 1,
            "online": True,
            "temperature": 20.5,
            "setpoint": 21.0,
            "setpoints": {},
            "mode": "at_home",
            "heating": True,
        }
    ]
    # Fix #2: bridge/holidays is published retained, keyed by home gid
    hol_pub = [p for t, p, retain in published if t == "danfoss2mqtt/bridge/holidays" and retain]
    assert hol_pub, "bridge/holidays must be published"
    assert json.loads(hol_pub[-1]) == {
        "home9": {"gid": "home9", "name": "Maison", "active": False, "scheduled": False}
    }

    # A setpoint /set goes through the proven room path and triggers an immediate re-poll
    subs["danfoss2mqtt/+/set"](
        "danfoss2mqtt/Salle 101/set",
        json.dumps({"occupied_heating_setpoint": 21.0}),
    )
    await asyncio.sleep(0.05)
    fake_client.async_set_room_setpoint.assert_awaited_once_with("dev_1", {"118": 210})

    # An unsupported /set field (child lock) is logged, never silently written, and does not route
    fake_client.async_set_room_setpoint.reset_mock()
    subs["danfoss2mqtt/+/set"](
        "danfoss2mqtt/Salle 101/set",
        json.dumps({"child_lock": "LOCK"}),
    )
    await asyncio.sleep(0.05)
    fake_client.async_set_room_setpoint.assert_not_awaited()

    # bridge/request/refresh succeeds; unknown action returns error
    subs["danfoss2mqtt/bridge/request/+"](
        "danfoss2mqtt/bridge/request/refresh",
        json.dumps({"transaction": "tx-1"}),
    )
    subs["danfoss2mqtt/bridge/request/+"](
        "danfoss2mqtt/bridge/request/unknown",
        json.dumps({"transaction": "tx-2"}),
    )
    await asyncio.sleep(0.05)
    assert any(t == "danfoss2mqtt/bridge/response/refresh" and '"ok"' in p for t, p, _ in published)
    assert any(
        t == "danfoss2mqtt/bridge/response/unknown" and '"error"' in p for t, p, _ in published
    )

    # Clean shutdown marks bridge offline
    stop_event.set()
    await task
    assert published[-1] == ("danfoss2mqtt/bridge/state", AVAIL_OFFLINE, True)


async def test_async_run_bridge_requires_credentials_without_client() -> None:
    class _NoopTransport:
        def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
            pass

        def subscribe(self, topic: str, callback: Any) -> None:
            pass

    with pytest.raises(ValueError, match="credentials"):
        await async_run_bridge(BridgeConfig(), transport=_NoopTransport())


def test_group_setpoint_dps_is_mode_aware() -> None:
    # Comfort slot (at_home): DP 118 in tenths of °C, rounded to the 0.5 °C step.
    assert _group_setpoint_dps(20.5, "at_home") == {"118": 205}
    assert _group_setpoint_dps(21.0, "at_home") == {"118": 210}
    # Setback slot (leaving_home): DP 109 AND DP 119, never comfort.
    assert _group_setpoint_dps(15.0, "leaving_home") == {"109": 150, "119": 150}
    # Any other mode (or none) has no proven path-1a group method: refuse, don't write comfort.
    for mode in ("manual", "pause", "holiday", "holiday_sat", None):
        with pytest.raises(CloudWriteError, match="comfort .* or setback"):
            _group_setpoint_dps(21.0, mode)


class _MemTransport:
    """In-memory MqttTransport double recording publishes and holding subscriptions."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self.subs: dict[str, Any] = {}

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: Any) -> None:
        self.subs[topic] = callback


def _fake_cloud(devices: list[DanfossDevice]) -> SimpleNamespace:
    return SimpleNamespace(
        async_list_devices=AsyncMock(return_value=devices),
        async_list_room_groups=AsyncMock(return_value=[]),
        async_list_holidays=AsyncMock(return_value=[]),
        async_set_room_setpoint=AsyncMock(return_value="grp1"),
        async_close=AsyncMock(return_value=None),
    )


async def _run_set(dev: DanfossDevice, payload: dict) -> SimpleNamespace:
    """Start the bridge with one device, deliver one ``/set`` payload, return the fake client."""
    transport = _MemTransport()
    client = _fake_cloud([dev])
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        async_run_bridge(
            BridgeConfig(base_topic="danfoss2mqtt", poll_interval=30),
            transport=transport,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            stop_event=stop_event,
        )
    )
    await asyncio.sleep(0.05)
    transport.subs["danfoss2mqtt/+/set"](f"danfoss2mqtt/{dev.name}/set", json.dumps(payload))
    await asyncio.sleep(0.05)
    stop_event.set()
    await task
    return client


async def test_setpoint_write_setback_mode_targets_setback_dps() -> None:
    # Fix #3: a head whose current slot is setback must write the SETBACK setpoint (DP 109 + 119),
    # not comfort. Regression on the real-hardware bug (E-2 Circulation, 2026-11-22).
    head = _head(dev_id="dev_sb", name="Circulation")
    head.mode = "leaving_home"
    client = await _run_set(head, {"occupied_heating_setpoint": 15.0})
    client.async_set_room_setpoint.assert_awaited_once_with("dev_sb", {"109": 150, "119": 150})


async def test_setpoint_write_refused_outside_comfort_or_setback() -> None:
    # Fix #3: a head in any other mode (manual/pause/holiday) has no proven group method; the write
    # is refused, never silently applied to comfort.
    head = _head(dev_id="dev_m", name="Manual")
    head.mode = "manual"
    client = await _run_set(head, {"occupied_heating_setpoint": 21.0})
    client.async_set_room_setpoint.assert_not_awaited()


def test_publish_fleet_rooms_payload_shape() -> None:
    # Fix #2: the bridge/rooms payload carries home_id + members per contract §3.
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport, base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    room = RoomState(
        group_id="g5",
        name="Chapelle",
        pid="pidX",
        head_count=2,
        online=True,
        temperature=18.5,
        setpoint=19.0,
        mode="at_home",
        heating=False,
        home_gid="home9",
        members=("dev_a", "dev_b"),
    )
    daemon.publish_fleet([_head()], rooms=[room], holidays={"home9": {"active": True}})
    rooms_pub = [p for t, p, retain in transport.published if t == "danfoss2mqtt/bridge/rooms"]
    assert json.loads(rooms_pub[-1])[0]["home_id"] == "home9"
    assert json.loads(rooms_pub[-1])[0]["members"] == ["dev_a", "dev_b"]
    hol_pub = [p for t, p, r in transport.published if t == "danfoss2mqtt/bridge/holidays"]
    assert json.loads(hol_pub[-1]) == {"home9": {"active": True}}


def test_publish_fleet_omits_room_and_holiday_topics_when_none() -> None:
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport, base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    daemon.publish_fleet([_head()])
    topics = {t for t, _, _ in transport.published}
    assert "danfoss2mqtt/bridge/rooms" not in topics
    assert "danfoss2mqtt/bridge/holidays" not in topics


async def test_build_rooms_aggregates_members_and_degrades() -> None:
    head = _head(dev_id="dev_1", name="Salle 101")
    groups = [Room(group_id="g1", name="Salle 101", pid="p", home_gid="h1", members=("dev_1",))]
    client = SimpleNamespace(async_list_room_groups=AsyncMock(return_value=groups))
    rooms = await _build_rooms(client, [head])  # type: ignore[arg-type]
    assert rooms is not None and len(rooms) == 1
    assert rooms[0].members == ("dev_1",)
    assert rooms[0].home_gid == "h1"
    assert rooms[0].online is True

    # Empty account -> [] (topic still published), read failure -> None (topic left untouched).
    empty = SimpleNamespace(async_list_room_groups=AsyncMock(return_value=[]))
    assert await _build_rooms(empty, [head]) == []  # type: ignore[arg-type]
    boom = SimpleNamespace(async_list_room_groups=AsyncMock(side_effect=CloudError("down")))
    assert await _build_rooms(boom, [head]) is None  # type: ignore[arg-type]


async def test_build_holidays_maps_by_gid_and_degrades() -> None:
    client = SimpleNamespace(
        async_list_holidays=AsyncMock(
            return_value=[
                {"gid": "h1", "name": "A", "active": True},
                {"name": "no gid"},  # dropped: no gid
            ]
        )
    )
    assert await _build_holidays(client) == {  # type: ignore[arg-type]
        "h1": {"gid": "h1", "name": "A", "active": True}
    }
    boom = SimpleNamespace(async_list_holidays=AsyncMock(side_effect=CloudError("down")))
    assert await _build_holidays(boom) is None  # type: ignore[arg-type]


def test_unsupported_set_keys_cover_non_cloud_fields() -> None:
    # Fields that only reach the head through Tuya cloud MQTT or the local tunnel must be refused by
    # the standalone cloud bridge, not silently dropped.
    for key in (
        "keypad_lockout",
        "child_lock",
        "window_open_feature",
        "max_heat_setpoint_limit",
        "min_heat_setpoint_limit",
        "programming_operation_mode",
        "external_measured_room_sensor",
        "load_room_mean",
        "weekly_schedule",
    ):
        assert key in _CLOUD_UNSUPPORTED_SET_KEYS


def test_load_bridge_config_auto_generates_and_persists_device_id(tmp_path: Path) -> None:
    opts = tmp_path / "options.json"
    opts.write_text(
        json.dumps(
            {
                "username": "user@example.test",
                "password": "pw",
                "sign_key": "sk",
                "ch_key": "ck",
            }
        ),
        encoding="utf-8",
    )
    args = build_arg_parser().parse_args(["--options", str(opts)])
    cfg1 = load_bridge_config(args)
    assert cfg1.has_cloud_credentials is True
    assert cfg1.device_id is not None and len(cfg1.device_id) == 44
    assert (tmp_path / "device_id").read_text(encoding="utf-8").strip() == cfg1.device_id

    # Second load reuses the exact same persisted device_id (stable across restarts)
    cfg2 = load_bridge_config(args)
    assert cfg2.device_id == cfg1.device_id


def test_publish_fleet_includes_device_home_id() -> None:
    from pydanfoss_cloud.bridge_daemon import StandaloneDanfoss2MqttDaemon

    published: dict[str, str] = {}

    class _Mem:
        def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
            published[topic] = payload

        def subscribe(self, topic: str, callback: Any) -> None:
            pass

    daemon = StandaloneDanfoss2MqttDaemon(_Mem())
    daemon.publish_fleet([_head("dev_1", "A-0 A03")], device_home={"dev_1": "244975257"})
    devs = json.loads(published["danfoss2mqtt/bridge/devices"])
    assert devs[0]["danfoss"]["home_id"] == "244975257"
