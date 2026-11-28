"""Boiler relay room linkage written by the bridge (``room_link``). Synthetic data only."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydanfoss_cloud import bridge_daemon
from pydanfoss_cloud.bridge_daemon import (
    BridgeConfig,
    RelayLinks,
    StandaloneDanfoss2MqttDaemon,
    async_run_bridge,
    write_relay_link,
)
from pydanfoss_cloud.client import CloudError, CloudWriteError
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.relay_links import (
    RelayLinksGate,
    RoomLink,
    check_room_link,
    link_confirmed,
    parse_room_link,
)

OPEN = RelayLinksGate(enabled=True)


def _relay(dev_id: str = "relay_1", name: str = "Relais") -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=name, category="relay", online=True, relay_on=False)


def _head(dev_id: str = "dev_1") -> DanfossDevice:
    return DanfossDevice(id=dev_id, name="Salle 101", category="head", product_id="03wvrwpv")


def _rooms(*linked: bool) -> list[dict[str, Any]]:
    return [
        {"room_id": str(70 + i), "name": f"Salle {i}", "linked": flag}
        for i, flag in enumerate(linked)
    ]


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


# --- pure logic --------------------------------------------------------------------------------


def test_gate_closed_by_default_and_trial_confines_to_one_relay() -> None:
    assert RelayLinksGate().open_for("relay_1") is False
    trial = RelayLinksGate(enabled=True, trial_relay="relay_1")
    assert trial.open_for("relay_1") is True
    assert trial.open_for("relay_2") is False
    assert OPEN.open_for("relay_2") is True


def test_parse_room_link_accepts_only_an_id_and_a_boolean() -> None:
    assert parse_room_link({"room_id": "71", "linked": True}) == RoomLink("71", True)
    assert parse_room_link({"room_id": 71, "linked": False}) == RoomLink("71", False)
    for bad in (
        "71",
        None,
        {"linked": True},
        {"room_id": "", "linked": True},
        {"room_id": True, "linked": True},
        {"room_id": "71", "linked": "true"},
        {"room_id": "71", "linked": 1},
        {"room_id": "71"},
    ):
        with pytest.raises(CloudWriteError):
            parse_room_link(bad)


def test_check_room_link_against_the_fresh_read() -> None:
    rooms = _rooms(True, False, True)
    assert check_room_link(RoomLink("71", True), rooms) is True  # link an unlinked room
    assert check_room_link(RoomLink("70", False), rooms) is True  # unlink one of two
    assert check_room_link(RoomLink("70", True), rooms) is False  # already linked: nothing
    assert check_room_link(RoomLink("71", False), rooms) is False  # already unlinked
    with pytest.raises(CloudWriteError, match="eligible"):
        check_room_link(RoomLink("99", True), rooms)
    with pytest.raises(CloudWriteError, match="last linked room"):
        check_room_link(RoomLink("70", False), _rooms(True, False))


def test_link_confirmed() -> None:
    assert link_confirmed(RoomLink("71", True), _rooms(False, True)) is True
    assert link_confirmed(RoomLink("71", True), _rooms(False, False)) is False
    assert link_confirmed(RoomLink("99", True), _rooms(True)) is False


# --- advertising -------------------------------------------------------------------------------


def test_room_link_is_advertised_only_when_open_and_rooms_known() -> None:
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport)  # type: ignore[arg-type]
    devices = [_head(), _relay(), _relay("relay_2", "Relais 2")]
    links = {"relay_1": _rooms(True, False)}

    daemon.publish_fleet(devices, relays=links)
    assert _descriptor(transport, "relay_1")["danfoss"]["set_keys"] == []  # gate closed

    daemon.relay_links_gate = RelayLinksGate(enabled=True, trial_relay="relay_1")
    daemon.publish_fleet(devices, relays=links)
    assert _descriptor(transport, "relay_1")["danfoss"]["set_keys"] == ["room_link"]
    assert _descriptor(transport, "relay_2")["danfoss"]["set_keys"] == []  # not the trial relay

    daemon.relay_links_gate = OPEN
    daemon.publish_fleet(devices, relays=links)
    # relay_2's rooms are unknown: nothing to choose from, so nothing advertised.
    assert _descriptor(transport, "relay_2")["danfoss"]["set_keys"] == []
    # The head's own keys are untouched.
    assert "room_link" not in _descriptor(transport, "dev_1")["danfoss"]["set_keys"]


# --- the write ---------------------------------------------------------------------------------


def _daemon(gate: RelayLinksGate = OPEN) -> StandaloneDanfoss2MqttDaemon:
    daemon = StandaloneDanfoss2MqttDaemon(_MemTransport())  # type: ignore[arg-type]
    daemon.relay_links_gate = gate
    return daemon


def _client(reads: list[Any]) -> SimpleNamespace:
    return SimpleNamespace(
        async_relay_rooms=AsyncMock(side_effect=reads),
        async_relay_bind=AsyncMock(return_value=True),
    )


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bridge_daemon, "RELAY_CONFIRM_DELAY_S", 0.0)


async def test_link_is_validated_written_and_read_back() -> None:
    before, after = _rooms(True, False), _rooms(True, True)
    client = _client([before, after])
    links = RelayLinks()
    ok = await write_relay_link(
        client,  # type: ignore[arg-type]
        _daemon(),
        links,
        _relay(),
        {"room_id": "71", "linked": True},
    )
    assert ok is True
    client.async_relay_bind.assert_awaited_once_with("relay_1", "71", True)
    assert client.async_relay_rooms.await_count == 2  # fresh read, then the read-back
    assert links.snapshot() == {"relay_1": after}


async def test_unconfirmed_write_publishes_what_was_read(caplog: pytest.LogCaptureFixture) -> None:
    before = _rooms(True, True)
    # The read-back keeps failing or showing the previous state.
    client = _client([before, CloudError("boom"), before, before])
    links = RelayLinks()
    ok = await write_relay_link(
        client,  # type: ignore[arg-type]
        _daemon(),
        links,
        _relay(),
        {"room_id": "71", "linked": False},
    )
    assert ok is True
    client.async_relay_bind.assert_awaited_once_with("relay_1", "71", False)
    assert client.async_relay_rooms.await_count == 1 + bridge_daemon.RELAY_CONFIRM_ATTEMPTS
    assert links.snapshot() == {"relay_1": before}  # never the asked state
    assert "NOT confirmed" in caplog.text


async def test_failed_bind_is_still_read_back() -> None:
    before = _rooms(True, False)
    client = _client([before, before, before, before])
    client.async_relay_bind.side_effect = CloudError("refused")
    links = RelayLinks()
    assert await write_relay_link(
        client,  # type: ignore[arg-type]
        _daemon(),
        links,
        _relay(),
        {"room_id": "71", "linked": True},
    )
    assert links.snapshot() == {"relay_1": before}


@pytest.mark.parametrize(
    ("gate", "dev", "raw"),
    [
        (RelayLinksGate(), _relay(), {"room_id": "71", "linked": True}),  # gate closed
        (
            RelayLinksGate(enabled=True, trial_relay="other"),
            _relay(),
            {"room_id": "71", "linked": True},
        ),
        (OPEN, _head(), {"room_id": "71", "linked": True}),  # not a relay
        (OPEN, _relay(), {"room_id": "71", "linked": "yes"}),  # invalid value
    ],
)
async def test_refused_before_any_request(
    gate: RelayLinksGate, dev: DanfossDevice, raw: Any
) -> None:
    client = _client([])
    ok = await write_relay_link(client, _daemon(gate), RelayLinks(), dev, raw)  # type: ignore[arg-type]
    assert ok is False
    client.async_relay_rooms.assert_not_awaited()
    client.async_relay_bind.assert_not_awaited()


@pytest.mark.parametrize(
    ("rooms", "raw"),
    [
        (_rooms(True, False), {"room_id": "99", "linked": True}),  # not eligible
        (_rooms(True, False), {"room_id": "70", "linked": False}),  # last linked room
        (_rooms(True, True), {"room_id": "71", "linked": True}),  # already linked
    ],
)
async def test_nothing_sent_after_the_fresh_read(rooms: list[dict[str, Any]], raw: Any) -> None:
    client = _client([rooms])
    links = RelayLinks()
    ok = await write_relay_link(client, _daemon(), links, _relay(), raw)  # type: ignore[arg-type]
    assert ok is True  # rooms were read: bridge/relays is republished with the truth
    client.async_relay_bind.assert_not_awaited()
    assert links.snapshot() == {"relay_1": rooms}


async def test_unreadable_rooms_send_nothing() -> None:
    client = _client([CloudError("boom")])
    ok = await write_relay_link(
        client,  # type: ignore[arg-type]
        _daemon(),
        RelayLinks(),
        _relay(),
        {"room_id": "71", "linked": True},
    )
    assert ok is False
    client.async_relay_bind.assert_not_awaited()


# --- end to end through the bridge -------------------------------------------------------------


async def _run(config: BridgeConfig, payload: dict[str, Any], reads: list[Any]) -> tuple[Any, Any]:
    transport = _MemTransport()
    relay = _relay()
    client = SimpleNamespace(
        async_list_devices=AsyncMock(return_value=[relay]),
        async_list_room_groups=AsyncMock(return_value=[]),
        async_list_holidays=AsyncMock(return_value=[]),
        async_relay_rooms=AsyncMock(side_effect=reads),
        async_relay_bind=AsyncMock(return_value=True),
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
    transport.subs["danfoss2mqtt/+/set"]("danfoss2mqtt/Relais/set", json.dumps(payload))
    await asyncio.sleep(0.05)
    stop_event.set()
    await task
    return transport, client


async def test_bridge_routes_room_link_and_republishes_relays_at_once() -> None:
    before, after = _rooms(True, False), _rooms(True, True)
    config = BridgeConfig(poll_interval=30, relay_links_writes=True)
    transport, client = await _run(
        config,
        {"room_link": {"room_id": "71", "linked": True}, "state": "ON"},
        [before, before, after],  # poll read, fresh read, read-back
    )
    client.async_relay_bind.assert_awaited_once_with("relay_1", "71", True)
    relays = [json.loads(p) for t, p, _ in transport.published if t == "danfoss2mqtt/bridge/relays"]
    assert relays[0] == {"relay_1": before}
    assert relays[-1] == {"relay_1": after}  # published right after the write, not at the TTL
    assert _descriptor(transport, "relay_1")["danfoss"]["set_keys"] == ["room_link"]


async def test_bridge_refuses_room_link_while_closed() -> None:
    before = _rooms(True, False)
    transport, client = await _run(
        BridgeConfig(poll_interval=30),
        {"room_link": {"room_id": "71", "linked": True}},
        [before],
    )
    client.async_relay_bind.assert_not_awaited()
    assert client.async_relay_rooms.await_count == 1  # the poll read only
    assert _descriptor(transport, "relay_1")["danfoss"]["set_keys"] == []


def test_options_open_the_relay_gate_only_when_asked(tmp_path) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"base_topic": "danfoss2mqtt"}), encoding="utf-8")
    args = bridge_daemon.build_arg_parser().parse_args(["--options", str(options)])
    config = bridge_daemon.load_bridge_config(args)
    assert config.relay_links_writes is False and config.relay_links_trial_relay is None
    options.write_text(
        json.dumps({"relay_links_writes": True, "relay_links_trial_relay": " relay_1 "}),
        encoding="utf-8",
    )
    config = bridge_daemon.load_bridge_config(args)
    assert config.relay_links_writes is True and config.relay_links_trial_relay == "relay_1"
    options.write_text(json.dumps({"relay_links_writes": "nope"}), encoding="utf-8")
    assert bridge_daemon.load_bridge_config(args).relay_links_writes is False
