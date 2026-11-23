"""Synthetic tests for the read-only room coordinator daemon (no network, no real site data).

A fake transport records every publish and routes injected messages to the subscribed callbacks,
so a full publish/subscribe cycle is exercised without a broker. The central guardrail of Stage 1 —
the coordinator never writes to a head — is asserted explicitly (`test_never_publishes_set`).
"""

from __future__ import annotations

import json

import pytest

from pydanfoss_cloud.bridge_daemon import _topic_matches
from pydanfoss_cloud.room_coordinator import RoomCoordinator
from pydanfoss_cloud.room_store import RoomStore


class FakeTransport:
    """Records publishes; delivers injected messages to matching subscription callbacks."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self._subs: list[tuple[str, object]] = []

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic, callback) -> None:  # noqa: ANN001
        self._subs.append((topic, callback))

    # test helpers -------------------------------------------------------------------------------

    def inject(self, topic: str, payload) -> None:  # noqa: ANN001
        text = payload if isinstance(payload, str) else json.dumps(payload)
        for sub, cb in self._subs:
            if _topic_matches(sub, topic):
                cb(topic, text)

    def last(self, topic: str):
        for t, p, _r in reversed(self.published):
            if t == topic:
                return p
        return None

    def topics(self) -> set[str]:
        return {t for t, _p, _r in self.published}


def _descriptor(friendly: str, ieee: str, model: str = "014G2461") -> dict:
    return {
        "friendly_name": friendly,
        "ieee_address": ieee,
        "definition": {"vendor": "Danfoss", "model": model},
    }


def _make(source="zigbee2mqtt", base="danfoss-rooms", store=None):
    tx = FakeTransport()
    coord = RoomCoordinator(
        tx, store if store is not None else RoomStore(), source_prefix=source, base_topic=base
    )
    return tx, coord


def test_source_and_base_must_differ() -> None:
    with pytest.raises(ValueError):
        RoomCoordinator(FakeTransport(), RoomStore(), source_prefix="z", base_topic="z")


def test_start_announces_and_subscribes() -> None:
    tx, coord = _make()
    coord.start()
    assert tx.last("danfoss-rooms/bridge/state") == "online"
    assert tx.last("danfoss-rooms/bridge/rooms") == "[]"
    assert tx.last("danfoss-rooms/bridge/heat_demand") is not None
    subs = {s for s, _ in coord._tx._subs}  # type: ignore[attr-defined]
    assert "zigbee2mqtt/#" in subs
    assert "danfoss-rooms/bridge/request/#" in subs


def test_rooms_aggregate_reported_state() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA", "0xBBBB"], room_id="r1")
    tx, coord = _make(store=store)
    coord.start()

    tx.inject(
        "zigbee2mqtt/bridge/devices",
        [_descriptor("liv_1", "0xAAAA"), _descriptor("liv_2", "0xBBBB")],
    )
    tx.inject(
        "zigbee2mqtt/liv_1",
        {"local_temperature": 20.0, "occupied_heating_setpoint": 21.0, "pi_heating_demand": 40},
    )
    tx.inject(
        "zigbee2mqtt/liv_2",
        {"local_temperature": 22.0, "occupied_heating_setpoint": 21.0, "pi_heating_demand": 0},
    )

    rooms = json.loads(tx.last("danfoss-rooms/bridge/rooms"))
    assert len(rooms) == 1
    assert rooms[0]["group_id"] == "r1"
    assert rooms[0]["temperature"] == 21.0
    assert rooms[0]["setpoint"] == 21.0
    assert rooms[0]["heating"] is True

    hd = json.loads(tx.last("danfoss-rooms/bridge/heat_demand"))
    assert hd["calling"] is True
    assert hd["rooms_calling"][0]["group_id"] == "r1"


def test_availability_offline_marks_room_offline() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _make(store=store)
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    assert json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]["online"] is True
    tx.inject("zigbee2mqtt/liv_1/availability", "offline")
    assert json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]["online"] is False


def test_non_danfoss_and_grouped_topics_ignored() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _make(store=store)
    coord.start()
    # A Sonoff router descriptor is not Danfoss -> ignored.
    tx.inject(
        "zigbee2mqtt/bridge/devices",
        [{"friendly_name": "router", "ieee_address": "0xZZ", "definition": {"vendor": "Sonoff"}}],
    )
    assert (
        json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]["head_count"] == 1
    )  # still 0 members matched
    assert json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]["online"] is False
    # Grouped topic and set echo must not crash or be treated as state.
    tx.inject("zigbee2mqtt/some/group", {"x": 1})
    tx.inject("zigbee2mqtt/liv_1/set", {"occupied_heating_setpoint": 30})


def test_request_create_edits_store_and_responds() -> None:
    tx, coord = _make()
    coord.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/create",
        {"name": "Kitchen", "members": ["0xAAAA"], "room_id": "k1", "transaction": "t7"},
    )
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/create"))
    assert resp["status"] == "ok"
    assert resp["room"]["room_id"] == "k1"
    assert resp["transaction"] == "t7"
    # Store changed and rooms republished.
    rooms = json.loads(tx.last("danfoss-rooms/bridge/rooms"))
    assert [r["group_id"] for r in rooms] == ["k1"]


def test_request_full_vocabulary() -> None:
    store = RoomStore()
    tx, coord = _make(store=store)
    coord.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/create",
        {"name": "A", "members": ["0xA"], "room_id": "r1"},
    )
    tx.inject("danfoss-rooms/bridge/request/room/rename", {"room_id": "r1", "name": "Lounge"})
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-members",
        {"room_id": "r1", "members": ["0xA", "0xB"]},
    )
    tx.inject("danfoss-rooms/bridge/request/room/set-sensor", {"room_id": "r1", "sensor": "0xS"})
    assert store.get("r1").name == "Lounge"
    assert store.get("r1").members == ("0xA", "0xB")
    assert store.get("r1").sensor == "0xS"
    tx.inject("danfoss-rooms/bridge/request/room/delete", {"room_id": "r1"})
    assert len(store) == 0
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/delete"))
    assert resp["status"] == "ok" and resp["room_id"] == "r1"


def test_request_errors() -> None:
    tx, coord = _make()
    coord.start()
    tx.inject("danfoss-rooms/bridge/request/room/rename", {"room_id": "nope", "name": "X"})
    assert json.loads(tx.last("danfoss-rooms/bridge/response/room/rename"))["status"] == "error"
    tx.inject("danfoss-rooms/bridge/request/room/delete", "{not json")
    assert json.loads(tx.last("danfoss-rooms/bridge/response/room/delete"))["status"] == "error"
    tx.inject("danfoss-rooms/bridge/request/room/create", {"name": ""})  # missing name
    assert json.loads(tx.last("danfoss-rooms/bridge/response/room/create"))["status"] == "error"
    tx.inject("danfoss-rooms/bridge/request/bogus", {})
    assert json.loads(tx.last("danfoss-rooms/bridge/response/bogus"))["status"] == "error"


def test_refresh_republishes() -> None:
    tx, coord = _make()
    coord.start()
    before = len([t for t, _p, _r in tx.published if t == "danfoss-rooms/bridge/rooms"])
    tx.inject("danfoss-rooms/bridge/request/refresh", {})
    after = len([t for t, _p, _r in tx.published if t == "danfoss-rooms/bridge/rooms"])
    assert after == before + 1
    assert json.loads(tx.last("danfoss-rooms/bridge/response/refresh"))["refreshed"] is True


def test_never_publishes_set() -> None:
    """Stage 1 guardrail: the coordinator must never write to a head (`<prefix>/<name>/set`)."""
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _make(store=store)
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("zigbee2mqtt/liv_1", {"local_temperature": 20.0, "pi_heating_demand": 50})
    tx.inject("danfoss-rooms/bridge/request/room/set-sensor", {"room_id": "r1", "sensor": "0xS"})
    coord.stop()
    assert not any(t.endswith("/set") for t in tx.topics())
    # Everything the coordinator publishes stays under its own base prefix.
    assert all(t.startswith("danfoss-rooms/") for t in tx.topics())


def test_stop_clears_heat_demand_and_goes_offline() -> None:
    tx, coord = _make()
    coord.start()
    coord.stop()
    assert tx.last("danfoss-rooms/bridge/state") == "offline"
    hd = json.loads(tx.last("danfoss-rooms/bridge/heat_demand"))
    assert hd["calling"] is False


def test_sensor_feeds_room_temperature() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], sensor="0xS1", room_id="r1")
    tx, coord = _make(store=store)
    coord.start()
    tx.inject(
        "zigbee2mqtt/bridge/devices",
        [_descriptor("liv_1", "0xAAAA"), _descriptor("sens_1", "0xS1", model="014G2480")],
    )
    tx.inject("zigbee2mqtt/liv_1", {"local_temperature": 18.0})
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.5, "humidity": 44.0})
    room = json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]
    assert room["temperature"] == 20.5
    assert room["humidity"] == 44.0


# --- Stage 2: setpoint fan-out (closed by default) ---------------------------------------------


def _armed(store, room_id, *, timeout=30.0, retries=2, source="zigbee2mqtt", base="danfoss-rooms"):
    """A coordinator with ``room_id`` armed for setpoint fan-out."""
    tx = FakeTransport()
    coord = RoomCoordinator(
        tx,
        store,
        source_prefix=source,
        base_topic=base,
        armed_rooms=(room_id,),
        setpoint_timeout_s=timeout,
        setpoint_retries=retries,
    )
    return tx, coord


def _join(coord) -> None:  # noqa: ANN001
    for thread in list(coord._jobs):  # type: ignore[attr-defined]
        thread.join(timeout=5.0)
        assert not thread.is_alive()


def _sets(tx) -> list[tuple[str, dict]]:  # noqa: ANN001
    return [(t, json.loads(p)) for t, p, _r in tx.published if t.endswith("/set")]


def test_set_setpoint_closed_by_default_refused_and_silent() -> None:
    """An unarmed room is refused; nothing is sent (the coordinator stays read-only)."""
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _make(store=store)  # no armed rooms
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-setpoint",
        {"room_id": "r1", "setpoint": 21.0, "transaction": "t1"},
    )
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["status"] == "closed"
    assert resp["transaction"] == "t1"
    assert _sets(tx) == []  # not one /set
    # Desired was not seeded either.
    assert json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]["desired"] == {}


def test_set_setpoint_unknown_room_errors_and_silent() -> None:
    store = RoomStore()
    tx, coord = _armed(store, "r1")
    coord.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-setpoint", {"room_id": "nope", "setpoint": 20.0}
    )
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["status"] == "error"
    assert _sets(tx) == []


def test_set_setpoint_invalid_value_refused_and_silent() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _armed(store, "r1")
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("danfoss-rooms/bridge/request/room/set-setpoint", {"room_id": "r1", "setpoint": 20.3})
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["status"] == "error"
    assert resp["job_status"] == "failed"
    assert _sets(tx) == []


def test_set_setpoint_armed_room_fans_out_and_confirms() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA", "0xBBBB"], room_id="r1")
    tx, coord = _armed(store, "r1")
    coord.start()
    tx.inject(
        "zigbee2mqtt/bridge/devices",
        [_descriptor("liv_1", "0xAAAA"), _descriptor("liv_2", "0xBBBB")],
    )
    # Heads start at a different setpoint, so confirmation exercises the write->report cycle.
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 18.0})
    tx.inject("zigbee2mqtt/liv_2", {"occupied_heating_setpoint": 18.0})
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-setpoint",
        {"room_id": "r1", "setpoint": 21.0, "source": "user", "transaction": "t9"},
    )
    # The heads report the new setpoint; the worker confirms each in turn.
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 21.0})
    tx.inject("zigbee2mqtt/liv_2", {"occupied_heating_setpoint": 21.0})
    _join(coord)

    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["status"] == "ok"
    assert resp["job_status"] == "applied"
    assert resp["setpoint"] == 21.0
    assert resp["property"] == "occupied_heating_setpoint"
    assert {h["address"]: h["status"] for h in resp["heads"]} == {
        "0xAAAA": "applied",
        "0xBBBB": "applied",
    }
    assert resp["transaction"] == "t9"
    # One /set per head, with the user property.
    topics = {t for t, _p in _sets(tx)}
    assert topics == {"zigbee2mqtt/liv_1/set", "zigbee2mqtt/liv_2/set"}
    assert all("occupied_heating_setpoint" in p for _t, p in _sets(tx))
    # Desired seeded; with both heads reporting 21.0, no drift.
    room = json.loads(tx.last("danfoss-rooms/bridge/rooms"))[0]
    assert room["desired"] == {"setpoint": 21.0}
    assert room["drift"] == []


def test_set_setpoint_coordinator_source_uses_scheduled_property() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _armed(store, "r1")
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 18.0})
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-setpoint",
        {"room_id": "r1", "setpoint": 19.5, "source": "coordinator"},
    )
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint_scheduled": 19.5})
    _join(coord)
    sets = _sets(tx)
    assert len(sets) == 1
    assert "occupied_heating_setpoint_scheduled" in sets[0][1]
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["property"] == "occupied_heating_setpoint_scheduled"
    assert resp["heads"][0]["status"] == "applied"


def test_set_setpoint_unreachable_on_timeout() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _armed(store, "r1", timeout=0.3, retries=0)
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 18.0})
    tx.inject("danfoss-rooms/bridge/request/room/set-setpoint", {"room_id": "r1", "setpoint": 21.0})
    _join(coord)  # the head never reports 21.0
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["heads"][0]["status"] == "unreachable"
    assert resp["status"] == "ok"
    assert resp["job_status"] == "unconfirmed"
    # The /set was still attempted once (no retries).
    assert len(_sets(tx)) == 1


def test_set_setpoint_mismatch_when_head_reports_other_value() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _armed(store, "r1", timeout=0.3, retries=0)
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("danfoss-rooms/bridge/request/room/set-setpoint", {"room_id": "r1", "setpoint": 21.0})
    # The head reports a different value within the window: a mismatch, not unreachable.
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 19.0})
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["heads"][0]["status"] == "mismatch"


def test_set_setpoint_absent_member_reported_unreachable() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA", "0xCCCC"], room_id="r1")
    tx, coord = _armed(store, "r1", timeout=0.3, retries=0)
    coord.start()
    # Only liv_1 (0xAAAA) is known to the bridge; 0xCCCC was never reported.
    tx.inject("zigbee2mqtt/bridge/devices", [_descriptor("liv_1", "0xAAAA")])
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 18.0})
    tx.inject("danfoss-rooms/bridge/request/room/set-setpoint", {"room_id": "r1", "setpoint": 21.0})
    tx.inject("zigbee2mqtt/liv_1", {"occupied_heating_setpoint": 21.0})
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    byaddr = {h["address"]: h["status"] for h in resp["heads"]}
    assert byaddr["0xAAAA"] == "applied"
    assert byaddr["0xCCCC"] == "unreachable"
    assert resp["status"] == "ok"
    assert resp["job_status"] == "partial"
    # No /set was addressed to the absent member.
    assert all("0xCCCC" not in t and "cccc" not in t.lower() for t, _p in _sets(tx))


def test_set_setpoint_bad_json_errors() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    tx, coord = _armed(store, "r1")
    coord.start()
    tx.inject("danfoss-rooms/bridge/request/room/set-setpoint", "{not json")
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-setpoint"))
    assert resp["status"] == "error"
    assert _sets(tx) == []
