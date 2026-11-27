"""Synthetic tests for the read-only room / heat-demand views (no network, no real site data)."""

from __future__ import annotations

from datetime import UTC, datetime

from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.room_store import CoordinatorRoom
from pydanfoss_cloud.room_view import (
    build_heat_demand,
    build_room_view,
    expected_scheduled_setpoint,
    within_schedule_transition_grace,
)


def _head(dev_id: str, **kw) -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=dev_id, category="head", online=True, **kw)


def _sensor(dev_id: str, **kw) -> DanfossDevice:
    return DanfossDevice(id=dev_id, name=dev_id, category="sensor", online=True, **kw)


def test_room_view_aggregates_reported_state() -> None:
    room = CoordinatorRoom(
        room_id="r1", name="Living", prefix="zigbee2mqtt", members=("0xA", "0xB")
    )
    members = [
        _head("0xA", temperature=20.0, setpoint=21.0, valve_opening=30.0),
        _head("0xB", temperature=22.0, setpoint=21.0, valve_opening=0.0),
    ]
    view = build_room_view(room, members)
    assert view["group_id"] == "r1"
    assert view["prefix"] == "zigbee2mqtt"
    assert view["temperature"] == 21.0  # mean of 20 and 22
    assert view["setpoint"] == 21.0
    assert view["heating"] is True  # one head's valve is open
    assert view["head_count"] == 2
    assert view["online"] is True
    assert view["desired"] == {}
    assert view["drift"] == []


def test_room_view_sensor_overrides_temperature_and_humidity() -> None:
    room = CoordinatorRoom(
        room_id="r1", name="Living", prefix="zigbee2mqtt", members=("0xA",), sensor="0xS"
    )
    members = [_head("0xA", temperature=19.0)]
    sensor = _sensor("0xS", temperature=20.6, humidity=48.0)
    view = build_room_view(room, members, sensor)
    assert view["temperature"] == 20.6
    assert view["humidity"] == 48.0
    assert view["sensor"] == "0xS"


def test_room_view_offline_sensor_does_not_override() -> None:
    room = CoordinatorRoom(room_id="r1", name="Living", prefix="zigbee2mqtt", members=("0xA",))
    members = [_head("0xA", temperature=19.0)]
    sensor = DanfossDevice(id="0xS", name="s", category="sensor", online=False, temperature=30.0)
    view = build_room_view(room, members, sensor)
    assert view["temperature"] == 19.0
    assert view["humidity"] is None


def test_drift_lists_members_differing_from_desired() -> None:
    room = CoordinatorRoom(
        room_id="r1",
        name="Living",
        prefix="zigbee2mqtt",
        members=("0xA", "0xB", "0xC"),
        desired={"setpoint": 21.0},
    )
    members = [
        _head("0xA", setpoint=21.0),  # matches
        _head("0xB", setpoint=19.0),  # drifts
        _head("0xC", setpoint=None),  # not reported -> not drift
    ]
    view = build_room_view(room, members)
    assert view["desired"] == {"setpoint": 21.0}
    assert view["drift"] == ["0xB"]


def test_drift_empty_without_desired() -> None:
    room = CoordinatorRoom(room_id="r1", name="Living", prefix="zigbee2mqtt", members=("0xA",))
    assert build_room_view(room, [_head("0xA", setpoint=19.0)])["drift"] == []


def test_heat_demand_aggregation() -> None:
    members = {
        "r1": [_head("0xA", valve_opening=40.0, heat_request=True)],
        "r2": [_head("0xB", valve_opening=0.0, heat_request=False)],
        "r3": [
            DanfossDevice(id="0xC", name="c", category="head", online=False, valve_opening=99.0)
        ],
    }
    hd = build_heat_demand(members, {"r1": "Living", "r2": "Kitchen"}, now=1000)
    assert hd["calling"] is True
    assert hd["max_valve"] == 40.0  # offline head excluded
    assert hd["ts"] == 1000
    assert hd["valid_for_s"] > 0
    calling_ids = {r["group_id"] for r in hd["rooms_calling"]}
    assert calling_ids == {"r1"}
    assert hd["rooms_calling"][0]["name"] == "Living"


def test_heat_demand_empty() -> None:
    hd = build_heat_demand({}, now=5)
    assert hd["calling"] is False
    assert hd["rooms_calling"] == []
    assert hd["max_valve"] == 0.0


def test_drift_mode_and_preheat_on_z2m_heads() -> None:
    room = CoordinatorRoom(
        room_id="r1",
        name="Living",
        prefix="zigbee2mqtt",
        members=("0xA", "0xB"),
        desired={"mode": "schedule", "preheat": True},
    )
    members = [
        _head("0xA", schedule_active=True, preheat=True),
        _head("0xB", schedule_active=False, preheat=False),
    ]
    view = build_room_view(room, members, reconcile_blocked=["0xB"])
    assert view["drift"] == ["0xB"]
    assert view["reconcile_blocked"] == ["0xB"]

    # In manual/pause mode, preheat=False reported by head must not trigger preheat drift.
    paused_room = CoordinatorRoom(
        room_id="r1",
        name="Living",
        prefix="zigbee2mqtt",
        members=("0xA",),
        desired={"mode": "pause", "preheat": True},
    )
    assert (
        build_room_view(paused_room, [_head("0xA", schedule_active=False, preheat=False)])["drift"]
        == []
    )


def test_drift_detects_scheduled_setpoint_mismatch_against_schedule() -> None:
    room = CoordinatorRoom(
        room_id="r1",
        name="Living",
        prefix="zigbee2mqtt",
        members=("0xA", "0xB"),
        schedule={
            "comfort": 21.0,
            "setback": 14.0,
            "days": {"fri": [{"start": "08:30", "end": "18:00"}], "sat": []},
        },
        desired={"mode": "schedule", "comfort": 21.0, "setback": 14.0, "preheat": True},
    )
    fri_comfort = datetime(2026, 11, 27, 9, 30, tzinfo=UTC)
    fri_setback = datetime(2026, 11, 27, 19, 0, tzinfo=UTC)
    fri_grace = datetime(2026, 11, 27, 8, 31, 30, tzinfo=UTC)

    assert expected_scheduled_setpoint(room, now=fri_comfort) == 21.0
    assert expected_scheduled_setpoint(room, now=fri_setback) == 14.0
    assert within_schedule_transition_grace(room, now=fri_grace) is True
    assert within_schedule_transition_grace(room, now=fri_comfort) is False

    view = build_room_view(
        room,
        [
            _head("0xA", schedule_active=True, preheat=True, setpoint=21.0),
            _head("0xB", schedule_active=True, preheat=True, setpoint=19.0),
        ],
        now=fri_comfort,
    )
    assert view["drift"] == ["0xB"]
