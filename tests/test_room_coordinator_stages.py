"""Synthetic tests for RoomCoordinator Stages 3–7 (no network, no real site data).

Covers:
- Stage 3: Event-driven room sensor feed, both ``radiator_covered`` modes, 30-min silence -> -8000,
  clean stop does not send -8000 for sensor, gated by ``feed_external_sensor`` + ``armed_rooms``.
- Stage 4: Weekly schedule fan-out (all 7 days + mode), R1 refusal (> 6 transitions including
  00:00), comfort/setback R3 rewrite, ``set-mode`` (``schedule`` / ``manual`` / ``pause``),
  ``mode_change_keeps_schedule``, ``get_schedule`` / ``restore_schedule`` + ``schedule_<day>``.
- Stage 5: Room-wide settings fan-out (skipping matching heads), hourly reconciliation capped at
  3 consecutive attempts then flagged ``reconcile_blocked``, reset when matching or updated.
- Stage 6: Load balancing across multi-head rooms with ``load_balancing_enable`` on all members,
  coalescing sensor + load into a single ``/set`` (5-min radio floor), clean stop -> -8000.
- Stage 7: Holidays (start/end edges, missed edge reconciled at ``start()``, ``bridge/holidays``)
  and cross-room open-window propagation (anti-loop on ``external_open``, clear on close, max
  duration expiry).
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from pydanfoss_cloud.bridge_daemon import _topic_matches
from pydanfoss_cloud.room_coordinator import (
    RoomCoordinator,
    build_arg_parser,
    load_coordinator_config,
)
from pydanfoss_cloud.room_store import RoomStore


class FakeTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self._subs: list[tuple[str, Any]] = []

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: Any) -> None:
        self._subs.append((topic, callback))

    def inject(self, topic: str, payload: Any) -> None:
        text = payload if isinstance(payload, str) else json.dumps(payload)
        for sub, cb in self._subs:
            if _topic_matches(sub, topic):
                cb(topic, text)

    def last(self, topic: str) -> str | None:
        for t, p, _r in reversed(self.published):
            if t == topic:
                return p
        return None

    def sets(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (t, json.loads(p))
            for t, p, _r in self.published
            if t.startswith("zigbee2mqtt/") and t.endswith("/set")
        ]


class FakeClock:
    def __init__(self, mono: float = 1000.0, wall: datetime | None = None) -> None:
        self.mono = mono
        self.wall = wall or datetime(2026, 12, 1, 10, 0, tzinfo=timezone(timedelta(hours=1)))

    def monotonic(self) -> float:
        return self.mono

    def wall_clock(self) -> datetime:
        return self.wall


def _desc(friendly: str, ieee: str, model: str = "014G2461") -> dict[str, Any]:
    return {
        "friendly_name": friendly,
        "ieee_address": ieee,
        "definition": {"vendor": "Danfoss", "model": model},
    }


def _join(coord: RoomCoordinator) -> None:
    for thread in list(coord._jobs):
        thread.join(timeout=5.0)
        assert not thread.is_alive()


def _setup_two_head_room(
    *,
    sensor: str | None = "0xS1",
    armed: tuple[str, ...] = ("r1",),
    **coord_kw: Any,
) -> tuple[FakeTransport, RoomCoordinator, RoomStore, FakeClock]:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xA", "0xB"], sensor=sensor, room_id="r1")
    tx = FakeTransport()
    clock = FakeClock()
    coord = RoomCoordinator(
        tx,
        store,
        armed_rooms=armed,
        tick_interval_s=0.0,
        monotonic_fn=clock.monotonic,
        wall_clock_fn=clock.wall_clock,
        **coord_kw,
    )
    coord.start()
    devices = [_desc("head_a", "0xA"), _desc("head_b", "0xB")]
    if sensor:
        devices.append(_desc("sens_1", sensor, model="014G2480"))
    tx.inject("zigbee2mqtt/bridge/devices", devices)
    return tx, coord, store, clock


# --- Closed by default -------------------------------------------------------------------------


def test_stages_3_to_7_closed_by_default() -> None:
    """Even with a room in armed_rooms, autonomous Stages 3/5/6/7 stay closed unless opted in."""
    tx, coord, store, clock = _setup_two_head_room(armed=("r1",))
    tx.inject(
        "zigbee2mqtt/head_a",
        {
            "radiator_covered": True,
            "load_balancing_enable": True,
            "load_estimate": 400,
            "window_open": True,
        },
    )
    tx.inject(
        "zigbee2mqtt/head_b",
        {"radiator_covered": True, "load_balancing_enable": True, "load_estimate": 600},
    )
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.5})
    store.update_desired("r1", {"child_lock": True})
    coord.tick(now=clock.mono + 4000)
    assert tx.sets() == []

    # And with flags on but room NOT in armed_rooms, still nothing is written.
    tx2, coord2, store2, clock2 = _setup_two_head_room(
        armed=(),
        feed_external_sensor=True,
        feed_load_mean=True,
        reconcile_drift=True,
        propagate_window_open=True,
    )
    tx2.inject(
        "zigbee2mqtt/head_a",
        {"load_balancing_enable": True, "load_estimate": 400, "window_open": True},
    )
    tx2.inject("zigbee2mqtt/head_b", {"load_balancing_enable": True, "load_estimate": 600})
    tx2.inject("zigbee2mqtt/sens_1", {"temperature": 20.5})
    store2.update_desired("r1", {"child_lock": True})
    coord2.tick(now=clock2.mono + 4000)
    assert tx2.sets() == []


# --- Stage 3: room sensor feed -----------------------------------------------------------------


def test_stage3_sensor_feed_event_driven_and_radiator_covered_modes() -> None:
    tx, coord, _store, clock = _setup_two_head_room(feed_external_sensor=True)
    # head_a in room-sensor mode (radiator_covered=True), head_b in offset mode (False)
    tx.inject("zigbee2mqtt/head_a", {"radiator_covered": True})
    tx.inject("zigbee2mqtt/head_b", {"radiator_covered": False})
    tx.published.clear()

    # Sensor reports 20.4 -> immediately event-driven write to both heads (2040).
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.4})
    assert tx.sets() == [
        ("zigbee2mqtt/head_a/set", {"external_measured_room_sensor": 2040}),
        ("zigbee2mqtt/head_b/set", {"external_measured_room_sensor": 2040}),
    ]
    tx.published.clear()

    # Small delta (< 0.2) within min interval -> nothing sent.
    clock.mono += 60.0
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.5})
    assert tx.sets() == []

    # 10 min later (600 s): big delta 21.0 -> head_a (covered, 5 min floor) updates;
    # head_b (offset mode, 30 min floor) does NOT update yet!
    clock.mono += 600.0
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 21.0})
    assert tx.sets() == [("zigbee2mqtt/head_a/set", {"external_measured_room_sensor": 2100})]
    tx.published.clear()

    # Keep-alive at +1200 s (total +1860 s from t=1000): sensor stays silent -> head_a gets
    # keep-alive at 20 min; head_b (offset mode, 2 h keep-alive) got updated at 1800s only if delta.
    clock.mono += 1200.0
    coord.tick(now=clock.mono)
    assert ("zigbee2mqtt/head_a/set", {"external_measured_room_sensor": 2100}) in tx.sets()
    # head_b missed the 21.0 update earlier, now > 30 min passed -> gets 2100.
    assert ("zigbee2mqtt/head_b/set", {"external_measured_room_sensor": 2100}) in tx.sets()
    tx.published.clear()

    # Sensor silent for 30 min (1800 s since last sens_1 message at t=1660 -> t=3460):
    # head_a (covered, 5 min floor) gets -8000 at once; head_b (offset, 30 min floor) gets -8000
    # once 30 min has passed since its 2860 write (at t=4660).
    clock.mono = 1660.0 + 1800.0
    coord.tick(now=clock.mono)
    assert tx.sets() == [("zigbee2mqtt/head_a/set", {"external_measured_room_sensor": -8000})]
    tx.published.clear()

    clock.mono = 2860.0 + 1800.0
    coord.tick(now=clock.mono)
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"external_measured_room_sensor": -8000})]
    tx.published.clear()

    # Next tick while still silent does not repeat -8000.
    clock.mono += 2000.0
    coord.tick(now=clock.mono)
    assert tx.sets() == []

    # Clean stop does NOT emit -8000 for sensor feed.
    coord.stop()
    assert tx.sets() == []


# --- Stage 4: schedule, comfort/setback, modes -------------------------------------------------


def test_stage4_schedule_r1_refusal_before_any_send() -> None:
    tx, coord, store, _clock = _setup_two_head_room()
    tx.published.clear()
    # 4 slots = 8 transitions > 6 -> refused before storing or sending!
    bad_days = {
        "mon": [
            {"start": "06:00", "end": "07:00"},
            {"start": "08:00", "end": "09:00"},
            {"start": "12:00", "end": "13:00"},
            {"start": "18:00", "end": "20:00"},
        ]
    }
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-schedule",
        {"room_id": "r1", "days": bad_days, "transaction": "s1"},
    )
    _join(coord)
    resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-schedule") or "{}")
    assert resp["status"] == "error"
    assert resp["job_status"] == "failed"
    assert resp["code"] == "too_many_transitions"
    assert resp["transaction"] == "s1"
    assert tx.sets() == []
    assert store.get("r1").schedule is None


def test_stage4_schedule_fanout_comfort_setback_r3_and_modes() -> None:
    tx, coord, store, _clock = _setup_two_head_room(setpoint_timeout_s=2.0, setpoint_retries=0)
    tx.inject(
        "zigbee2mqtt/head_a",
        {"programming_operation_mode": "setpoint", "occupied_heating_setpoint": 18.0},
    )
    tx.inject(
        "zigbee2mqtt/head_b",
        {"programming_operation_mode": "setpoint", "occupied_heating_setpoint": 18.0},
    )
    tx.published.clear()

    def _auto_confirm() -> None:
        # Simulate both heads confirming schedule_active=True after receiving the schedule.
        for _ in range(50):
            sets = tx.sets()
            for topic, body in sets:
                if body.get("programming_operation_mode") in ("schedule", "schedule_with_preheat"):
                    friendly = topic.split("/")[1]
                    tx.inject(
                        f"zigbee2mqtt/{friendly}",
                        {
                            "programming_operation_mode": body["programming_operation_mode"],
                            "schedule_monday": "07:00/21.5 22:00/17.0",
                        },
                    )
            if len([t for t, b in sets if "programming_operation_mode" in b]) >= 2:
                return
            time.sleep(0.01)

    t = threading.Thread(target=_auto_confirm, daemon=True)
    t.start()
    tx.inject(
        "danfoss-rooms/bridge/request/set_schedule",
        {
            "target": {"type": "room", "id": "r1"},
            "days": {"mon": [{"start": "07:00", "end": "22:00"}]},
            "comfort": 21.5,
            "setback": 17.0,
            "transaction": "sched-1",
        },
    )
    _join(coord)
    t.join(timeout=2.0)

    resp = json.loads(tx.last("danfoss-rooms/bridge/response/set_schedule") or "{}")
    assert resp["status"] == "ok"
    assert resp["job_status"] == "applied"
    assert resp["data"]["written"] is True
    assert resp["transaction"] == "sched-1"
    # 7 days + 1 mode write per head = 16 /set messages across 2 heads
    assert len(tx.sets()) == 16

    # get-schedule returns stored schedule + reported schedule_monday read-back!
    tx.inject("danfoss-rooms/bridge/request/get_schedule", {"target": {"type": "room", "id": "r1"}})
    get_resp = json.loads(tx.last("danfoss-rooms/bridge/response/get_schedule") or "{}")
    assert get_resp["status"] == "ok"
    assert get_resp["data"]["comfort"] == 21.5
    assert get_resp["data"]["days"]["mon"] == [{"start": "07:00", "end": "22:00"}]
    assert get_resp["reported_schedules"]["0xA"]["mon"] == "07:00/21.5 22:00/17.0"

    # Changing comfort/setback rewrites the schedule on every member (R3).
    tx.published.clear()
    t2 = threading.Thread(target=_auto_confirm, daemon=True)
    t2.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-comfort-setback",
        {"room_id": "r1", "comfort": 22.0},
    )
    _join(coord)
    t2.join(timeout=2.0)
    assert store.get("r1").schedule["comfort"] == 22.0
    assert any(
        b.get("weekly_schedule", {}).get("dayofweek") == ["monday"]
        and "mode" not in b["weekly_schedule"]
        and "num_of_transitions" not in b["weekly_schedule"]
        and b["weekly_schedule"]["transitions"][1]["heat_setpoint"] == 22.0
        for _t, b in tx.sets()
    )

    # Switch to pause mode (writes programming_operation_mode="setpoint" + held setpoint).
    tx.published.clear()

    def _confirm_pause() -> None:
        for _ in range(50):
            for topic, body in tx.sets():
                if body.get("programming_operation_mode") == "setpoint":
                    friendly = topic.split("/")[1]
                    tx.inject(
                        f"zigbee2mqtt/{friendly}",
                        {
                            "programming_operation_mode": "setpoint",
                            "occupied_heating_setpoint": body["occupied_heating_setpoint"],
                        },
                    )
            if len(tx.sets()) >= 2:
                return
            time.sleep(0.01)

    t3 = threading.Thread(target=_confirm_pause, daemon=True)
    t3.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-mode",
        {"room_id": "r1", "mode": "pause", "setpoint": 14.0},
    )
    _join(coord)
    t3.join(timeout=2.0)
    mode_resp = json.loads(tx.last("danfoss-rooms/bridge/response/room/set-mode") or "{}")
    assert mode_resp["job_status"] == "applied"
    assert store.get("r1").desired["mode"] == "pause"
    assert store.get("r1").desired["setpoint"] == 14.0

    # Restore schedule with mode_change_keeps_schedule=False -> rewrites 7 days + mode!
    tx.published.clear()
    t4 = threading.Thread(target=_auto_confirm, daemon=True)
    t4.start()
    tx.inject(
        "danfoss-rooms/bridge/request/restore_schedule", {"target": {"type": "room", "id": "r1"}}
    )
    _join(coord)
    t4.join(timeout=2.0)
    rest_resp = json.loads(tx.last("danfoss-rooms/bridge/response/restore_schedule") or "{}")
    assert rest_resp["job_status"] == "applied"
    assert rest_resp["data"]["restored"] is True
    assert len(tx.sets()) == 16  # 7 days + mode per head
    assert "setpoint" not in store.get("r1").desired


def test_stage4_mode_change_keeps_schedule_sends_mode_only() -> None:
    tx, coord, store, _clock = _setup_two_head_room(
        mode_change_keeps_schedule=True, setpoint_timeout_s=2.0, setpoint_retries=0
    )
    store.set_schedule(
        "r1",
        {"comfort": 21.0, "setback": 17.0, "days": {"mon": [{"start": "07:00", "end": "22:00"}]}},
    )
    store.update_desired("r1", {"mode": "pause", "setpoint": 15.0})
    tx.published.clear()

    def _confirm() -> None:
        for _ in range(50):
            for topic, body in tx.sets():
                if body.get("programming_operation_mode") == "schedule":
                    friendly = topic.split("/")[1]
                    tx.inject(f"zigbee2mqtt/{friendly}", {"programming_operation_mode": "schedule"})
            if len(tx.sets()) >= 2:
                return
            time.sleep(0.01)

    t = threading.Thread(target=_confirm, daemon=True)
    t.start()
    tx.inject("danfoss-rooms/bridge/request/room/set-mode", {"room_id": "r1", "mode": "schedule"})
    _join(coord)
    t.join(timeout=2.0)
    # Only 1 mode write per head (no 7-day rewrite).
    assert len(tx.sets()) == 2
    assert all(b == {"programming_operation_mode": "schedule"} for _t, b in tx.sets())


# --- Stage 5: room-wide settings & reconciliation ----------------------------------------------


def test_stage5_settings_skips_matching_head_and_reconciles_three_times() -> None:
    tx, coord, store, clock = _setup_two_head_room(
        reconcile_drift=True, setpoint_timeout_s=2.0, setpoint_retries=0
    )
    # head_a already has child_lock=True; head_b has child_lock=False
    tx.inject("zigbee2mqtt/head_a", {"keypad_lockout": "lock"})
    tx.inject("zigbee2mqtt/head_b", {"keypad_lockout": "unlock"})
    tx.published.clear()

    def _confirm_both() -> None:
        confirmed: set[str] = set()
        for _ in range(80):
            for topic, body in tx.sets():
                friendly = topic.split("/")[1]
                if friendly not in confirmed and "max_heat_setpoint_limit" in body:
                    tx.inject(f"zigbee2mqtt/{friendly}", body)
                    confirmed.add(friendly)
            if len(confirmed) >= 2:
                return
            time.sleep(0.01)

    t = threading.Thread(target=_confirm_both, daemon=True)
    t.start()
    tx.inject(
        "danfoss-rooms/bridge/request/room/set-settings",
        {"room_id": "r1", "child_lock": True, "max_setpoint": 26.0},
    )
    _join(coord)
    t.join(timeout=2.0)
    # head_a only needed max_setpoint; head_b needed both!
    sets_by_topic = dict(tx.sets())
    assert sets_by_topic["zigbee2mqtt/head_a/set"] == {"max_heat_setpoint_limit": 26.0}
    assert sets_by_topic["zigbee2mqtt/head_b/set"] == {
        "keypad_lockout": "lock",
        "max_heat_setpoint_limit": 26.0,
    }

    # Now head_b is turned by hand to max_heat_setpoint_limit=22.0 and setpoint=20.0 -> drifts!
    store.update_desired("r1", {"setpoint": 21.0})
    tx.inject(
        "zigbee2mqtt/head_a", {"occupied_heating_setpoint": 21.0, "max_heat_setpoint_limit": 26.0}
    )
    tx.inject(
        "zigbee2mqtt/head_b", {"occupied_heating_setpoint": 19.0, "max_heat_setpoint_limit": 22.0}
    )
    tx.published.clear()

    # Attempt 1 at t=1000
    res1 = coord.tick(now=clock.mono)
    assert res1["reconcile_writes"] == 1
    assert tx.sets() == [
        (
            "zigbee2mqtt/head_b/set",
            {"occupied_heating_setpoint_scheduled": 21.0, "max_heat_setpoint_limit": 26.0},
        )
    ]
    tx.published.clear()

    # Within 1h cooldown -> 0 writes
    assert coord.tick(now=clock.mono + 1800)["reconcile_writes"] == 0

    # Attempt 2 at +3600s
    assert coord.tick(now=clock.mono + 3600)["reconcile_writes"] == 1
    # Attempt 3 at +7200s
    assert coord.tick(now=clock.mono + 7200)["reconcile_writes"] == 1
    tx.published.clear()

    # After 3 consecutive failures (+10800s): stops writing and flags head_b as reconcile_blocked!
    assert coord.tick(now=clock.mono + 10800)["reconcile_writes"] == 0
    assert tx.sets() == []
    room_view = json.loads(tx.last("danfoss-rooms/bridge/rooms") or "[]")[0]
    assert room_view["reconcile_blocked"] == ["0xB"]


# --- Stage 6: load balancing & coalesced telemetry ---------------------------------------------


def test_stage6_load_balancing_coalesces_with_sensor_and_clears_on_stop() -> None:
    tx, coord, _store, clock = _setup_two_head_room(feed_external_sensor=True, feed_load_mean=True)
    # head_b hasn't reported load_balancing_enable=True yet -> load_room_mean must NOT run!
    tx.inject("zigbee2mqtt/head_a", {"load_balancing_enable": True, "load_estimate": 200})
    tx.inject("zigbee2mqtt/head_b", {"load_balancing_enable": False, "load_estimate": 400})
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.0})
    assert all("load_room_mean" not in b for _t, b in tx.sets())

    # Now both heads have load_balancing_enable=True; advance past 5-min telemetry floor.
    tx.published.clear()
    clock.mono += 400.0
    tx.inject("zigbee2mqtt/head_b", {"load_balancing_enable": True, "load_estimate": 400})
    # Advance sensor by 0.5°C on tick -> sensor + load share ONE /set per head!
    clock.mono += 900.0
    tx.inject("zigbee2mqtt/sens_1", {"temperature": 20.5})
    sets = dict(tx.sets())
    assert sets["zigbee2mqtt/head_a/set"] == {
        "external_measured_room_sensor": 2050,
        "load_room_mean": 300,
    }
    assert sets["zigbee2mqtt/head_b/set"] == {
        "external_measured_room_sensor": 2050,
        "load_room_mean": 300,
    }

    # Clean stop writes load_room_mean = -8000 (§5.6)
    tx.published.clear()
    coord.stop()
    assert sorted(tx.sets(), key=lambda x: x[0]) == [
        ("zigbee2mqtt/head_a/set", {"load_room_mean": -8000}),
        ("zigbee2mqtt/head_b/set", {"load_room_mean": -8000}),
    ]


# --- Stage 7: holidays & open window across a room ---------------------------------------------


def test_stage7_holiday_edges_and_missed_edge_on_restart() -> None:
    tz = timezone(timedelta(hours=1))
    wall_before = datetime(2026, 12, 20, 8, 0, tzinfo=tz)
    wall_during = datetime(2026, 12, 21, 10, 0, tzinfo=tz)
    wall_after = datetime(2026, 12, 25, 10, 0, tzinfo=tz)

    store = RoomStore()
    store.create("Living", "zigbee2mqtt", members=["0xA"], room_id="r1")
    store.set_schedule(
        "r1",
        {"comfort": 21.0, "setback": 17.0, "days": {"mon": [{"start": "07:00", "end": "22:00"}]}},
    )
    tx = FakeTransport()
    clock = FakeClock(wall=wall_before)
    coord = RoomCoordinator(
        tx,
        store,
        armed_rooms=("r1",),
        enable_holidays=True,
        tick_interval_s=0.0,
        monotonic_fn=clock.monotonic,
        wall_clock_fn=clock.wall_clock,
    )
    coord.start()
    tx.inject("zigbee2mqtt/bridge/devices", [_desc("head_a", "0xA")])

    tx.inject(
        "danfoss-rooms/bridge/request/holiday/set",
        {
            "holiday_id": "h1",
            "name": "Winter",
            "start": "2026-12-21T08:00:00+01:00",
            "end": "2026-12-24T18:00:00+01:00",
            "temperature": 12.0,
            "room_ids": ["r1"],
        },
    )
    holidays_pub = json.loads(tx.last("danfoss-rooms/bridge/holidays") or "{}")
    assert holidays_pub["h1"]["scheduled"] is True
    assert holidays_pub["h1"]["active"] is False
    assert tx.sets() == []

    # Cross start edge -> applies scheduled setpoint 12.0 + programming_operation_mode="setpoint"
    clock.wall = wall_during
    coord.tick(wall_now=wall_during)
    assert tx.sets() == [
        (
            "zigbee2mqtt/head_a/set",
            {"occupied_heating_setpoint_scheduled": 12.0, "programming_operation_mode": "setpoint"},
        )
    ]
    assert store.get_holiday("h1").active is True

    # Simulate coordinator stopped across the end edge, then restarted after end edge:
    # start() reconciles missed end edge immediately!
    coord.stop()
    tx2 = FakeTransport()
    clock2 = FakeClock(wall=wall_after)
    coord2 = RoomCoordinator(
        tx2,
        store,
        armed_rooms=("r1",),
        enable_holidays=True,
        tick_interval_s=0.0,
        monotonic_fn=clock2.monotonic,
        wall_clock_fn=clock2.wall_clock,
    )
    # On restart, bridge/devices triggers missed end-edge reconciliation to restore the schedule.
    coord2.start()
    tx2.inject("zigbee2mqtt/bridge/devices", [_desc("head_a", "0xA")])
    assert store.get_holiday("h1").active is False
    assert store.get("r1").desired["mode"] == "schedule"
    assert any(b == {"programming_operation_mode": "schedule"} for _t, b in tx2.sets())


def test_stage7_open_window_propagation_anti_loop_and_expiry() -> None:
    tx, coord, _store, clock = _setup_two_head_room(
        propagate_window_open=True, window_open_max_duration_s=1800.0
    )
    tx.published.clear()

    # head_a detects open window -> head_b receives window_open_external=True
    tx.inject("zigbee2mqtt/head_a", {"window_open_internal": "open"})
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"window_open_external": True})]
    tx.published.clear()

    # head_b echoes external_open -> must NOT bounce back to head_a
    tx.inject("zigbee2mqtt/head_b", {"window_open_internal": "external_open"})
    assert tx.sets() == []

    # head_a closes -> clears window_open_external=False on head_b even before head_b updates
    tx.inject("zigbee2mqtt/head_a", {"window_open_internal": "closed"})
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"window_open_external": False})]
    tx.published.clear()

    # Re-open head_a, then exceed max duration (1800 s) -> cleared and suppressed until closed!
    tx.inject("zigbee2mqtt/head_b", {"window_open_internal": "closed"})
    tx.inject("zigbee2mqtt/head_a", {"window_open_internal": "open"})
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"window_open_external": True})]
    tx.published.clear()

    clock.mono += 1800.0
    coord.tick(now=clock.mono)
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"window_open_external": False})]


def test_config_flags_all_closed_by_default(tmp_path) -> None:
    args = build_arg_parser().parse_args([])
    cfg = load_coordinator_config(args)
    assert cfg.armed_rooms == ()
    assert cfg.feed_external_sensor is False
    assert cfg.mode_change_keeps_schedule is False
    assert cfg.reconcile_drift is False
    assert cfg.feed_load_mean is False
    assert cfg.enable_holidays is False
    assert cfg.propagate_window_open is False

    opts_file = tmp_path / "options.json"
    opts_file.write_text(
        json.dumps(
            {
                "armed_rooms": ["r1"],
                "feed_external_sensor": True,
                "feed_load_mean": True,
                "reconcile_drift": True,
                "enable_holidays": True,
                "propagate_window_open": True,
                "mode_change_keeps_schedule": True,
            }
        ),
        encoding="utf-8",
    )
    cfg2 = load_coordinator_config(args, options_path=opts_file)
    assert cfg2.armed_rooms == ("r1",)
    assert cfg2.feed_external_sensor is True
    assert cfg2.feed_load_mean is True
    assert cfg2.reconcile_drift is True
    assert cfg2.enable_holidays is True
    assert cfg2.propagate_window_open is True
    assert cfg2.mode_change_keeps_schedule is True


def test_stage5_reconciles_scheduled_setpoint_after_transition_grace() -> None:
    tx, coord, store, clock = _setup_two_head_room(reconcile_drift=True)
    store.set_schedule(
        "r1",
        {
            "comfort": 21.0,
            "setback": 14.0,
            "days": {"fri": [{"start": "08:30", "end": "18:00"}]},
        },
    )
    store.update_desired("r1", {"mode": "schedule", "comfort": 21.0, "setback": 14.0})
    tx.inject(
        "zigbee2mqtt/head_a",
        {"programming_operation_mode": "schedule", "occupied_heating_setpoint": 21.0},
    )
    tx.inject(
        "zigbee2mqtt/head_b",
        {"programming_operation_mode": "schedule", "occupied_heating_setpoint": 19.0},
    )
    tx.published.clear()

    # Within 3-min transition grace (08:31:00) -> no write yet, view still shows drift
    grace_wall = datetime(2026, 11, 27, 8, 31, 0, tzinfo=UTC)
    clock.wall = grace_wall
    res_grace = coord.tick(now=clock.mono, wall_now=grace_wall)
    assert res_grace["reconcile_writes"] == 0
    assert tx.sets() == []

    # Past 3-min grace (09:30:00) -> reconciles head_b with occupied_heating_setpoint_scheduled=21.0
    late_wall = datetime(2026, 11, 27, 9, 30, 0, tzinfo=UTC)
    clock.wall = late_wall
    res_late = coord.tick(now=clock.mono, wall_now=late_wall)
    assert res_late["reconcile_writes"] == 1
    assert tx.sets() == [("zigbee2mqtt/head_b/set", {"occupied_heating_setpoint_scheduled": 21.0})]
