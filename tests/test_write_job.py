"""Synthetic tests for the room setpoint fan-out core (``write_job.py``, Stage 2).

Pure and synchronous: ``publish_fn`` and ``confirm_fn`` are fakes, so the sequence, bounded retry,
confirmation outcomes, property choice and value validation are exercised with no broker and no
threads. No real site data, nothing sent to a head.
"""

from __future__ import annotations

import pytest

from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.write_job import (
    HEAD_APPLIED,
    HEAD_MISMATCH,
    HEAD_UNREACHABLE,
    JOB_APPLIED,
    JOB_PARTIAL,
    JOB_UNCONFIRMED,
    SetpointValidationError,
    property_for_source,
    run_setpoint_job,
    validate_setpoint,
)


def _head(dev_id: str, *, low: float | None = None, high: float | None = None) -> DanfossDevice:
    return DanfossDevice(
        id=dev_id, name=dev_id, category="head", online=True, min_setpoint=low, max_setpoint=high
    )


# --- value validation (before any send) --------------------------------------------------------


def test_validate_setpoint_rounds_to_step() -> None:
    assert validate_setpoint(20.0, [_head("a")]) == 20.0
    assert validate_setpoint(21.5, [_head("a")]) == 21.5


def test_validate_setpoint_rejects_off_step() -> None:
    with pytest.raises(SetpointValidationError, match="step"):
        validate_setpoint(20.3, [_head("a")])


def test_validate_setpoint_rejects_non_numeric() -> None:
    with pytest.raises(SetpointValidationError, match="non-numeric"):
        validate_setpoint("warm", [_head("a")])


def test_validate_setpoint_rejects_outside_a_members_bounds() -> None:
    members = [_head("a", low=5.0, high=28.0), _head("b", low=16.0, high=24.0)]
    # 26 °C is fine for a but above b's ceiling -> whole room refused, nothing sent.
    with pytest.raises(SetpointValidationError, match="'b'"):
        validate_setpoint(26.0, members)


def test_validate_setpoint_uses_global_range_without_reported_bounds() -> None:
    assert validate_setpoint(35.0, [_head("a")]) == 35.0
    with pytest.raises(SetpointValidationError):
        validate_setpoint(40.0, [_head("a")])


def test_validate_setpoint_refuses_empty_room() -> None:
    with pytest.raises(SetpointValidationError, match="no members"):
        validate_setpoint(20.0, [])


def test_validate_setpoint_degenerate_bounds_fall_back_to_global() -> None:
    # A head reporting min >= max is not trusted; the global range applies.
    assert validate_setpoint(22.0, [_head("a", low=25.0, high=20.0)]) == 22.0


# --- property choice by source -----------------------------------------------------------------


def test_property_for_source() -> None:
    assert property_for_source("user") == "occupied_heating_setpoint"
    assert property_for_source("coordinator") == "occupied_heating_setpoint_scheduled"
    # Unknown / empty source defaults to the user (aggressive) property.
    assert property_for_source("") == "occupied_heating_setpoint"
    assert property_for_source("whatever") == "occupied_heating_setpoint"


# --- fan-out sequencing ------------------------------------------------------------------------


def _recording_publish(log: list[tuple[str, str, float]]):
    def _publish(dev: DanfossDevice, prop: str, value: float) -> None:
        log.append((dev.id, prop, value))

    return _publish


def test_fanout_all_applied_in_order() -> None:
    members = [_head("a"), _head("b"), _head("c")]
    log: list[tuple[str, str, float]] = []

    def confirm(dev: DanfossDevice, target: float) -> tuple[str, str | None]:
        return HEAD_APPLIED, None

    job = run_setpoint_job(
        room_id="r1",
        value=20.5,
        members=members,
        source="user",
        publish_fn=_recording_publish(log),
        confirm_fn=confirm,
    )
    assert job.status == JOB_APPLIED
    assert [h.address for h in job.heads] == ["a", "b", "c"]
    assert all(h.status == HEAD_APPLIED and h.attempts == 1 for h in job.heads)
    # One send per head, in order, with the user property.
    assert log == [("a", "occupied_heating_setpoint", 20.5)] * 1 + [
        ("b", "occupied_heating_setpoint", 20.5),
        ("c", "occupied_heating_setpoint", 20.5),
    ]


def test_fanout_coordinator_source_uses_scheduled_property() -> None:
    log: list[tuple[str, str, float]] = []
    run_setpoint_job(
        room_id="r1",
        value=18.0,
        members=[_head("a")],
        source="coordinator",
        publish_fn=_recording_publish(log),
        confirm_fn=lambda d, t: (HEAD_APPLIED, None),
    )
    assert log == [("a", "occupied_heating_setpoint_scheduled", 18.0)]


def test_fanout_retries_then_mismatch() -> None:
    log: list[tuple[str, str, float]] = []
    calls = {"n": 0}

    def confirm(dev: DanfossDevice, target: float) -> tuple[str, str | None]:
        calls["n"] += 1
        return HEAD_MISMATCH, "reported 19.0 °C"

    job = run_setpoint_job(
        room_id="r1",
        value=21.0,
        members=[_head("a")],
        source="user",
        publish_fn=_recording_publish(log),
        confirm_fn=confirm,
        retries=2,
    )
    # Initial attempt + 2 retries = 3 sends and 3 confirms.
    assert calls["n"] == 3
    assert len(log) == 3
    assert job.heads[0].status == HEAD_MISMATCH
    assert job.heads[0].attempts == 3
    assert job.status == JOB_UNCONFIRMED


def test_fanout_retry_succeeds_on_second_attempt() -> None:
    seq = iter([HEAD_UNREACHABLE, HEAD_APPLIED])

    def confirm(dev: DanfossDevice, target: float) -> tuple[str, str | None]:
        return next(seq), None

    job = run_setpoint_job(
        room_id="r1",
        value=21.0,
        members=[_head("a")],
        source="user",
        publish_fn=lambda d, p, v: None,
        confirm_fn=confirm,
        retries=2,
    )
    assert job.heads[0].status == HEAD_APPLIED
    assert job.heads[0].attempts == 2
    assert job.status == JOB_APPLIED


def test_fanout_partial_when_one_head_fails() -> None:
    def confirm(dev: DanfossDevice, target: float) -> tuple[str, str | None]:
        return (HEAD_APPLIED, None) if dev.id == "a" else (HEAD_UNREACHABLE, "silent")

    job = run_setpoint_job(
        room_id="r1",
        value=20.0,
        members=[_head("a"), _head("b")],
        source="user",
        publish_fn=lambda d, p, v: None,
        confirm_fn=confirm,
        retries=0,
    )
    assert {h.address: h.status for h in job.heads} == {
        "a": HEAD_APPLIED,
        "b": HEAD_UNREACHABLE,
    }
    assert job.status == JOB_PARTIAL


def test_fanout_no_retries_single_attempt() -> None:
    log: list[tuple[str, str, float]] = []
    run_setpoint_job(
        room_id="r1",
        value=20.0,
        members=[_head("a")],
        source="user",
        publish_fn=_recording_publish(log),
        confirm_fn=lambda d, t: (HEAD_UNREACHABLE, "x"),
        retries=0,
    )
    assert len(log) == 1
