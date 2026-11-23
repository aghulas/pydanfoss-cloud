"""Room setpoint fan-out write job (``docs/z2m-cloud-parity.md`` §3, §5.2 — Stage 2).

A Zigbee2MQTT room has **no group write** (§3): a broadcast is not buffered for the sleeping TRVs,
and the thermostat cluster is not bindable. A room setpoint change is therefore a **fan-out of
per-head writes**, each verified on that head's own state topic. This module holds that logic as a
pure, synchronous sequencer so the sequence, retry, confirmation and value validation are tested
without a broker or threads:

- :func:`validate_setpoint` — **value validation first** (0.5 °C step, within every member's
  reported bounds). On any invalid value the job is ``failed`` and **nothing is sent**.
- :func:`run_setpoint_job` — one ``/set`` per member, **in sequence**, each confirmed by an injected
  ``confirm_fn`` within a timeout, retried a bounded number of times, reported per head
  (``applied`` / ``mismatch`` / ``unreachable``), exactly as the Companion's ``WriteJob`` does.

The ``publish_fn`` and ``confirm_fn`` are injected: the coordinator wires them to its transport and
its state-confirmation condition (:mod:`pydanfoss_cloud.room_coordinator`); tests pass fakes.

**Property choice** (§3): a *user* change writes ``occupied_heating_setpoint`` (the "aggressive"
change, pushed with a strong valve response); a change the *coordinator* makes on its own (holiday
edge, reconciliation) writes ``occupied_heating_setpoint_scheduled`` (no aggressive response).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .const import SETPOINT_MAX, SETPOINT_MIN, SETPOINT_STEP
from .decode import DanfossDevice

# Per-head outcome vocabulary — the same words the Companion's WriteJob uses (writejob.py).
HEAD_APPLIED = "applied"
HEAD_MISMATCH = "mismatch"
HEAD_UNREACHABLE = "unreachable"

# Job (lot) outcome.
JOB_APPLIED = "applied"  # every head applied
JOB_PARTIAL = "partial"  # at least one applied, at least one did not
JOB_FAILED = "failed"  # a value was invalid: nothing was sent at all
JOB_UNCONFIRMED = "unconfirmed"  # sent, but no head could be confirmed

# Setpoint source -> the Zigbee2MQTT property carrying the write (§3).
SOURCE_USER = "user"
SOURCE_COORDINATOR = "coordinator"
PROPERTY_BY_SOURCE = {
    SOURCE_USER: "occupied_heating_setpoint",
    SOURCE_COORDINATOR: "occupied_heating_setpoint_scheduled",
}


class SetpointValidationError(ValueError):
    """A setpoint value that is refused before any ``/set`` is sent (bad step or out of bounds)."""


def property_for_source(source: str) -> str:
    """Return the Zigbee2MQTT setpoint property for a change ``source`` (§3).

    Unknown or missing sources are treated as a user change — the safe default (an aggressive write
    simply responds faster; it never writes a property the heads do not understand).
    """
    return PROPERTY_BY_SOURCE.get((source or "").strip().lower(), PROPERTY_BY_SOURCE[SOURCE_USER])


def _bounds(dev: DanfossDevice) -> tuple[float, float]:
    """A head's accepted setpoint range: its reported bounds, else the product's global range."""
    low = dev.min_setpoint if dev.min_setpoint is not None else SETPOINT_MIN
    high = dev.max_setpoint if dev.max_setpoint is not None else SETPOINT_MAX
    # A head that reports an inverted or degenerate pair falls back to the global range.
    if low >= high:
        return SETPOINT_MIN, SETPOINT_MAX
    return low, high


def validate_setpoint(value: Any, members: Sequence[DanfossDevice]) -> float:
    """Validate a room setpoint (°C) against every member, **before any send** (§5.2).

    Checks the 0.5 °C step and that the value sits within *every* member's accepted range (so the
    whole room takes the same value, or the job fails without touching a head). Returns the value
    rounded onto the step. Raises :class:`SetpointValidationError` with a clear reason otherwise.
    """
    try:
        celsius = float(value)
    except (TypeError, ValueError) as err:
        raise SetpointValidationError(f"non-numeric setpoint: {value!r}") from err

    stepped = round(celsius / SETPOINT_STEP) * SETPOINT_STEP
    if abs(stepped - celsius) > 1e-9:
        raise SetpointValidationError(
            f"setpoint {celsius} °C is not on the {SETPOINT_STEP} °C step"
        )

    if not members:
        raise SetpointValidationError("room has no members to write to")

    for dev in members:
        low, high = _bounds(dev)
        if not low <= stepped <= high:
            raise SetpointValidationError(
                f"setpoint {stepped} °C is outside head {dev.id!r} range ({low}-{high} °C)"
            )
    return stepped


@dataclass
class HeadWriteResult:
    """One member's result in the fan-out."""

    address: str
    friendly: str
    status: str = HEAD_APPLIED
    detail: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "address": self.address,
            "friendly": self.friendly,
            "status": self.status,
            "detail": self.detail,
            "attempts": self.attempts,
        }


@dataclass
class SetpointJob:
    """Report of a room setpoint fan-out, per head and overall (§5.2)."""

    room_id: str
    setpoint: float
    property: str
    status: str = JOB_APPLIED
    heads: list[HeadWriteResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "setpoint": self.setpoint,
            "property": self.property,
            "status": self.status,
            "heads": [h.to_dict() for h in self.heads],
        }


def _job_status(heads: Sequence[HeadWriteResult]) -> str:
    statuses = {h.status for h in heads}
    if not statuses:
        return JOB_UNCONFIRMED
    if statuses == {HEAD_APPLIED}:
        return JOB_APPLIED
    if HEAD_APPLIED in statuses:
        return JOB_PARTIAL
    return JOB_UNCONFIRMED


def recompute_status(job: SetpointJob) -> None:
    """Set ``job.status`` from its current heads (used after extra heads are appended)."""
    job.status = _job_status(job.heads)


# confirm_fn(dev, target_celsius) -> (status, detail): status is HEAD_APPLIED when the head reported
# the target (within 0.1 °C) inside the timeout, HEAD_MISMATCH when it reported a different value,
# HEAD_UNREACHABLE when it reported nothing in the window.
ConfirmFn = Callable[[DanfossDevice, float], "tuple[str, str | None]"]
# publish_fn(dev, property_name, value_celsius): send one <source>/<friendly>/set.
PublishFn = Callable[[DanfossDevice, str, float], None]


def run_setpoint_job(
    *,
    room_id: str,
    value: float,
    members: Sequence[DanfossDevice],
    source: str,
    publish_fn: PublishFn,
    confirm_fn: ConfirmFn,
    retries: int = 2,
) -> SetpointJob:
    """Fan a validated room setpoint out to its members, one head at a time, confirming each (§5.2).

    ``value`` MUST already be validated (:func:`validate_setpoint`); call that first and return a
    ``failed`` job without calling this if it raises. Each head is written then confirmed; on a
    non-applied outcome it is retried up to ``retries`` extra times, then recorded with its last
    outcome. Writes are sequential: the next head is not touched until the current one resolves.
    """
    prop = property_for_source(source)
    job = SetpointJob(room_id=room_id, setpoint=value, property=prop)

    for dev in members:
        result = HeadWriteResult(address=dev.id, friendly=dev.name)
        status: str = HEAD_UNREACHABLE
        detail: str | None = "no attempt"
        for attempt in range(retries + 1):
            result.attempts = attempt + 1
            publish_fn(dev, prop, value)
            status, detail = confirm_fn(dev, value)
            if status == HEAD_APPLIED:
                break
        result.status, result.detail = status, detail
        job.heads.append(result)

    job.status = _job_status(job.heads)
    return job
