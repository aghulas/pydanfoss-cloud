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

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .const import SETPOINT_MAX, SETPOINT_MIN, SETPOINT_STEP
from .decode import DanfossDevice
from .schedule import DAYS, MAX_TRANSITIONS_PER_DAY, validate_days

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

# Generic per-head action callbacks (used for schedules, modes, room settings).
HeadWriteFn = Callable[[DanfossDevice], None]
HeadConfirmFn = Callable[[DanfossDevice], "tuple[str, str | None]"]


def run_head_write_loop(
    members: Sequence[DanfossDevice],
    *,
    write_fn: HeadWriteFn,
    confirm_fn: HeadConfirmFn,
    retries: int = 2,
) -> list[HeadWriteResult]:
    """Write and confirm each head sequentially with bounded retries, returning per-head results."""
    results: list[HeadWriteResult] = []
    for dev in members:
        result = HeadWriteResult(address=dev.id, friendly=dev.name)
        status: str = HEAD_UNREACHABLE
        detail: str | None = "no attempt"
        for attempt in range(retries + 1):
            result.attempts = attempt + 1
            write_fn(dev)
            status, detail = confirm_fn(dev)
            if status == HEAD_APPLIED:
                break
        result.status, result.detail = status, detail
        results.append(result)
    return results


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
    job.heads = run_head_write_loop(
        members,
        write_fn=lambda dev: publish_fn(dev, prop, value),
        confirm_fn=lambda dev: confirm_fn(dev, value),
        retries=retries,
    )
    job.status = _job_status(job.heads)
    return job


# --- Stage 4: weekly schedule validation & Z2M payload builder ---------------------------------

_DAY_ALIASES: dict[str, str] = {
    "sun": "sun", "sunday": "sun",
    "mon": "mon", "monday": "mon",
    "tue": "tue", "tues": "tue", "tuesday": "tue",
    "wed": "wed", "wednesday": "wed",
    "thu": "thu", "thur": "thu", "thurs": "thu", "thursday": "thu",
    "fri": "fri", "friday": "fri",
    "sat": "sat", "saturday": "sat",
}  # fmt: skip

_Z2M_DAY_BITS: dict[str, int] = {
    "sun": 0x01,
    "mon": 0x02,
    "tue": 0x04,
    "wed": 0x08,
    "thu": 0x10,
    "fri": 0x20,
    "sat": 0x40,
}

# Zigbee2MQTT converter day names for optional ``schedule_<day>`` read-back verification.
Z2M_SCHEDULE_DAY_NAMES: dict[str, str] = {
    "mon": "monday",
    "tue": "tuesday",
    "wed": "wednesday",
    "thu": "thursday",
    "fri": "friday",
    "sat": "saturday",
    "sun": "sunday",
}


class ScheduleWriteValidationError(ValueError):
    """A room schedule request that is refused before anything is stored or sent (R1)."""

    def __init__(self, message: str, errors: list[dict[str, str]] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


def normalize_day_key(name: str) -> str | None:
    """Canonical 3-letter day key (``mon``..``sun``) for ``name``, or ``None``."""
    return _DAY_ALIASES.get((name or "").strip().lower())


def _hhmm_to_minutes(hhmm: str) -> int:
    hh, mm = (int(x) for x in hhmm.strip().split(":"))
    return hh * 60 + mm


def build_z2m_day_transitions(
    windows: Sequence[Mapping[str, str]], comfort_c: float, setback_c: float
) -> list[dict[str, float | int]]:
    """Convert comfort windows ``[{"start": "07:00", "end": "17:00"}]`` into ZCL/Z2M transitions."""
    transitions: list[dict[str, float | int]] = [
        {"transition_time": 0, "heat_setpoint": float(setback_c)}
    ]
    for w in sorted(windows, key=lambda x: _hhmm_to_minutes(x["start"])):
        start_m = _hhmm_to_minutes(w["start"])
        end_m = _hhmm_to_minutes(w["end"])
        if start_m == 0:
            transitions[0]["heat_setpoint"] = float(comfort_c)
        else:
            transitions.append({"transition_time": start_m, "heat_setpoint": float(comfort_c)})
        if end_m < 1440:
            transitions.append({"transition_time": end_m, "heat_setpoint": float(setback_c)})
    return transitions


def validate_step_setpoint(value: Any, *, label: str = "setpoint") -> float:
    """Validate a setpoint on the 0.5 °C step within ``SETPOINT_MIN..SETPOINT_MAX``."""
    try:
        celsius = float(value)
    except (TypeError, ValueError) as err:
        raise SetpointValidationError(f"non-numeric {label}: {value!r}") from err
    if isinstance(value, bool):
        raise SetpointValidationError(f"non-numeric {label}: {value!r}")
    stepped = round(celsius / SETPOINT_STEP) * SETPOINT_STEP
    if abs(stepped - celsius) > 1e-9:
        raise SetpointValidationError(f"{label} {celsius} °C is not on the {SETPOINT_STEP} °C step")
    if not (SETPOINT_MIN <= stepped <= SETPOINT_MAX):
        raise SetpointValidationError(
            f"{label} {stepped} °C is outside {SETPOINT_MIN}-{SETPOINT_MAX} °C"
        )
    return stepped


def build_z2m_schedule_payloads(
    raw_days: Mapping[str, Any],
    comfort_c: float,
    setback_c: float,
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, Any]]]:
    """Validate all days (R1) and build 7 ``weekly_schedule`` ``/set`` payloads.

    Refuses the entire request before anything is stored or sent when any day is unknown, malformed,
    or needs more than ``MAX_TRANSITIONS_PER_DAY`` (6) ZCL transitions (including the 00:00 entry).
    Always fills all 7 canonical days (omitted days become empty comfort slots -> baseline setback).
    """
    if not isinstance(raw_days, Mapping):
        raise ScheduleWriteValidationError("days must be a mapping")

    canonical: dict[str, list[dict[str, str]]] = {d: [] for d in DAYS}
    for raw_day, raw_windows in raw_days.items():
        day = normalize_day_key(str(raw_day))
        if day is None:
            raise ScheduleWriteValidationError(f"invalid day name: {raw_day!r}")
        if not isinstance(raw_windows, list):
            raise ScheduleWriteValidationError(f"day {raw_day!r} windows must be a list")
        cleaned: list[dict[str, str]] = []
        for win in raw_windows:
            if not isinstance(win, Mapping):
                raise ScheduleWriteValidationError(f"day {raw_day!r} slot must be an object")
            cleaned.append({"start": str(win.get("start") or ""), "end": str(win.get("end") or "")})
        canonical[day] = cleaned

    errors = validate_days(canonical)
    if errors:
        raise ScheduleWriteValidationError(errors[0]["message"], errors)

    payloads: list[dict[str, Any]] = []
    for day in DAYS:
        transitions = build_z2m_day_transitions(canonical[day], comfort_c, setback_c)
        if len(transitions) > MAX_TRANSITIONS_PER_DAY:
            raise ScheduleWriteValidationError(
                f"too many transitions on {day}: {len(transitions)} > {MAX_TRANSITIONS_PER_DAY} "
                "(R1); nothing was sent",
                [
                    {
                        "code": "too_many_transitions",
                        "day": day,
                        "message": (
                            f"too many transitions on {day}: {len(transitions)} > "
                            f"{MAX_TRANSITIONS_PER_DAY}"
                        ),
                    }
                ],
            )
        payloads.append(
            {
                "weekly_schedule": {
                    "dayofweek": _Z2M_DAY_BITS[day],
                    "num_of_transitions": len(transitions),
                    "transitions": transitions,
                }
            }
        )
    return canonical, payloads


# --- Stage 5: room-wide settings validation ----------------------------------------------------

ROOM_SETTING_KEYS = frozenset(
    {"child_lock", "window_detection", "min_setpoint", "max_setpoint", "preheat"}
)


class SettingsValidationError(ValueError):
    """A room-wide setting request refused before any ``/set`` is sent."""


def validate_room_settings(
    raw_settings: Mapping[str, Any],
    members: Sequence[DanfossDevice],
    *,
    current_desired: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate room-wide settings (child_lock, window_detection, min/max_setpoint, preheat)."""
    if not isinstance(raw_settings, Mapping) or not raw_settings:
        raise SettingsValidationError("settings must be a non-empty object")
    unknown = sorted(set(raw_settings) - ROOM_SETTING_KEYS)
    if unknown:
        raise SettingsValidationError(f"unsupported room setting(s): {', '.join(unknown)}")

    cleaned: dict[str, Any] = {}
    for key in ("child_lock", "window_detection", "preheat"):
        if key in raw_settings:
            val = raw_settings[key]
            if not isinstance(val, bool):
                raise SettingsValidationError(f"{key} must be a boolean")
            cleaned[key] = val

    for key in ("min_setpoint", "max_setpoint"):
        if key in raw_settings:
            try:
                cleaned[key] = validate_step_setpoint(raw_settings[key], label=key)
            except SetpointValidationError as err:
                raise SettingsValidationError(str(err)) from err

    # Enforce min_setpoint <= max_setpoint against both the request and existing bounds.
    eff_min = cleaned.get("min_setpoint")
    eff_max = cleaned.get("max_setpoint")
    if (
        eff_min is None
        and current_desired
        and isinstance(current_desired.get("min_setpoint"), (int, float))
    ):
        eff_min = float(current_desired["min_setpoint"])
    if (
        eff_max is None
        and current_desired
        and isinstance(current_desired.get("max_setpoint"), (int, float))
    ):
        eff_max = float(current_desired["max_setpoint"])
    if eff_min is not None and eff_max is not None and eff_min > eff_max:
        raise SettingsValidationError(
            f"min_setpoint ({eff_min} °C) cannot exceed max_setpoint ({eff_max} °C)"
        )

    if "min_setpoint" in cleaned and "max_setpoint" not in cleaned:
        for dev in members:
            if dev.max_setpoint is not None and cleaned["min_setpoint"] > dev.max_setpoint:
                raise SettingsValidationError(
                    f"min_setpoint ({cleaned['min_setpoint']} °C) exceeds head {dev.id!r} "
                    f"max_setpoint ({dev.max_setpoint} °C)"
                )
    if "max_setpoint" in cleaned and "min_setpoint" not in cleaned:
        for dev in members:
            if dev.min_setpoint is not None and cleaned["max_setpoint"] < dev.min_setpoint:
                raise SettingsValidationError(
                    f"max_setpoint ({cleaned['max_setpoint']} °C) is below head {dev.id!r} "
                    f"min_setpoint ({dev.min_setpoint} °C)"
                )
    return cleaned


# --- Stage 7: holiday validation ---------------------------------------------------------------


class HolidayValidationError(ValueError):
    """A holiday request refused before storing or touching any head."""


def parse_iso_datetime(value: str, *, default_tz: Any = None) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError) as err:
        raise HolidayValidationError(f"invalid ISO datetime: {value!r}") from err
    if dt.tzinfo is None and default_tz is not None:
        dt = dt.replace(tzinfo=default_tz)
    return dt


def validate_holiday(
    *,
    start: Any,
    end: Any,
    temperature: Any,
    room_ids: Any,
    known_room_ids: set[str],
    default_tz: Any = None,
) -> tuple[str, str, float, tuple[str, ...]]:
    """Validate a holiday definition before persisting or applying it."""
    if not isinstance(start, str) or not start.strip():
        raise HolidayValidationError("holiday start is required (ISO-8601)")
    if not isinstance(end, str) or not end.strip():
        raise HolidayValidationError("holiday end is required (ISO-8601)")
    start_s = start.strip()
    end_s = end.strip()
    start_dt = parse_iso_datetime(start_s, default_tz=default_tz)
    end_dt = parse_iso_datetime(end_s, default_tz=default_tz)
    if end_dt <= start_dt:
        raise HolidayValidationError("holiday end must be after start")

    try:
        temp = validate_step_setpoint(temperature, label="temperature")
    except SetpointValidationError as err:
        raise HolidayValidationError(str(err)) from err

    if not isinstance(room_ids, (list, tuple)) or not room_ids:
        raise HolidayValidationError("room_ids must be a non-empty list")
    cleaned_rooms: list[str] = []
    seen: set[str] = set()
    for raw in room_ids:
        rid = str(raw).strip()
        if not rid or rid in seen:
            continue
        if rid not in known_room_ids:
            raise HolidayValidationError(f"unknown room {rid!r}")
        seen.add(rid)
        cleaned_rooms.append(rid)
    if not cleaned_rooms:
        raise HolidayValidationError("room_ids must be a non-empty list")
    return start_s, end_s, temp, tuple(cleaned_rooms)
