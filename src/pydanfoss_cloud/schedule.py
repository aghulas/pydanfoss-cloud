"""Weekly schedule of Danfoss heads/rooms (path 1a): decoding and re-send helpers.

Pure functions, with no Home Assistant or network dependency (reusable layer). The schedule is read
via ``tuya.m.custom.wkf.week.timer.list`` as ``{"data": [{loops, loopData}]}``:

- ``loops``: 7-bit mask, bit 0 (leftmost) = SUNDAY (``"0100000"`` = Monday).
- ``loopData``: windows of the day, ``{startTime, endTime, mode}`` where startTime/endTime are
  MINUTES since midnight and ``mode`` is ``at_home`` (comfort) or ``leaving_home`` (setback).

Writing (``week.timer.create``) instead expects ``periods`` ``[{startTime, endTime}]`` as
``"HH:MM"`` text, and carries only the COMFORT windows (setback is the baseline outside the
windows). These helpers convert one to the other to RE-SEND a day verbatim, which is enough to make
the heads pick up the new setpoint without touching the schedule.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_WEEKDAYS_SUN_FIRST = (
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
)


def hhmm(minutes: int) -> str:
    """Minutes since midnight -> "HH:MM"."""
    m = int(minutes)
    return f"{m // 60:02d}:{m % 60:02d}"


def day_from_loop(loops: str) -> str:
    """Single-day ``loops`` mask -> day name (bit 0 = Sunday)."""
    pos = (loops or "").find("1")
    return _WEEKDAYS_SUN_FIRST[pos] if 0 <= pos < len(_WEEKDAYS_SUN_FIRST) else (loops or "")


def comfort_periods(loop_data: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    """COMFORT windows of a day -> ``periods`` ``[{startTime, endTime}]`` as "HH:MM".

    Keeps only the comfort windows (``mode`` ``at_home`` or absent): those are the only ones
    ``week.timer.create`` carries. Explicit "setback" entries are ignored (they are the baseline).
    Times are converted minutes -> "HH:MM".
    """
    out: list[dict[str, str]] = []
    for window in loop_data or []:
        if window.get("mode") not in (None, "at_home"):
            continue
        out.append(
            {
                "startTime": hhmm(window["startTime"]),
                "endTime": hhmm(window["endTime"]),
            }
        )
    return out


def first_comfort_day(data: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """First ``data`` entry with at least one comfort window (else ``None``).

    Re-sending just this one day verbatim is enough: the cloud then pushes the whole week to the
    heads with the setpoints of the moment (observed on 2026-11-16).
    """
    for entry in data or []:
        if comfort_periods(entry.get("loopData")):
            return entry
    return None


# ================================================================================================
# "Contract" layer (docs/interface-contract.md §3.2): English days mon..sun, {start, end} slots
# as "HH:MM", R1 validation and the Schedule model. Built on the helpers above.
# ================================================================================================

DAYS: tuple[str, ...] = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MAX_TRANSITIONS_PER_DAY = 6

_DAY_ORDER_SUN_FIRST: tuple[str, ...] = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
_LOOP: dict[str, str] = {
    d: "".join("1" if i == p else "0" for i in range(7)) for p, d in enumerate(_DAY_ORDER_SUN_FIRST)
}

ERR_INVALID_TIME = "invalid_time"
ERR_EMPTY_OR_REVERSED = "empty_or_reversed"
ERR_OVERLAP = "overlap"
ERR_TOO_MANY = "too_many_transitions"


def day_to_loop(day: str) -> str:
    """English day name (``mon``..``sun``) -> single-day ``loops`` mask (Sunday first)."""
    try:
        return _LOOP[day]
    except KeyError as err:
        raise ValueError(f"unknown day: {day!r}") from err


def loop_to_days(loops: str) -> list[str]:
    """``loops`` mask -> list of English days it carries (one or more bits set)."""
    return [
        _DAY_ORDER_SUN_FIRST[i]
        for i, c in enumerate(loops or "")
        if c == "1" and i < len(_DAY_ORDER_SUN_FIRST)
    ]


def _to_minutes(hhmm_str: str) -> int:
    h, _, m = hhmm_str.partition(":")
    return int(h) * 60 + int(m)


def valid_hhmm(value: object) -> bool:
    """True if ``value`` is a "HH:MM" string in 00:00-23:59."""
    if not isinstance(value, str):
        return False
    h, sep, m = value.partition(":")
    return (
        sep == ":"
        and len(h) == 2
        and len(m) == 2
        and h.isdigit()
        and m.isdigit()
        and 0 <= int(h) <= 23
        and 0 <= int(m) <= 59
    )


def decode_week_timer(data: object) -> dict[str, list[dict[str, str]]]:
    """Raw ``[{loops, loopData}]`` response -> ``{en_day: [{start, end}]}`` (all seven days)."""
    out: dict[str, list[dict[str, str]]] = {d: [] for d in DAYS}
    for entry in data if isinstance(data, list) else []:
        if not isinstance(entry, dict):
            continue
        ranges = [
            {"start": p["startTime"], "end": p["endTime"]}
            for p in comfort_periods(entry.get("loopData"))
        ]
        for day in loop_to_days(entry.get("loops", "")):
            out[day] = ranges
    return out


def count_transitions(ranges: list[dict[str, str]]) -> tuple[int, bool]:
    """Transitions a day needs (excluding 00:00): (count, implicit_midnight). See R1."""
    ranges = ranges or []
    starts = {r["start"] for r in ranges}
    n = 0
    for r in ranges:
        if r["start"] != "00:00":
            n += 1
        if r["end"] not in starts:
            n += 1
    return n, bool(ranges) and "00:00" not in starts


def transitions_per_day(days: dict[str, list[dict[str, str]]]) -> dict[str, int]:
    """Transition count (R1, excluding midnight) per given day - for the "n / 6" display."""
    out: dict[str, int] = {}
    for day, ranges in days.items():
        if all(valid_hhmm(r.get("start")) and valid_hhmm(r.get("end")) for r in ranges):
            out[day], _ = count_transitions(ranges)
    return out


def validate_day(day: str, ranges: list[dict[str, str]]) -> list[dict[str, str]]:
    """Errors (contract §5.4) of a day. Empty = valid. Stops at the first failing step."""
    errors: list[dict[str, str]] = []
    for r in ranges:
        for value in (r.get("start"), r.get("end")):
            if not valid_hhmm(value):
                errors.append(
                    {"code": ERR_INVALID_TIME, "message": f"invalid time: {value!r}", "day": day}
                )
    if errors:
        return errors
    for r in ranges:
        if _to_minutes(r["start"]) >= _to_minutes(r["end"]):
            errors.append(
                {
                    "code": ERR_EMPTY_OR_REVERSED,
                    "message": f"empty or reversed slot: {r['start']}-{r['end']}",
                    "day": day,
                }
            )
    if errors:
        return errors
    ordered = sorted(ranges, key=lambda r: _to_minutes(r["start"]))
    for a, b in zip(ordered, ordered[1:], strict=False):
        if _to_minutes(b["start"]) < _to_minutes(a["end"]):
            errors.append(
                {
                    "code": ERR_OVERLAP,
                    "message": (
                        f"overlapping slots: {a['start']}-{a['end']} and {b['start']}-{b['end']}"
                    ),
                    "day": day,
                }
            )
    if errors:
        return errors
    n, _ = count_transitions(ordered)
    if n > MAX_TRANSITIONS_PER_DAY:
        errors.append(
            {
                "code": ERR_TOO_MANY,
                "message": (
                    f"too many slots: {n} transitions, the head keeps "
                    f"{MAX_TRANSITIONS_PER_DAY} at most (3 separate slots)"
                ),
                "day": day,
            }
        )
    return errors


def validate_days(days: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    """Errors of all given days (contract §5.4). Empty = valid."""
    errors: list[dict[str, str]] = []
    for day, ranges in days.items():
        errors.extend(validate_day(day, ranges))
    return errors


def to_periods(ranges: list[dict[str, str]]) -> list[dict[str, str]]:
    """{start, end} slots -> sorted ``periods`` ``[{startTime, endTime}]`` (write_week_day)."""
    ordered = sorted(ranges, key=lambda r: _to_minutes(r["start"]))
    return [{"startTime": r["start"], "endTime": r["end"]} for r in ordered]


@dataclass
class Schedule:
    """Weekly schedule of a target (contract §3.2)."""

    target: dict[str, str]
    name: str | None = None
    comfort: float | None = None
    setback: float | None = None
    days: dict[str, list[dict[str, str]]] = field(default_factory=lambda: {d: [] for d in DAYS})
    source: str = "cloud"
    read_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "target": self.target,
            "name": self.name,
            "comfort": self.comfort,
            "setback": self.setback,
            "days": {d: self.days.get(d, []) for d in DAYS},
            "max_transitions_per_day": MAX_TRANSITIONS_PER_DAY,
            "source": self.source,
            "read_at": self.read_at,
        }
