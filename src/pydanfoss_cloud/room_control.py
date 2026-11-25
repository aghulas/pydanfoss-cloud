"""Control loops for the standalone room coordinator (``docs/z2m-cloud-parity.md`` §5.5–§5.8).

Pure state trackers with injectable timestamps (no MQTT, no threads, no network), used by
:mod:`pydanfoss_cloud.room_coordinator`:

- **Stage 3 (Room sensor feed)** + **Stage 6 (Load balancing)** via :class:`RoomTelemetryTracker`:
  event-driven and keep-alive external-sensor writes (both ``radiator_covered`` modes), 30-min
  sensor silence -> ``-8000``, load mean across ``load_balancing_enable`` rooms, and per-head 5-min
  radio budget coalescing (sensor + load share one ``/set``).
- **Stage 5 (Reconciliation)** via :class:`ReconcileTracker`: compares reported state against
  ``room.desired`` (via :func:`~pydanfoss_cloud.room_view.member_drift_keys`), rewrites a differing
  head at most once an hour and three times in a row, then stops and flags it as drifting.
- **Stage 7 (Open window across a room)** via :class:`WindowPropagationTracker`: when a member
  reports an internal open window, sets ``window_open_external`` on the other members until it
  closes or a maximum duration elapses.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .decode import DanfossDevice
from .room_store import CoordinatorRoom, normalize_address
from .room_view import member_drift_keys
from .z2m_contract import encode_z2m_set_command

# Stage 3 — External sensor timing (§3, §5.6)
SENSOR_DELTA_C = 0.2
SENSOR_MIN_INTERVAL_COVERED_S = 300.0  # 5 min (radiator_covered=True or unknown)
SENSOR_KEEP_ALIVE_COVERED_S = 1200.0  # 20 min (< 35 min head reset)
SENSOR_MIN_INTERVAL_OFFSET_S = 1800.0  # 30 min (radiator_covered=False)
SENSOR_KEEP_ALIVE_OFFSET_S = 7200.0  # 2 h (< 3 h head reset)
SENSOR_SILENCE_TIMEOUT_S = 1800.0  # 30 min without sensor message -> -8000 once
SENSOR_DISABLED_C = -80.0  # encodes to -8000 (0.01 °C)

# Stage 6 — Load balancing (§5.6)
LOAD_MEAN_DELTA = 25
LOAD_MEAN_REFRESH_S = 900.0  # 15 min
LOAD_DISABLED_RAW = -8000

# Radio budget (§5.8)
TELEMETRY_MIN_INTERVAL_S = 300.0  # at most one telemetry write per head every 5 min

# Stage 5 — Reconciliation (§5.5)
RECONCILE_COOLDOWN_S = 3600.0  # at most once an hour
RECONCILE_MAX_ATTEMPTS = 3  # at most 3 times in a row

# Stage 7 — Open window across a room (§5.7)
WINDOW_OPEN_MAX_DURATION_S = 1800.0  # 30 min safety cap


@dataclass
class _HeadSensorFeed:
    value: float | None = None  # None when disabled / -8000 / never fed
    sent_at: float | None = None


@dataclass
class _RoomLoadFeed:
    last_mean: int | None = None
    sent_at: float | None = None
    active_members: set[str] = field(default_factory=set)  # normalized addresses


class RoomTelemetryTracker:
    """Tracks Stage 3 (external sensor feed) and Stage 6 (load mean) with §5.8 coalescing."""

    def __init__(
        self,
        *,
        telemetry_min_interval_s: float = TELEMETRY_MIN_INTERVAL_S,
        sensor_silence_timeout_s: float = SENSOR_SILENCE_TIMEOUT_S,
    ) -> None:
        self._min_interval_s = telemetry_min_interval_s
        self._silence_timeout_s = sensor_silence_timeout_s
        self._sensor_last_seen: dict[str, float] = {}
        self._sensor_feeds: dict[str, _HeadSensorFeed] = {}
        self._load_feeds: dict[str, _RoomLoadFeed] = {}
        self._last_head_write: dict[str, float] = {}

    def note_sensor_seen(self, sensor_addr: str, now: float) -> None:
        self._sensor_last_seen[normalize_address(sensor_addr)] = now

    def drop_room(self, room_id: str) -> None:
        self._load_feeds.pop(room_id, None)

    def _sensor_cadence(self, dev: DanfossDevice) -> tuple[float, float]:
        """Return ``(min_interval_s, keep_alive_s)`` for ``dev.radiator_covered`` (§5.6)."""
        if dev.radiator_covered is False:
            return SENSOR_MIN_INTERVAL_OFFSET_S, SENSOR_KEEP_ALIVE_OFFSET_S
        return max(self._min_interval_s, SENSOR_MIN_INTERVAL_COVERED_S), SENSOR_KEEP_ALIVE_COVERED_S

    def _planned_sensor_temp(
        self,
        dev: DanfossDevice,
        sensor: DanfossDevice | None,
        sensor_addr: str,
        now: float,
    ) -> tuple[float | None, float | None, bool]:
        """Return ``(payload_temp_c, new_feed_value, should_send)`` for one head."""
        addr = normalize_address(dev.id)
        feed = self._sensor_feeds.setdefault(addr, _HeadSensorFeed())
        min_iv, keep_alive = self._sensor_cadence(dev)

        if feed.sent_at is not None and (now - feed.sent_at) < min_iv:
            return None, None, False

        last_seen = self._sensor_last_seen.get(normalize_address(sensor_addr))
        sensor_fresh = (
            sensor is not None
            and sensor.online
            and sensor.temperature is not None
            and last_seen is not None
            and (now - last_seen) < self._silence_timeout_s
        )
        if not sensor_fresh:
            if feed.value is None:
                return None, None, False
            return SENSOR_DISABLED_C, None, True

        assert sensor is not None and sensor.temperature is not None
        temp = float(sensor.temperature)
        due = (
            feed.value is None
            or feed.sent_at is None
            or abs(temp - feed.value) >= SENSOR_DELTA_C - 1e-9
            or (now - feed.sent_at) >= keep_alive
        )
        if not due:
            return None, None, False
        return temp, temp, True

    def _planned_load_mean(
        self,
        room: CoordinatorRoom,
        members_by_norm: dict[str, DanfossDevice],
        now: float,
    ) -> int | None:
        """Return new ``load_room_mean`` if due for ``room``, else ``None`` (§5.6)."""
        if len(room.members) < 2:
            return None
        resolved: list[DanfossDevice] = []
        for addr in room.members:
            dev = members_by_norm.get(normalize_address(addr))
            if dev is None or not dev.online or dev.load_balancing_enable is not True:
                return None
            resolved.append(dev)

        loads = [dev.load_estimate for dev in resolved if dev.load_estimate is not None]
        if len(loads) < 2:
            return None

        mean_load = int(round(sum(loads) / len(loads)))
        state = self._load_feeds.setdefault(room.room_id, _RoomLoadFeed())
        due = (
            state.last_mean is None
            or state.sent_at is None
            or abs(mean_load - state.last_mean) >= LOAD_MEAN_DELTA
            or (now - state.sent_at) >= LOAD_MEAN_REFRESH_S
        )
        return mean_load if due else None

    def evaluate_room(
        self,
        room: CoordinatorRoom,
        members: Sequence[DanfossDevice],
        sensor: DanfossDevice | None,
        *,
        feed_external_sensor: bool,
        feed_load_mean: bool,
        now: float,
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        """Compute coalesced ``/set`` payloads for ``room`` respecting the 5-min radio floor."""
        by_norm = {normalize_address(d.id): d for d in members}
        online_members = [
            by_norm[normalize_address(a)]
            for a in room.members
            if normalize_address(a) in by_norm and by_norm[normalize_address(a)].online
        ]
        if not online_members:
            return []

        want_load: int | None = None
        if feed_load_mean:
            want_load = self._planned_load_mean(room, by_norm, now)

        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        load_sent_to: list[str] = []

        for dev in online_members:
            addr = normalize_address(dev.id)
            last_write = self._last_head_write.get(addr)
            if last_write is not None and (now - last_write) < self._min_interval_s:
                continue

            kwargs: dict[str, Any] = {}
            sensor_new_val: float | None = None
            send_sensor = False

            if feed_external_sensor and room.sensor:
                temp_c, sensor_new_val, send_sensor = self._planned_sensor_temp(
                    dev, sensor, room.sensor, now
                )
                if send_sensor and temp_c is not None:
                    kwargs["external_sensor_temp_c"] = temp_c

            if want_load is not None:
                kwargs["load_room_mean"] = want_load

            if not kwargs:
                continue

            payload = encode_z2m_set_command(**kwargs)
            writes.append((dev, payload))
            self._last_head_write[addr] = now
            if send_sensor:
                self._sensor_feeds[addr] = _HeadSensorFeed(value=sensor_new_val, sent_at=now)
            if want_load is not None:
                load_sent_to.append(addr)

        if want_load is not None and load_sent_to:
            lf = self._load_feeds.setdefault(room.room_id, _RoomLoadFeed())
            lf.last_mean = want_load
            lf.sent_at = now
            lf.active_members.update(load_sent_to)

        return writes

    def stop_load_mean_writes(
        self,
        rooms: Sequence[CoordinatorRoom],
        fleet: dict[str, DanfossDevice],
        armed_rooms: frozenset[str],
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        """Write ``load_room_mean = -8000`` on clean stop to active armed heads (§5.6)."""
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        for room in rooms:
            if room.room_id not in armed_rooms:
                continue
            lf = self._load_feeds.get(room.room_id)
            if lf is None or lf.last_mean is None or not lf.active_members:
                continue
            for addr in room.members:
                norm = normalize_address(addr)
                if norm not in lf.active_members:
                    continue
                dev = fleet.get(norm)
                if dev is not None and dev.online:
                    writes.append((dev, encode_z2m_set_command(load_room_mean=LOAD_DISABLED_RAW)))
            lf.last_mean = None
            lf.active_members.clear()
        return writes


@dataclass
class _HeadReconcileState:
    attempts: int = 0
    last_attempt_at: float | None = None
    exhausted: bool = False


class ReconcileTracker:
    """Tracks Stage 5 drift reconciliation (<= 1/hour, <= 3 consecutive attempts, §5.5)."""

    def __init__(
        self,
        *,
        cooldown_s: float = RECONCILE_COOLDOWN_S,
        max_attempts: int = RECONCILE_MAX_ATTEMPTS,
    ) -> None:
        self._cooldown_s = cooldown_s
        self._max_attempts = max_attempts
        self._states: dict[tuple[str, str], _HeadReconcileState] = {}

    def reset_room(self, room_id: str) -> None:
        for key in [k for k in self._states if k[0] == room_id]:
            self._states.pop(key, None)

    def blocked_members(self, room: CoordinatorRoom) -> list[str]:
        return [
            addr
            for addr in room.members
            if (st := self._states.get((room.room_id, normalize_address(addr)))) is not None
            and st.exhausted
        ]

    def evaluate_room(
        self,
        room: CoordinatorRoom,
        members: Sequence[DanfossDevice],
        *,
        now: float,
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        if not room.desired:
            self.reset_room(room.room_id)
            return []

        by_norm = {normalize_address(d.id): d for d in members}
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []

        for addr in room.members:
            norm = normalize_address(addr)
            dev = by_norm.get(norm)
            if dev is None or not dev.online:
                continue

            drifts = member_drift_keys(room, dev)
            key = (room.room_id, norm)
            if not drifts:
                self._states.pop(key, None)
                continue

            state = self._states.setdefault(key, _HeadReconcileState())
            if state.exhausted:
                continue
            if (
                state.last_attempt_at is not None
                and (now - state.last_attempt_at) < self._cooldown_s
            ):
                continue
            if state.attempts >= self._max_attempts:
                state.exhausted = True
                continue

            payload = _build_reconcile_payload(room, drifts)
            if not payload:
                continue
            state.attempts += 1
            state.last_attempt_at = now
            writes.append((dev, payload))

        return writes


def _build_reconcile_payload(room: CoordinatorRoom, drifts: Sequence[str]) -> dict[str, Any]:
    """Build one ``/set`` payload for the drifting keys in ``room.desired``."""
    kwargs: dict[str, Any] = {}
    desired = room.desired
    if "setpoint" in drifts:
        kwargs["setpoint"] = float(desired["setpoint"])
        kwargs["setpoint_scheduled"] = True  # coordinator change (§5.2)
    if "child_lock" in drifts:
        kwargs["child_lock"] = bool(desired["child_lock"])
    if "window_detection" in drifts:
        kwargs["window_detection"] = bool(desired["window_detection"])
    if "min_setpoint" in drifts:
        kwargs["min_setpoint"] = float(desired["min_setpoint"])
    if "max_setpoint" in drifts:
        kwargs["max_setpoint"] = float(desired["max_setpoint"])
    if "mode" in drifts:
        norm_mode = str(desired["mode"]).strip().lower()
        if norm_mode == "schedule":
            kwargs["preheat"] = bool(desired.get("preheat", False))
            # If also drifting on mode, encode_z2m_set_command handles programming_operation_mode.
        else:
            payload = encode_z2m_set_command(**kwargs) if kwargs else {}
            payload["programming_operation_mode"] = "setpoint"
            return payload
    elif "preheat" in drifts:
        kwargs["preheat"] = bool(desired["preheat"])
    return encode_z2m_set_command(**kwargs) if kwargs else {}


@dataclass
class _RoomWindowState:
    started_at: float | None = None
    expired: bool = False
    driven_heads: set[str] = field(default_factory=set)
    clearing_heads: set[str] = field(default_factory=set)


class WindowPropagationTracker:
    """Tracks Stage 7 cross-room open-window propagation (``window_open_external``, §5.7)."""

    def __init__(self, *, max_duration_s: float = WINDOW_OPEN_MAX_DURATION_S) -> None:
        self._max_duration_s = max_duration_s
        self._rooms: dict[str, _RoomWindowState] = {}

    def drop_room(self, room_id: str) -> None:
        self._rooms.pop(room_id, None)

    def _clear_driven(
        self,
        state: _RoomWindowState,
        by_norm: dict[str, DanfossDevice],
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        for norm in list(state.driven_heads):
            dev = by_norm.get(norm)
            if dev is not None and dev.online:
                writes.append((dev, encode_z2m_set_command(window_open_external=False)))
                if dev.window_open is True:
                    state.clearing_heads.add(norm)
            state.driven_heads.discard(norm)
        return writes

    def evaluate_room(
        self,
        room: CoordinatorRoom,
        members: Sequence[DanfossDevice],
        *,
        now: float,
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        by_norm = {normalize_address(d.id): d for d in members}
        state = self._rooms.setdefault(room.room_id, _RoomWindowState())

        for norm in list(state.clearing_heads):
            dev = by_norm.get(norm)
            if dev is None or dev.window_open is not True:
                state.clearing_heads.discard(norm)

        internal_open: set[str] = set()
        for addr in room.members:
            norm = normalize_address(addr)
            dev = by_norm.get(norm)
            if (
                dev is not None
                and dev.online
                and dev.window_open is True
                and dev.window_open_internal != "external_open"
                and norm not in state.driven_heads
                and norm not in state.clearing_heads
            ):
                internal_open.add(norm)

        if not internal_open:
            state.started_at = None
            state.expired = False
            return self._clear_driven(state, by_norm)

        if state.started_at is None:
            state.started_at = now
        if not state.expired and (now - state.started_at) >= self._max_duration_s:
            state.expired = True

        if state.expired:
            return self._clear_driven(state, by_norm)

        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        for addr in room.members:
            norm = normalize_address(addr)
            if norm in internal_open or norm in state.driven_heads:
                continue
            dev = by_norm.get(norm)
            if dev is not None and dev.online:
                state.clearing_heads.discard(norm)
                writes.append((dev, encode_z2m_set_command(window_open_external=True)))
                state.driven_heads.add(norm)

        return writes
