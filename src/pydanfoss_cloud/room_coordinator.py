"""Standalone room coordinator daemon (``docs/z2m-cloud-parity.md`` §5–§7).

A sibling of ``danfoss2mqtt`` for heads paired to a **Zigbee2MQTT** coordinator with no Danfoss
gateway and no cloud. It subscribes to the Zigbee2MQTT prefix (read only unless explicitly armed),
keeps its own room & holiday store (:mod:`pydanfoss_cloud.room_store`), and publishes **under its
own prefix** — never inside Zigbee2MQTT's namespace:

- ``<base>/bridge/state`` (retained, last-will ``offline``),
- ``<base>/bridge/rooms`` (retained): the aggregated room objects + ``desired`` / ``drift`` (§5.1),
- ``<base>/bridge/holidays`` (retained): coordinator-managed holiday entries (§5.4),
- ``<base>/bridge/heat_demand`` (retained): rooms calling for heat (§5.7),
- ``<base>/bridge/request/<action>`` -> ``<base>/bridge/response/<action>``.

**All write stages are closed by default** (§7, project rule):
- No head receives any ``/set`` unless its ``room_id`` is in ``armed_rooms`` (empty by default).
- Autonomous / reactive loops (Stage 3 sensor feed, Stage 5 reconciliation, Stage 6 load mean,
  Stage 7 holidays & open-window propagation) additionally require their own opt-in flag
  (all ``False`` by default).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .bridge_daemon import (
    AVAIL_OFFLINE,
    AVAIL_ONLINE,
    MqttTransport,
    PahoMqttTransport,
)
from .decode import DanfossDevice
from .room_control import (
    RECONCILE_COOLDOWN_S,
    RECONCILE_MAX_ATTEMPTS,
    SENSOR_SILENCE_TIMEOUT_S,
    TELEMETRY_MIN_INTERVAL_S,
    WINDOW_OPEN_MAX_DURATION_S,
    ReconcileTracker,
    RoomTelemetryTracker,
    WindowPropagationTracker,
)
from .room_store import (
    CoordinatorHoliday,
    CoordinatorRoom,
    RoomStore,
    RoomStoreError,
    normalize_address,
)
from .room_view import (
    DEFAULT_HEAT_DEMAND_VALID_S,
    build_heat_demand,
    build_holidays_view,
    build_room_view,
    member_drift_keys,
)
from .schedule import DAYS, Schedule
from .write_job import (
    HEAD_APPLIED,
    HEAD_MISMATCH,
    HEAD_UNREACHABLE,
    Z2M_SCHEDULE_DAY_NAMES,
    HeadWriteResult,
    HolidayValidationError,
    ScheduleWriteValidationError,
    SetpointValidationError,
    SettingsValidationError,
    _job_status,
    build_z2m_schedule_payloads,
    parse_iso_datetime,
    recompute_status,
    run_head_write_loop,
    run_setpoint_job,
    validate_holiday,
    validate_room_settings,
    validate_setpoint,
    validate_step_setpoint,
)
from .z2m_contract import decode_z2m_descriptor, decode_z2m_state, encode_z2m_set_command

_LOGGER = logging.getLogger(__name__)

DEFAULT_SOURCE_PREFIX = "zigbee2mqtt"
DEFAULT_BASE_TOPIC = "danfoss-rooms"

# Room request actions; every one mutates the STORE only, never a head.
_ROOM_STORE_ACTIONS = frozenset(
    {"room/create", "room/rename", "room/set-members", "room/set-sensor", "room/delete"}
)

_SETPOINT_ACTION = "room/set-setpoint"
_SCHEDULE_SET_ACTIONS = frozenset({"room/set-schedule", "set_schedule"})
_SCHEDULE_GET_ACTIONS = frozenset({"room/get-schedule", "get_schedule"})
_SCHEDULE_RESTORE_ACTIONS = frozenset({"room/restore-schedule", "restore_schedule"})
_COMFORT_SETBACK_ACTIONS = frozenset({"room/set-comfort-setback", "room/set-setpoints"})
_MODE_ACTION = "room/set-mode"
_SETTINGS_ACTION = "room/set-settings"

DEFAULT_SETPOINT_TIMEOUT_S = 30.0
DEFAULT_SETPOINT_RETRIES = 2
DEFAULT_TICK_INTERVAL_S = 15.0
_SETPOINT_MATCH_C = 0.1
_DEFAULT_COMFORT_C = 21.0
_DEFAULT_SETBACK_C = 17.0
_DEFAULT_PAUSE_C = 15.0
_VALID_MODES = frozenset({"schedule", "manual", "pause"})


def _as_text(raw: str | bytes) -> str:
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def _default_wall_clock() -> datetime:
    return datetime.now().astimezone()


def _extract_room_id(payload: Mapping[str, Any]) -> str:
    rid = str(payload.get("room_id") or "").strip()
    if rid:
        return rid
    target = payload.get("target")
    if isinstance(target, Mapping) and target.get("type") == "room":
        rid = str(target.get("id") or "").strip()
        if rid:
            return rid
    raise RoomStoreError("room_id is required")


class RoomCoordinator:
    """Room coordinator over a Zigbee2MQTT prefix, transport-agnostic (testable).

    Inject any :class:`~pydanfoss_cloud.bridge_daemon.MqttTransport`; the daemon publishes and
    subscribes through it and never touches the network directly.
    """

    def __init__(
        self,
        transport: MqttTransport,
        store: RoomStore,
        *,
        source_prefix: str = DEFAULT_SOURCE_PREFIX,
        base_topic: str = DEFAULT_BASE_TOPIC,
        heat_demand_valid_s: int = DEFAULT_HEAT_DEMAND_VALID_S,
        armed_rooms: tuple[str, ...] = (),
        setpoint_timeout_s: float = DEFAULT_SETPOINT_TIMEOUT_S,
        setpoint_retries: int = DEFAULT_SETPOINT_RETRIES,
        feed_external_sensor: bool = False,
        mode_change_keeps_schedule: bool = False,
        reconcile_drift: bool = False,
        reconcile_cooldown_s: float = RECONCILE_COOLDOWN_S,
        reconcile_max_attempts: int = RECONCILE_MAX_ATTEMPTS,
        feed_load_mean: bool = False,
        enable_holidays: bool = False,
        propagate_window_open: bool = False,
        window_open_max_duration_s: float = WINDOW_OPEN_MAX_DURATION_S,
        telemetry_min_interval_s: float = TELEMETRY_MIN_INTERVAL_S,
        sensor_silence_timeout_s: float = SENSOR_SILENCE_TIMEOUT_S,
        tick_interval_s: float = DEFAULT_TICK_INTERVAL_S,
        max_writes_per_tick: int = 0,
        monotonic_fn: Callable[[], float] | None = None,
        wall_clock_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._tx = transport
        self._store = store
        self._source = source_prefix.rstrip("/") or DEFAULT_SOURCE_PREFIX
        self._base = base_topic.rstrip("/") or DEFAULT_BASE_TOPIC
        if self._source == self._base:
            raise ValueError(
                "the coordinator must publish under a prefix different from the source"
            )
        self._valid = heat_demand_valid_s
        # CLOSED BY DEFAULT (§7, project rule): empty ``armed_rooms`` means no room is written.
        self._armed_rooms = frozenset(r for r in armed_rooms if r)
        self._setpoint_timeout_s = setpoint_timeout_s
        self._setpoint_retries = setpoint_retries

        # Stage 3–7 feature gates (all False by default).
        self._feed_external_sensor = feed_external_sensor
        self._mode_change_keeps_schedule = mode_change_keeps_schedule
        self._reconcile_drift = reconcile_drift
        self._feed_load_mean = feed_load_mean
        self._enable_holidays = enable_holidays
        self._propagate_window_open = propagate_window_open

        self._tick_interval_s = tick_interval_s
        self._max_writes_per_tick = max(0, int(max_writes_per_tick))
        self._now = monotonic_fn or time.monotonic
        self._wall_now = wall_clock_fn or _default_wall_clock

        self._telemetry = RoomTelemetryTracker(
            telemetry_min_interval_s=telemetry_min_interval_s,
            sensor_silence_timeout_s=sensor_silence_timeout_s,
        )
        self._reconcile = ReconcileTracker(
            cooldown_s=reconcile_cooldown_s,
            max_attempts=reconcile_max_attempts,
        )
        self._windows = WindowPropagationTracker(max_duration_s=window_open_max_duration_s)

        # normalized address -> reported device / friendly mapping / optional schedule_<day>
        self._fleet: dict[str, DanfossDevice] = {}
        self._friendly_to_addr: dict[str, str] = {}
        self._addr_to_friendly: dict[str, str] = {}
        self._reported_schedules: dict[str, dict[str, Any]] = {}

        # Single re-entrant lock + condition for state confirmations and thread safety.
        self._lock = threading.RLock()
        self._state_cv = threading.Condition(self._lock)
        self._state_seq: dict[str, int] = {}
        self._write_baseline: dict[str, int] = {}
        self._stopping = threading.Event()
        self._jobs: list[threading.Thread] = []
        self._tick_thread: threading.Thread | None = None

    @property
    def base_topic(self) -> str:
        return self._base

    # --- lifecycle -----------------------------------------------------------------------------

    def start(self) -> None:
        """Announce online, subscribe to source & request topics, reconcile holidays & publish."""
        self._stopping.clear()
        self._tx.publish(f"{self._base}/bridge/state", AVAIL_ONLINE, retain=True)
        self._tx.subscribe(f"{self._source}/#", self._on_source_message)
        # ``#`` (not ``+``): room actions are two segments, e.g. ``bridge/request/room/create``.
        self._tx.subscribe(f"{self._base}/bridge/request/#", self._on_request)
        if self._enable_holidays:
            self._reconcile_holidays(self._wall_now())
        self.republish()
        if self._tick_interval_s > 0:
            self._tick_thread = threading.Thread(
                target=self._tick_loop,
                name="coordinator-tick",
                daemon=True,
            )
            self._tick_thread.start()

    def stop(self) -> None:
        """Stop running workers, send -8000 for active load mean (§5.6), clear heat demand."""
        self._stopping.set()
        with self._state_cv:
            self._state_cv.notify_all()
        if self._tick_thread is not None:
            self._tick_thread.join(timeout=2.0)
            self._tick_thread = None
        for thread in list(self._jobs):
            thread.join(timeout=self._setpoint_timeout_s + 1.0)
        if self._feed_load_mean:
            with self._lock:
                stop_writes = self._telemetry.stop_load_mean_writes(
                    self._store.rooms(), self._fleet, self._armed_rooms
                )
            self._publish_writes(stop_writes)
        self._tx.publish(
            f"{self._base}/bridge/heat_demand",
            json.dumps(build_heat_demand({}, valid_for_s=self._valid)),
            retain=True,
        )
        self._tx.publish(f"{self._base}/bridge/state", AVAIL_OFFLINE, retain=True)

    def _tick_loop(self) -> None:
        while not self._stopping.wait(timeout=self._tick_interval_s):
            with contextlib.suppress(Exception):
                self.tick()

    def tick(
        self,
        *,
        now: float | None = None,
        wall_now: datetime | None = None,
    ) -> dict[str, int]:
        """Run one periodic evaluation pass (telemetry, reconciliation, holidays, open-window).

        Public and deterministic so tests can drive time without sleeping.
        """
        mono = self._now() if now is None else now
        wall = self._wall_now() if wall_now is None else wall_now

        telemetry_writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        reconcile_writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        window_writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        holiday_changed = False
        blocked_changed = False

        with self._lock:
            if self._enable_holidays:
                holiday_changed = self._reconcile_holidays(wall)

            for room in self._store.rooms():
                if room.room_id not in self._armed_rooms:
                    continue
                members = self._members_of(room.members)
                sensor = self._fleet.get(normalize_address(room.sensor)) if room.sensor else None

                if self._feed_external_sensor or self._feed_load_mean:
                    telemetry_writes.extend(
                        self._telemetry.evaluate_room(
                            room,
                            members,
                            sensor,
                            feed_external_sensor=self._feed_external_sensor,
                            feed_load_mean=self._feed_load_mean,
                            now=mono,
                        )
                    )
                if self._reconcile_drift:
                    before_blocked = self._reconcile.blocked_members(room)
                    reconcile_writes.extend(self._reconcile.evaluate_room(room, members, now=mono))
                    if self._reconcile.blocked_members(room) != before_blocked:
                        blocked_changed = True
                if self._propagate_window_open:
                    window_writes.extend(self._windows.evaluate_room(room, members, now=mono))

        all_writes = telemetry_writes + reconcile_writes + window_writes
        if self._max_writes_per_tick > 0:
            all_writes = all_writes[: self._max_writes_per_tick]
        self._publish_writes(all_writes)

        if reconcile_writes or blocked_changed or holiday_changed:
            self.republish()

        return {
            "telemetry_writes": len(telemetry_writes),
            "reconcile_writes": len(reconcile_writes),
            "window_writes": len(window_writes),
        }

    # --- source (Zigbee2MQTT) ingestion ---------------------------------------------------------

    def _on_source_message(self, topic: str, raw: str | bytes) -> None:
        prefix = f"{self._source}/"
        if not topic.startswith(prefix):
            return
        suffix = topic[len(prefix) :]
        if suffix == "bridge/devices":
            self._ingest_devices(raw)
            return
        if suffix.startswith("bridge/"):
            return
        if suffix.endswith("/availability"):
            friendly = suffix[: -len("/availability")]
            if "/" not in friendly:
                self._ingest_availability(friendly, raw)
            return
        if "/" in suffix:
            return
        self._ingest_state(suffix, raw)

    def _ingest_devices(self, raw: str | bytes) -> None:
        try:
            entries = json.loads(_as_text(raw))
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(entries, list):
            return

        with self._lock:
            seen: set[str] = set()
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                dev = decode_z2m_descriptor(entry)
                if dev is None or not (dev.is_head or dev.is_sensor):
                    continue
                friendly = str(entry.get("friendly_name") or dev.name)
                addr = normalize_address(dev.id)
                seen.add(addr)
                existing = self._fleet.get(addr)
                if existing is not None:
                    existing.name = dev.name
                    existing.product_id = dev.product_id or existing.product_id
                    existing.node_id = dev.node_id or existing.node_id
                else:
                    self._fleet[addr] = dev
                self._friendly_to_addr[friendly] = addr
                self._addr_to_friendly[addr] = friendly

            for addr in [a for a in self._fleet if a not in seen]:
                self._fleet.pop(addr, None)
                self._reported_schedules.pop(addr, None)
                stale_friendly = self._addr_to_friendly.pop(addr, None)
                if stale_friendly is not None:
                    self._friendly_to_addr.pop(stale_friendly, None)

        if self._enable_holidays:
            self._reconcile_holidays(self._wall_now())
        self.republish()

    def _ingest_state(self, friendly: str, raw: str | bytes) -> None:
        try:
            payload = json.loads(_as_text(raw))
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(payload, dict):
            return

        now = self._now()
        reactive_writes: list[tuple[DanfossDevice, dict[str, Any]]] = []

        with self._lock:
            addr = self._friendly_to_addr.get(friendly)
            dev = self._fleet.get(addr) if addr else None
            if dev is None:
                return
            decode_z2m_state(dev, payload)
            key = normalize_address(dev.id)

            # Capture optional Z2M ``schedule_<day>`` read-back properties if exposed (§5.3).
            for short_day, long_day in Z2M_SCHEDULE_DAY_NAMES.items():
                prop = f"schedule_{long_day}"
                if prop in payload and payload[prop] is not None:
                    self._reported_schedules.setdefault(key, {})[short_day] = payload[prop]

            if dev.is_sensor:
                self._telemetry.note_sensor_seen(key, now)

            self._state_seq[key] = self._state_seq.get(key, 0) + 1
            self._state_cv.notify_all()

            # Event-driven Stage 3 sensor feed & Stage 7B open-window propagation.
            reactive_writes = self._evaluate_event_driven_locked(key, dev, now)

        self._publish_writes(reactive_writes)
        self.republish()

    def _evaluate_event_driven_locked(
        self, dev_addr: str, dev: DanfossDevice, now: float
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        """Run event-driven Stage 3 (sensor/covered change) and Stage 7B (window) for ``dev``."""
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        for room in self._store.rooms():
            if room.room_id not in self._armed_rooms:
                continue
            member_norms = {normalize_address(m) for m in room.members}
            sensor_norm = normalize_address(room.sensor) if room.sensor else None
            is_sensor_for_room = sensor_norm == dev_addr
            is_member = dev_addr in member_norms
            if not (is_sensor_for_room or is_member):
                continue

            members = self._members_of(room.members)
            sensor = self._fleet.get(sensor_norm) if sensor_norm else None

            if self._feed_external_sensor and (is_sensor_for_room or is_member):
                writes.extend(
                    self._telemetry.evaluate_room(
                        room,
                        members,
                        sensor,
                        feed_external_sensor=True,
                        feed_load_mean=self._feed_load_mean,
                        now=now,
                    )
                )
            if self._propagate_window_open and is_member:
                writes.extend(self._windows.evaluate_room(room, members, now=now))
        return writes

    def _ingest_availability(self, friendly: str, raw: str | bytes) -> None:
        online = _parse_online(_as_text(raw))
        if online is None:
            return
        now = self._now()
        reactive_writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        with self._lock:
            addr = self._friendly_to_addr.get(friendly)
            dev = self._fleet.get(addr) if addr else None
            if dev is None:
                return
            dev.online = online
            if dev.is_sensor and online:
                self._telemetry.note_sensor_seen(addr or normalize_address(dev.id), now)
            reactive_writes = self._evaluate_event_driven_locked(
                normalize_address(dev.id), dev, now
            )
        self._publish_writes(reactive_writes)
        self.republish()

    # --- publication ----------------------------------------------------------------------------

    def _members_of(self, addresses: tuple[str, ...]) -> list[DanfossDevice]:
        out: list[DanfossDevice] = []
        for addr in addresses:
            dev = self._fleet.get(normalize_address(addr))
            if dev is not None:
                out.append(dev)
        return out

    def republish(self) -> None:
        """Recompute and publish ``bridge/rooms``, ``bridge/holidays``, ``bridge/heat_demand``."""
        with self._lock:
            rooms = self._store.rooms()
            views: list[dict[str, Any]] = []
            heat_members: dict[str, list[DanfossDevice]] = {}
            names: dict[str, str] = {}
            for room in rooms:
                members = self._members_of(room.members)
                sensor = self._fleet.get(normalize_address(room.sensor)) if room.sensor else None
                views.append(
                    build_room_view(
                        room,
                        members,
                        sensor,
                        reconcile_blocked=self._reconcile.blocked_members(room),
                    )
                )
                heat_members[room.room_id] = members
                names[room.room_id] = room.name
            holidays_payload = build_holidays_view(self._store.holidays(), now=self._wall_now())

        self._tx.publish(f"{self._base}/bridge/rooms", json.dumps(views), retain=True)
        self._tx.publish(f"{self._base}/bridge/holidays", json.dumps(holidays_payload), retain=True)
        self._tx.publish(
            f"{self._base}/bridge/heat_demand",
            json.dumps(build_heat_demand(heat_members, names, valid_for_s=self._valid)),
            retain=True,
        )

    def _publish_writes(self, writes: Sequence[tuple[DanfossDevice, dict[str, Any]]]) -> None:
        for dev, payload in writes:
            self._publish_payload(dev, payload)

    def _publish_payload(self, dev: DanfossDevice, payload: Mapping[str, Any]) -> None:
        friendly = self._addr_to_friendly.get(normalize_address(dev.id), dev.name)
        self._tx.publish(f"{self._source}/{friendly}/set", json.dumps(dict(payload)), retain=False)

    # --- request dispatch -----------------------------------------------------------------------

    def _on_request(self, topic: str, raw: str | bytes) -> None:
        prefix = f"{self._base}/bridge/request/"
        if not topic.startswith(prefix):
            return
        action = topic[len(prefix) :]
        if action == _SETPOINT_ACTION:
            self._on_set_setpoint(raw)
            return
        if action in _SCHEDULE_SET_ACTIONS:
            self._on_set_schedule(action, raw)
            return
        if action in _SCHEDULE_GET_ACTIONS:
            self._on_get_schedule(action, raw)
            return
        if action in _SCHEDULE_RESTORE_ACTIONS:
            self._on_set_mode(action, raw, forced_mode="schedule")
            return
        if action in _COMFORT_SETBACK_ACTIONS:
            self._on_set_comfort_setback(action, raw)
            return
        if action == _MODE_ACTION:
            self._on_set_mode(action, raw)
            return
        if action == _SETTINGS_ACTION:
            self._on_set_settings(raw)
            return
        reply = self._handle_request(action, raw)
        self._tx.publish(f"{self._base}/bridge/response/{action}", json.dumps(reply), retain=False)

    def _respond(self, action: str, reply: dict[str, Any], transaction: Any) -> None:
        if transaction is not None:
            reply["transaction"] = transaction
        self._tx.publish(f"{self._base}/bridge/response/{action}", json.dumps(reply), retain=False)

    def _parse_armed_room_request(
        self, action: str, raw: str | bytes, gate_label: str
    ) -> tuple[dict[str, Any], Any, CoordinatorRoom] | None:
        try:
            payload = json.loads(_as_text(raw)) if raw else {}
            if not isinstance(payload, dict):
                raise RoomStoreError("request payload must be a JSON object")
        except (ValueError, UnicodeDecodeError) as err:
            self._respond(action, {"status": "error", "error": f"invalid JSON: {err}"}, None)
            return None

        transaction = payload.get("transaction")
        try:
            with self._lock:
                room = self._store.get(_extract_room_id(payload))
        except RoomStoreError as err:
            self._respond(action, {"status": "error", "error": str(err)}, transaction)
            return None

        if room.room_id not in self._armed_rooms:
            self._respond(
                action,
                {
                    "status": "closed",
                    "error": (
                        f"room {room.room_id!r} is not armed for {gate_label} "
                        "(closed by default; add it to armed_rooms for a supervised trial)"
                    ),
                },
                transaction,
            )
            return None

        return payload, transaction, room

    def _split_members(self, room: CoordinatorRoom) -> tuple[list[DanfossDevice], list[str]]:
        with self._lock:
            present = self._members_of(room.members)
            present_addrs = {normalize_address(d.id) for d in present}
            absent = [a for a in room.members if normalize_address(a) not in present_addrs]
            return present, absent

    def _capture_baselines(self, present: Sequence[DanfossDevice]) -> None:
        with self._state_cv:
            for dev in present:
                addr = normalize_address(dev.id)
                self._write_baseline[addr] = self._state_seq.get(addr, 0)

    def _spawn_job(self, name: str, target: Callable[..., None], *args: Any) -> None:
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        self._jobs.append(thread)
        thread.start()

    # --- Stage 2: setpoint fan-out (closed by default) -----------------------------------------

    def _on_set_setpoint(self, raw: str | bytes) -> None:
        parsed = self._parse_armed_room_request(_SETPOINT_ACTION, raw, "setpoint fan-out")
        if parsed is None:
            return
        payload, transaction, room = parsed
        source = str(payload.get("source") or "user")
        present, absent = self._split_members(room)

        try:
            value = validate_setpoint(payload.get("setpoint"), present)
        except SetpointValidationError as err:
            self._respond(
                _SETPOINT_ACTION,
                {"status": "error", "error": str(err), "job_status": "failed"},
                transaction,
            )
            return

        self._capture_baselines(present)
        with self._lock:
            with contextlib.suppress(RoomStoreError):
                self._store.update_desired(room.room_id, {"setpoint": value})
            self._reconcile.reset_room(room.room_id)
        self.republish()

        self._spawn_job(
            f"setpoint-{room.room_id}",
            self._run_setpoint_job,
            room.room_id,
            value,
            present,
            absent,
            source,
            transaction,
        )

    def _run_setpoint_job(
        self,
        room_id: str,
        value: float,
        present: list[DanfossDevice],
        absent: list[str],
        source: str,
        transaction: Any,
    ) -> None:
        job = run_setpoint_job(
            room_id=room_id,
            value=value,
            members=present,
            source=source,
            publish_fn=self._publish_set,
            confirm_fn=self._confirm_setpoint,
            retries=self._setpoint_retries,
        )
        for addr in absent:
            job.heads.append(
                HeadWriteResult(
                    address=addr,
                    friendly=addr,
                    status=HEAD_UNREACHABLE,
                    detail="member not reported by the bridge",
                )
            )
        if absent:
            recompute_status(job)
        job_dict = job.to_dict()
        reply = {
            "status": "ok",
            "job_status": job_dict["status"],
            "room_id": job_dict["room_id"],
            "setpoint": job_dict["setpoint"],
            "property": job_dict["property"],
            "heads": job_dict["heads"],
        }
        self._respond(_SETPOINT_ACTION, reply, transaction)
        self.republish()

    def _publish_set(self, dev: DanfossDevice, prop: str, value: float) -> None:
        self._publish_payload(dev, {prop: float(value)})

    def _wait_confirm(
        self,
        dev: DanfossDevice,
        predicate: Callable[[DanfossDevice], bool],
        mismatch_detail: Callable[[DanfossDevice], str],
    ) -> tuple[str, str | None]:
        addr = normalize_address(dev.id)
        baseline = self._write_baseline.get(addr, 0)
        deadline = time.monotonic() + self._setpoint_timeout_s
        with self._state_cv:
            while True:
                cur = self._fleet.get(addr)
                if cur is not None and predicate(cur):
                    return HEAD_APPLIED, None
                if self._stopping.is_set():
                    return HEAD_UNREACHABLE, "coordinator stopping"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._state_seq.get(addr, 0) > baseline and cur is not None:
                        return HEAD_MISMATCH, mismatch_detail(cur)
                    return HEAD_UNREACHABLE, "no state reported within timeout"
                self._state_cv.wait(remaining)

    def _confirm_setpoint(self, dev: DanfossDevice, target: float) -> tuple[str, str | None]:
        return self._wait_confirm(
            dev,
            lambda cur: (
                cur.setpoint is not None and abs(cur.setpoint - target) <= _SETPOINT_MATCH_C
            ),
            lambda cur: f"reported {cur.setpoint} °C, wanted {target} °C",
        )

    # --- Stage 4: weekly schedule, comfort/setback & modes ------------------------------------

    def _on_get_schedule(self, action: str, raw: str | bytes) -> None:
        try:
            payload = json.loads(_as_text(raw)) if raw else {}
            if not isinstance(payload, dict):
                raise RoomStoreError("request payload must be a JSON object")
        except (ValueError, UnicodeDecodeError) as err:
            self._respond(action, {"status": "error", "error": f"invalid JSON: {err}"}, None)
            return

        transaction = payload.get("transaction")
        try:
            with self._lock:
                room = self._store.get(_extract_room_id(payload))
                reported: dict[str, dict[str, Any]] = {}
                for addr in room.members:
                    norm = normalize_address(addr)
                    if norm in self._reported_schedules:
                        reported[addr] = dict(self._reported_schedules[norm])
        except RoomStoreError as err:
            self._respond(
                action, {"status": "error", "code": "unknown_room", "error": str(err)}, transaction
            )
            return

        sched = room.schedule or {}
        raw_days = sched.get("days")
        days: Mapping[str, Any] = raw_days if isinstance(raw_days, Mapping) else {}
        comfort = sched.get("comfort", room.desired.get("comfort"))
        setback = sched.get("setback", room.desired.get("setback"))
        schedule_obj = Schedule(
            target={"type": "room", "id": room.room_id},
            name=room.name,
            comfort=float(comfort) if isinstance(comfort, (int, float)) else None,
            setback=float(setback) if isinstance(setback, (int, float)) else None,
            days={d: list(days.get(d, [])) for d in DAYS},
            source="coordinator",
            read_at=self._wall_now().isoformat(timespec="seconds"),
        ).to_dict()
        self._respond(
            action,
            {
                "status": "ok",
                "room_id": room.room_id,
                "schedule": room.schedule,
                "reported_schedules": reported,
                "data": schedule_obj,
            },
            transaction,
        )

    def _resolve_comfort_setback(
        self,
        room: CoordinatorRoom,
        present: Sequence[DanfossDevice],
        raw_comfort: Any,
        raw_setback: Any,
    ) -> tuple[float, float]:
        sched = room.schedule or {}
        c_val = (
            raw_comfort
            if raw_comfort is not None
            else sched.get("comfort", room.desired.get("comfort", _DEFAULT_COMFORT_C))
        )
        s_val = (
            raw_setback
            if raw_setback is not None
            else sched.get("setback", room.desired.get("setback", _DEFAULT_SETBACK_C))
        )
        if present:
            return validate_setpoint(c_val, present), validate_setpoint(s_val, present)
        return (
            validate_step_setpoint(c_val, label="comfort"),
            validate_step_setpoint(s_val, label="setback"),
        )

    def _on_set_schedule(self, action: str, raw: str | bytes) -> None:
        parsed = self._parse_armed_room_request(action, raw, "schedule fan-out")
        if parsed is None:
            return
        payload, transaction, room = parsed
        present, absent = self._split_members(room)

        raw_days = payload.get("days")
        if raw_days is None and isinstance(room.schedule, Mapping):
            raw_days = room.schedule.get("days")

        try:
            comfort, setback = self._resolve_comfort_setback(
                room, present, payload.get("comfort"), payload.get("setback")
            )
            if raw_days is None:
                raise ScheduleWriteValidationError("days is required")
            canonical_days, day_payloads = build_z2m_schedule_payloads(raw_days, comfort, setback)
        except (SetpointValidationError, ScheduleWriteValidationError) as err:
            errors = err.errors if isinstance(err, ScheduleWriteValidationError) else []
            first = errors[0] if errors else {}
            self._respond(
                action,
                {
                    "status": "error",
                    "code": first.get("code", "invalid_schedule"),
                    "error": str(err),
                    "errors": errors,
                    "day": first.get("day"),
                    "job_status": "failed",
                },
                transaction,
            )
            return

        preheat = (
            bool(payload["preheat"])
            if isinstance(payload.get("preheat"), bool)
            else bool(room.desired.get("preheat", False))
        )
        mode = str(payload.get("mode") or "schedule").strip().lower()
        if mode not in _VALID_MODES:
            self._respond(
                action,
                {"status": "error", "error": f"unsupported mode {mode!r}", "job_status": "failed"},
                transaction,
            )
            return

        stored_sched = {"comfort": comfort, "setback": setback, "days": canonical_days}
        self._capture_baselines(present)
        with self._lock:
            self._store.set_schedule(room.room_id, stored_sched)
            remove_keys = ("setpoint",) if mode == "schedule" else ()
            self._store.update_desired(
                room.room_id,
                {"comfort": comfort, "setback": setback, "mode": mode, "preheat": preheat},
                remove_keys=remove_keys,
            )
            self._reconcile.reset_room(room.room_id)
        self.republish()

        if mode == "schedule":
            prog_mode = "schedule_with_preheat" if preheat else "schedule"
        else:
            prog_mode = "setpoint"
        self._spawn_job(
            f"schedule-{room.room_id}",
            self._run_schedule_job,
            action,
            room.room_id,
            present,
            absent,
            day_payloads,
            prog_mode,
            mode == "schedule",
            transaction,
        )

    def _run_schedule_job(
        self,
        action: str,
        room_id: str,
        present: list[DanfossDevice],
        absent: list[str],
        day_payloads: list[dict[str, Any]],
        prog_mode: str,
        want_schedule_active: bool,
        transaction: Any,
    ) -> None:
        def _write_head(dev: DanfossDevice) -> None:
            for p in day_payloads:
                self._publish_payload(dev, p)
            self._publish_payload(dev, {"programming_operation_mode": prog_mode})

        def _confirm_head(dev: DanfossDevice) -> tuple[str, str | None]:
            return self._wait_confirm(
                dev,
                lambda cur: cur.schedule_active is want_schedule_active,
                lambda cur: (
                    f"reported schedule_active={cur.schedule_active}, wanted {want_schedule_active}"
                ),
            )

        heads = run_head_write_loop(
            present, write_fn=_write_head, confirm_fn=_confirm_head, retries=self._setpoint_retries
        )
        for addr in absent:
            heads.append(
                HeadWriteResult(
                    address=addr,
                    friendly=addr,
                    status=HEAD_UNREACHABLE,
                    detail="member not reported by the bridge",
                )
            )
        data: dict[str, Any] = {"target": {"type": "room", "id": room_id}, "written": True}
        if action in _SCHEDULE_RESTORE_ACTIONS:
            data["restored"] = True
        reply = {
            "status": "ok",
            "job_status": _job_status(heads),
            "room_id": room_id,
            "programming_operation_mode": prog_mode,
            "heads": [h.to_dict() for h in heads],
            "data": data,
        }
        self._respond(action, reply, transaction)
        self.republish()

    def _on_set_comfort_setback(self, action: str, raw: str | bytes) -> None:
        parsed = self._parse_armed_room_request(action, raw, "comfort/setback write")
        if parsed is None:
            return
        payload, transaction, room = parsed
        present, absent = self._split_members(room)

        raw_comfort = payload.get("comfort")
        raw_setback = payload.get("setback")
        raw_pause = payload.get("pause", payload.get("pause_setpoint"))
        if raw_comfort is None and raw_setback is None and raw_pause is None:
            self._respond(
                action,
                {
                    "status": "error",
                    "error": "give at least one of comfort, setback, pause",
                    "job_status": "failed",
                },
                transaction,
            )
            return

        try:
            comfort, setback = self._resolve_comfort_setback(
                room, present, raw_comfort, raw_setback
            )
            pause_sp: float | None = None
            if raw_pause is not None:
                pause_sp = (
                    validate_setpoint(raw_pause, present)
                    if present
                    else validate_step_setpoint(raw_pause, label="pause")
                )
            sched_days = room.schedule.get("days") if isinstance(room.schedule, Mapping) else None
            day_payloads: list[dict[str, Any]] = []
            canonical_days: dict[str, list[dict[str, str]]] | None = None
            if (raw_comfort is not None or raw_setback is not None) and isinstance(
                sched_days, Mapping
            ):
                canonical_days, day_payloads = build_z2m_schedule_payloads(
                    sched_days, comfort, setback
                )
        except (SetpointValidationError, ScheduleWriteValidationError) as err:
            self._respond(
                action, {"status": "error", "error": str(err), "job_status": "failed"}, transaction
            )
            return

        desired_updates: dict[str, Any] = {"comfort": comfort, "setback": setback}
        if pause_sp is not None:
            desired_updates["pause_setpoint"] = pause_sp

        current_mode = str(room.desired.get("mode") or "schedule").strip().lower()
        preheat = bool(room.desired.get("preheat", False))

        self._capture_baselines(present)
        with self._lock:
            if canonical_days is not None:
                self._store.set_schedule(
                    room.room_id, {"comfort": comfort, "setback": setback, "days": canonical_days}
                )
            if pause_sp is not None and current_mode == "pause":
                desired_updates["setpoint"] = pause_sp
            self._store.update_desired(room.room_id, desired_updates)
            self._reconcile.reset_room(room.room_id)
        self.republish()

        if day_payloads:
            # R3: changing comfort or setback rewrites the schedule on every member.
            prog_mode = (
                ("schedule_with_preheat" if preheat else "schedule")
                if current_mode == "schedule"
                else "setpoint"
            )
            self._spawn_job(
                f"comfort-setback-{room.room_id}",
                self._run_schedule_job,
                action,
                room.room_id,
                present,
                absent,
                day_payloads,
                prog_mode,
                current_mode == "schedule",
                transaction,
            )
        elif pause_sp is not None and current_mode == "pause":
            self._spawn_job(
                f"pause-setpoint-{room.room_id}",
                self._run_pause_or_mode_job,
                action,
                room.room_id,
                present,
                absent,
                "pause",
                pause_sp,
                "setpoint",
                transaction,
            )
        else:
            self._respond(
                action,
                {"status": "ok", "room_id": room.room_id, "desired": desired_updates, "heads": []},
                transaction,
            )

    def _on_set_mode(
        self, action: str, raw: str | bytes, *, forced_mode: str | None = None
    ) -> None:
        parsed = self._parse_armed_room_request(action, raw, "mode change")
        if parsed is None:
            return
        payload, transaction, room = parsed
        present, absent = self._split_members(room)

        mode = (forced_mode or str(payload.get("mode") or "")).strip().lower()
        if mode not in _VALID_MODES:
            self._respond(
                action,
                {
                    "status": "error",
                    "error": f"mode must be one of {sorted(_VALID_MODES)}",
                    "job_status": "failed",
                },
                transaction,
            )
            return

        preheat = bool(room.desired.get("preheat", False))
        day_payloads: list[dict[str, Any]] = []
        hold_setpoint: float | None = None

        if mode in ("manual", "pause"):
            raw_sp = payload.get("setpoint")
            if raw_sp is None:
                if mode == "pause":
                    raw_sp = room.desired.get("pause_setpoint", _DEFAULT_PAUSE_C)
                else:
                    raw_sp = room.desired.get("setpoint")
                    if raw_sp is None:
                        reported_sps = [d.setpoint for d in present if d.setpoint is not None]
                        raw_sp = reported_sps[0] if reported_sps else _DEFAULT_COMFORT_C
            try:
                hold_setpoint = (
                    validate_setpoint(raw_sp, present)
                    if present
                    else validate_step_setpoint(raw_sp, label="setpoint")
                )
            except SetpointValidationError as err:
                self._respond(
                    action,
                    {"status": "error", "error": str(err), "job_status": "failed"},
                    transaction,
                )
                return
            desired_updates: dict[str, Any] = {"mode": mode, "setpoint": hold_setpoint}
            if mode == "pause":
                desired_updates["pause_setpoint"] = hold_setpoint
            remove_keys: tuple[str, ...] = ()
            prog_mode = "setpoint"
        else:
            # Returning to schedule: rewrite stored schedule unless mode_change_keeps_schedule.
            if not self._mode_change_keeps_schedule and isinstance(room.schedule, Mapping):
                days = room.schedule.get("days")
                if isinstance(days, Mapping):
                    try:
                        comfort, setback = self._resolve_comfort_setback(room, present, None, None)
                        _, day_payloads = build_z2m_schedule_payloads(days, comfort, setback)
                    except (SetpointValidationError, ScheduleWriteValidationError) as err:
                        self._respond(
                            action,
                            {"status": "error", "error": str(err), "job_status": "failed"},
                            transaction,
                        )
                        return
            desired_updates = {"mode": "schedule"}
            remove_keys = ("setpoint",)
            prog_mode = "schedule_with_preheat" if preheat else "schedule"

        self._capture_baselines(present)
        with self._lock:
            self._store.update_desired(room.room_id, desired_updates, remove_keys=remove_keys)
            self._reconcile.reset_room(room.room_id)
        self.republish()

        if mode == "schedule" and day_payloads:
            self._spawn_job(
                f"mode-{room.room_id}",
                self._run_schedule_job,
                action,
                room.room_id,
                present,
                absent,
                day_payloads,
                prog_mode,
                True,
                transaction,
            )
            return

        self._spawn_job(
            f"mode-{room.room_id}",
            self._run_pause_or_mode_job,
            action,
            room.room_id,
            present,
            absent,
            mode,
            hold_setpoint,
            prog_mode,
            transaction,
        )

    def _run_pause_or_mode_job(
        self,
        action: str,
        room_id: str,
        present: list[DanfossDevice],
        absent: list[str],
        mode: str,
        hold_setpoint: float | None,
        prog_mode: str,
        transaction: Any,
    ) -> None:
        want_schedule = mode == "schedule"

        def _write_head(dev: DanfossDevice) -> None:
            out: dict[str, Any] = {"programming_operation_mode": prog_mode}
            if hold_setpoint is not None:
                out["occupied_heating_setpoint"] = float(hold_setpoint)
            self._publish_payload(dev, out)

        def _confirm_head(dev: DanfossDevice) -> tuple[str, str | None]:
            def _ok(cur: DanfossDevice) -> bool:
                if cur.schedule_active is not want_schedule:
                    return False
                if hold_setpoint is not None:
                    return (
                        cur.setpoint is not None
                        and abs(cur.setpoint - hold_setpoint) <= _SETPOINT_MATCH_C
                    )
                return True

            return self._wait_confirm(
                dev,
                _ok,
                lambda cur: (
                    f"reported schedule_active={cur.schedule_active}, setpoint={cur.setpoint}"
                ),
            )

        heads = run_head_write_loop(
            present, write_fn=_write_head, confirm_fn=_confirm_head, retries=self._setpoint_retries
        )
        for addr in absent:
            heads.append(
                HeadWriteResult(
                    address=addr,
                    friendly=addr,
                    status=HEAD_UNREACHABLE,
                    detail="member not reported by the bridge",
                )
            )
        reply: dict[str, Any] = {
            "status": "ok",
            "job_status": _job_status(heads),
            "room_id": room_id,
            "mode": mode,
            "programming_operation_mode": prog_mode,
            "heads": [h.to_dict() for h in heads],
        }
        if action in _SCHEDULE_RESTORE_ACTIONS:
            reply["data"] = {"target": {"type": "room", "id": room_id}, "restored": True}
        self._respond(action, reply, transaction)
        self.republish()

    # --- Stage 5: room-wide settings fan-out ---------------------------------------------------

    def _on_set_settings(self, raw: str | bytes) -> None:
        parsed = self._parse_armed_room_request(_SETTINGS_ACTION, raw, "room settings")
        if parsed is None:
            return
        payload, transaction, room = parsed
        present, absent = self._split_members(room)

        raw_settings = payload.get("settings")
        if raw_settings is None:
            raw_settings = {
                k: v for k, v in payload.items() if k not in ("room_id", "target", "transaction")
            }

        try:
            cleaned = validate_room_settings(raw_settings, present, current_desired=room.desired)
        except SettingsValidationError as err:
            self._respond(
                _SETTINGS_ACTION,
                {"status": "error", "error": str(err), "job_status": "failed"},
                transaction,
            )
            return

        self._capture_baselines(present)
        with self._lock:
            updated_room = self._store.update_desired(room.room_id, cleaned)
            self._reconcile.reset_room(room.room_id)
        self.republish()

        self._spawn_job(
            f"settings-{room.room_id}",
            self._run_settings_job,
            updated_room,
            cleaned,
            present,
            absent,
            transaction,
        )

    def _run_settings_job(
        self,
        room: CoordinatorRoom,
        cleaned: dict[str, Any],
        present: list[DanfossDevice],
        absent: list[str],
        transaction: Any,
    ) -> None:
        in_schedule_mode = str(room.desired.get("mode") or "schedule").strip().lower() == "schedule"

        def _head_payload(dev: DanfossDevice) -> dict[str, Any]:
            drifts = set(member_drift_keys(room, dev))
            kw: dict[str, Any] = {}
            if "child_lock" in cleaned and (dev.child_lock is None or "child_lock" in drifts):
                kw["child_lock"] = cleaned["child_lock"]
            if "window_detection" in cleaned and (
                dev.window_detection is None or "window_detection" in drifts
            ):
                kw["window_detection"] = cleaned["window_detection"]
            if "min_setpoint" in cleaned and (dev.min_setpoint is None or "min_setpoint" in drifts):
                kw["min_setpoint"] = cleaned["min_setpoint"]
            if "max_setpoint" in cleaned and (dev.max_setpoint is None or "max_setpoint" in drifts):
                kw["max_setpoint"] = cleaned["max_setpoint"]
            if (
                "preheat" in cleaned
                and in_schedule_mode
                and (dev.preheat is None or "preheat" in drifts)
            ):
                kw["preheat"] = cleaned["preheat"]
            return encode_z2m_set_command(**kw) if kw else {}

        def _head_matches(cur: DanfossDevice) -> bool:
            if "child_lock" in cleaned and cur.child_lock is not cleaned["child_lock"]:
                return False
            if (
                "window_detection" in cleaned
                and cur.window_detection is not cleaned["window_detection"]
            ):
                return False
            if "min_setpoint" in cleaned and (
                cur.min_setpoint is None or abs(cur.min_setpoint - cleaned["min_setpoint"]) > 0.1
            ):
                return False
            if "max_setpoint" in cleaned and (
                cur.max_setpoint is None or abs(cur.max_setpoint - cleaned["max_setpoint"]) > 0.1
            ):
                return False
            return not (
                "preheat" in cleaned and in_schedule_mode and cur.preheat is not cleaned["preheat"]
            )

        heads: list[HeadWriteResult] = []
        for dev in present:
            cmd = _head_payload(dev)
            if not cmd:
                # §5.8: nothing sent when the reported value already matches.
                heads.append(
                    HeadWriteResult(address=dev.id, friendly=dev.name, status=HEAD_APPLIED)
                )
                continue

            def _write_one(d: DanfossDevice, payload_cmd: dict[str, Any] = cmd) -> None:
                self._publish_payload(d, payload_cmd)

            heads.extend(
                run_head_write_loop(
                    [dev],
                    write_fn=_write_one,
                    confirm_fn=lambda d: self._wait_confirm(
                        d,
                        _head_matches,
                        lambda cur: "reported head settings do not match target",
                    ),
                    retries=self._setpoint_retries,
                )
            )
        for addr in absent:
            heads.append(
                HeadWriteResult(
                    address=addr,
                    friendly=addr,
                    status=HEAD_UNREACHABLE,
                    detail="member not reported by the bridge",
                )
            )
        reply = {
            "status": "ok",
            "job_status": _job_status(heads),
            "room_id": room.room_id,
            "settings": cleaned,
            "heads": [h.to_dict() for h in heads],
        }
        self._respond(_SETTINGS_ACTION, reply, transaction)
        self.republish()

    # --- Stage 7: holidays ---------------------------------------------------------------------

    def _reconcile_holidays(self, wall_now: datetime) -> bool:
        """Apply missed or current start/end holiday edges to armed rooms (§5.4).

        Returns ``True`` when any room or holiday state changed.
        """
        changed = False
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []

        with self._lock:
            for hol in self._store.holidays():
                try:
                    start_dt = parse_iso_datetime(hol.start, default_tz=wall_now.tzinfo)
                    end_dt = parse_iso_datetime(hol.end, default_tz=wall_now.tzinfo)
                except HolidayValidationError:
                    continue

                if start_dt <= wall_now < end_dt and not hol.active:
                    saved = dict(hol.saved_modes)
                    any_armed = False
                    for rid in hol.room_ids:
                        if rid not in self._armed_rooms:
                            continue
                        try:
                            room = self._store.get(rid)
                        except RoomStoreError:
                            continue
                        members = self._members_of(room.members)
                        if room.members and not members:
                            # Wait for bridge/devices to report members before flipping active.
                            continue
                        any_armed = True
                        saved.setdefault(rid, str(room.desired.get("mode") or "schedule"))
                        self._store.update_desired(
                            rid, {"mode": "holiday", "setpoint": hol.temperature}
                        )
                        self._reconcile.reset_room(rid)
                        for dev in members:
                            if dev.online:
                                payload = encode_z2m_set_command(
                                    setpoint=hol.temperature, setpoint_scheduled=True
                                )
                                payload["programming_operation_mode"] = "setpoint"
                                writes.append((dev, payload))
                    if any_armed:
                        self._store.update_holiday_state(
                            hol.holiday_id, active=True, saved_modes=saved
                        )
                        changed = True

                elif wall_now >= end_dt and hol.active:
                    pending_devices = False
                    for rid in hol.room_ids:
                        if rid not in self._armed_rooms:
                            continue
                        try:
                            room = self._store.get(rid)
                        except RoomStoreError:
                            continue
                        if room.members and not self._members_of(room.members):
                            pending_devices = True
                            continue
                        writes.extend(self._build_holiday_exit_writes_locked(rid))
                    if not pending_devices:
                        self._store.update_holiday_state(hol.holiday_id, active=False)
                        changed = True

        self._publish_writes(writes)
        return changed

    def _build_holiday_exit_writes_locked(
        self, room_id: str
    ) -> list[tuple[DanfossDevice, dict[str, Any]]]:
        """Return writes to return ``room_id`` back to ``schedule`` after a holiday ends."""
        try:
            room = self._store.get(room_id)
        except RoomStoreError:
            return []
        preheat = bool(room.desired.get("preheat", False))
        self._store.update_desired(room_id, {"mode": "schedule"}, remove_keys=("setpoint",))
        self._reconcile.reset_room(room_id)

        day_payloads: list[dict[str, Any]] = []
        if not self._mode_change_keeps_schedule and isinstance(room.schedule, Mapping):
            days = room.schedule.get("days")
            if isinstance(days, Mapping):
                sched = room.schedule
                c = float(sched.get("comfort", room.desired.get("comfort", _DEFAULT_COMFORT_C)))
                s = float(sched.get("setback", room.desired.get("setback", _DEFAULT_SETBACK_C)))
                with contextlib.suppress(ScheduleWriteValidationError):
                    _, day_payloads = build_z2m_schedule_payloads(days, c, s)

        prog_mode = "schedule_with_preheat" if preheat else "schedule"
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        for dev in self._members_of(room.members):
            if not dev.online:
                continue
            for dp in day_payloads:
                writes.append((dev, dp))
            writes.append((dev, {"programming_operation_mode": prog_mode}))
        return writes

    # --- store-only & holiday requests ---------------------------------------------------------

    def _handle_request(self, action: str, raw: str | bytes) -> dict[str, Any]:
        try:
            payload = json.loads(_as_text(raw)) if raw else {}
            if not isinstance(payload, dict):
                raise RoomStoreError("request payload must be a JSON object")
        except (ValueError, UnicodeDecodeError) as err:
            return {"status": "error", "error": f"invalid JSON: {err}"}

        transaction = payload.get("transaction")
        try:
            data = self._apply_request(action, payload)
        except (RoomStoreError, HolidayValidationError) as err:
            reply: dict[str, Any] = {"status": "error", "error": str(err)}
        else:
            reply = {"status": "ok", **data}
            if (
                action == "refresh"
                or action in _ROOM_STORE_ACTIONS
                or action.startswith("holiday/")
            ):
                self.republish()
        if transaction is not None:
            reply["transaction"] = transaction
        return reply

    def _apply_request(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "refresh":
            return {"refreshed": True}
        with self._lock:
            if action == "room/create":
                room = self._store.create(
                    name=str(payload.get("name") or ""),
                    prefix=str(payload.get("prefix") or self._source),
                    members=payload.get("members") or [],
                    sensor=payload.get("sensor"),
                    home_id=payload.get("home_id"),
                    room_id=payload.get("room_id"),
                )
                return {"room": room.to_dict()}
            if action == "room/rename":
                room = self._store.rename(_extract_room_id(payload), str(payload.get("name") or ""))
                return {"room": room.to_dict()}
            if action == "room/set-members":
                room = self._store.set_members(
                    _extract_room_id(payload), payload.get("members") or []
                )
                return {"room": room.to_dict()}
            if action == "room/set-sensor":
                room = self._store.set_sensor(_extract_room_id(payload), payload.get("sensor"))
                return {"room": room.to_dict()}
            if action == "room/delete":
                rid = _extract_room_id(payload)
                self._store.delete(rid)
                self._telemetry.drop_room(rid)
                self._reconcile.reset_room(rid)
                self._windows.drop_room(rid)
                return {"room_id": rid}
            if action == "holiday/list":
                return {"holidays": [h.to_dict() for h in self._store.holidays()]}
            if action == "holiday/set":
                return self._apply_holiday_set(payload)
            if action == "holiday/cancel":
                return self._apply_holiday_cancel(payload)
        raise RoomStoreError(f"unsupported request {action!r}")

    def _apply_holiday_set(self, payload: dict[str, Any]) -> dict[str, Any]:
        wall = self._wall_now()
        known = {r.room_id for r in self._store.rooms()}
        start_s, end_s, temp, room_ids = validate_holiday(
            start=payload.get("start"),
            end=payload.get("end"),
            temperature=payload.get("temperature"),
            room_ids=payload.get("room_ids", payload.get("rooms")),
            known_room_ids=known,
            default_tz=wall.tzinfo,
        )
        hol: CoordinatorHoliday = self._store.upsert_holiday(
            start=start_s,
            end=end_s,
            temperature=temp,
            room_ids=room_ids,
            name=str(payload.get("name") or "Holiday"),
            holiday_id=payload.get("holiday_id"),
        )
        if self._enable_holidays:
            self._reconcile_holidays(wall)
            hol = self._store.get_holiday(hol.holiday_id)
        return {"holiday": hol.to_dict()}

    def _apply_holiday_cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        hid = str(payload.get("holiday_id") or "").strip()
        if not hid:
            raise RoomStoreError("holiday_id is required")
        hol = self._store.get_holiday(hid)
        writes: list[tuple[DanfossDevice, dict[str, Any]]] = []
        if hol.active and self._enable_holidays:
            for rid in hol.room_ids:
                if rid in self._armed_rooms:
                    writes.extend(self._build_holiday_exit_writes_locked(rid))
        self._store.delete_holiday(hid)
        if writes:
            self._publish_writes(writes)
        return {"holiday_id": hid, "cancelled": True}


def _parse_online(text: str) -> bool | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if raw in ("online", "offline"):
        return raw == "online"
    with contextlib.suppress(ValueError):
        obj = json.loads(raw)
        if isinstance(obj, dict) and "state" in obj:
            return str(obj["state"]).lower() == "online"
    return None


# --- CLI / configuration -----------------------------------------------------------------------


@dataclass
class CoordinatorConfig:
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: bool = False
    source_prefix: str = DEFAULT_SOURCE_PREFIX
    base_topic: str = DEFAULT_BASE_TOPIC
    store_path: str = "rooms.json"
    heat_demand_valid_s: int = DEFAULT_HEAT_DEMAND_VALID_S
    # Master allowlist, CLOSED BY DEFAULT (§7): a room is written only when its id is listed here.
    armed_rooms: tuple[str, ...] = field(default_factory=tuple)
    setpoint_timeout_s: float = DEFAULT_SETPOINT_TIMEOUT_S
    setpoint_retries: int = DEFAULT_SETPOINT_RETRIES
    # Stages 3–7 feature gates, all CLOSED BY DEFAULT.
    feed_external_sensor: bool = False
    mode_change_keeps_schedule: bool = False
    reconcile_drift: bool = False
    feed_load_mean: bool = False
    enable_holidays: bool = False
    propagate_window_open: bool = False
    window_open_max_duration_s: float = WINDOW_OPEN_MAX_DURATION_S
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    max_writes_per_tick: int = 0


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off", ""):
        return False
    return default


def load_coordinator_config(
    args: argparse.Namespace,
    options_path: str | Path | None = None,
) -> CoordinatorConfig:
    """Build :class:`CoordinatorConfig` from an add-on ``options.json``, env vars and CLI args."""
    opts: dict[str, Any] = {}
    path = options_path or getattr(args, "options", None)
    if path:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            opts = raw

    def _pick(arg_name: str, opt_key: str, env_key: str, default: Any) -> Any:
        cli_val = getattr(args, arg_name, None)
        if cli_val is not None:
            return cli_val
        if opts.get(opt_key) not in (None, ""):
            return opts[opt_key]
        env_val = os.environ.get(env_key)
        if env_val not in (None, ""):
            return env_val
        return default

    return CoordinatorConfig(
        mqtt_host=str(_pick("mqtt_host", "mqtt_host", "DANFOSS_MQTT_HOST", "localhost")),
        mqtt_port=int(_pick("mqtt_port", "mqtt_port", "DANFOSS_MQTT_PORT", 1883)),
        mqtt_username=_pick("mqtt_username", "mqtt_username", "DANFOSS_MQTT_USERNAME", None),
        mqtt_password=_pick("mqtt_password", "mqtt_password", "DANFOSS_MQTT_PASSWORD", None),
        mqtt_tls=_parse_bool(_pick("mqtt_tls", "mqtt_tls", "DANFOSS_MQTT_TLS", False)),
        source_prefix=str(
            _pick("source_prefix", "source_prefix", "DANFOSS_SOURCE_PREFIX", DEFAULT_SOURCE_PREFIX)
        ),
        base_topic=str(
            _pick("base_topic", "base_topic", "DANFOSS_ROOMS_BASE_TOPIC", DEFAULT_BASE_TOPIC)
        ),
        store_path=str(_pick("store_path", "store_path", "DANFOSS_ROOMS_STORE", "rooms.json")),
        heat_demand_valid_s=int(
            _pick(
                "heat_demand_valid_s",
                "heat_demand_valid_s",
                "DANFOSS_HEAT_DEMAND_VALID_S",
                DEFAULT_HEAT_DEMAND_VALID_S,
            )
        ),
        armed_rooms=_parse_armed_rooms(
            _pick("armed_rooms", "armed_rooms", "DANFOSS_ARMED_ROOMS", ())
        ),
        setpoint_timeout_s=float(
            _pick(
                "setpoint_timeout_s",
                "setpoint_timeout_s",
                "DANFOSS_SETPOINT_TIMEOUT_S",
                DEFAULT_SETPOINT_TIMEOUT_S,
            )
        ),
        setpoint_retries=int(
            _pick(
                "setpoint_retries",
                "setpoint_retries",
                "DANFOSS_SETPOINT_RETRIES",
                DEFAULT_SETPOINT_RETRIES,
            )
        ),
        feed_external_sensor=_parse_bool(
            _pick(
                "feed_external_sensor",
                "feed_external_sensor",
                "DANFOSS_FEED_EXTERNAL_SENSOR",
                False,
            )
        ),
        mode_change_keeps_schedule=_parse_bool(
            _pick(
                "mode_change_keeps_schedule",
                "mode_change_keeps_schedule",
                "DANFOSS_MODE_CHANGE_KEEPS_SCHEDULE",
                False,
            )
        ),
        reconcile_drift=_parse_bool(
            _pick("reconcile_drift", "reconcile_drift", "DANFOSS_RECONCILE_DRIFT", False)
        ),
        feed_load_mean=_parse_bool(
            _pick("feed_load_mean", "feed_load_mean", "DANFOSS_FEED_LOAD_MEAN", False)
        ),
        enable_holidays=_parse_bool(
            _pick("enable_holidays", "enable_holidays", "DANFOSS_ENABLE_HOLIDAYS", False)
        ),
        propagate_window_open=_parse_bool(
            _pick(
                "propagate_window_open",
                "propagate_window_open",
                "DANFOSS_PROPAGATE_WINDOW_OPEN",
                False,
            )
        ),
        window_open_max_duration_s=float(
            _pick(
                "window_open_max_duration_s",
                "window_open_max_duration_s",
                "DANFOSS_WINDOW_OPEN_MAX_DURATION_S",
                WINDOW_OPEN_MAX_DURATION_S,
            )
        ),
        tick_interval_s=float(
            _pick(
                "tick_interval_s",
                "tick_interval_s",
                "DANFOSS_TICK_INTERVAL_S",
                DEFAULT_TICK_INTERVAL_S,
            )
        ),
        max_writes_per_tick=int(
            _pick("max_writes_per_tick", "max_writes_per_tick", "DANFOSS_MAX_WRITES_PER_TICK", 0)
        ),
    )


def _parse_armed_rooms(value: Any) -> tuple[str, ...]:
    """Normalize the armed-rooms allowlist from a list (option/CLI) or a comma string (env var)."""
    if value in (None, ""):
        return ()
    items = value if isinstance(value, (list, tuple)) else str(value).split(",")
    return tuple(r.strip() for r in items if str(r).strip())


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="danfoss-room-coordinator",
        description="Standalone room coordinator for Zigbee2MQTT Danfoss heads.",
    )
    parser.add_argument("--options", default=os.environ.get("DANFOSS_OPTIONS_FILE"))
    parser.add_argument("--mqtt-host", default=None)
    parser.add_argument("--mqtt-port", type=int, default=None)
    parser.add_argument("--mqtt-username", default=None)
    parser.add_argument("--mqtt-password", default=None)
    parser.add_argument("--mqtt-tls", action="store_true", default=None)
    parser.add_argument("--source-prefix", dest="source_prefix", default=None)
    parser.add_argument("--base-topic", dest="base_topic", default=None)
    parser.add_argument("--store-path", dest="store_path", default=None)
    parser.add_argument("--heat-demand-valid-s", dest="heat_demand_valid_s", type=int, default=None)
    parser.add_argument(
        "--armed-room",
        dest="armed_rooms",
        action="append",
        default=None,
        metavar="ROOM_ID",
        help="arm a room for head writes (repeatable; closed by default)",
    )
    parser.add_argument("--setpoint-timeout-s", dest="setpoint_timeout_s", type=float, default=None)
    parser.add_argument("--setpoint-retries", dest="setpoint_retries", type=int, default=None)
    parser.add_argument(
        "--feed-external-sensor", dest="feed_external_sensor", action="store_true", default=None
    )
    parser.add_argument(
        "--mode-change-keeps-schedule",
        dest="mode_change_keeps_schedule",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--reconcile-drift", dest="reconcile_drift", action="store_true", default=None
    )
    parser.add_argument(
        "--feed-load-mean", dest="feed_load_mean", action="store_true", default=None
    )
    parser.add_argument(
        "--enable-holidays", dest="enable_holidays", action="store_true", default=None
    )
    parser.add_argument(
        "--propagate-window-open", dest="propagate_window_open", action="store_true", default=None
    )
    parser.add_argument(
        "--window-open-max-duration-s",
        dest="window_open_max_duration_s",
        type=float,
        default=None,
    )
    parser.add_argument("--tick-interval-s", dest="tick_interval_s", type=float, default=None)
    parser.add_argument("--max-writes-per-tick", dest="max_writes_per_tick", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def run_coordinator(
    config: CoordinatorConfig,
    *,
    transport: MqttTransport | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """Connect, run the coordinator, and block until ``stop_event`` is set (or SIGINT/SIGTERM)."""
    own_transport = transport is None
    if transport is None:
        paho = PahoMqttTransport(
            host=config.mqtt_host,
            port=config.mqtt_port,
            base_topic=config.base_topic,
            username=config.mqtt_username,
            password=config.mqtt_password,
            tls=config.mqtt_tls,
            client_id="danfoss-room-coordinator",
        )
        paho.connect()
        transport = paho

    store = RoomStore(config.store_path).load()
    coordinator = RoomCoordinator(
        transport,
        store,
        source_prefix=config.source_prefix,
        base_topic=config.base_topic,
        heat_demand_valid_s=config.heat_demand_valid_s,
        armed_rooms=config.armed_rooms,
        setpoint_timeout_s=config.setpoint_timeout_s,
        setpoint_retries=config.setpoint_retries,
        feed_external_sensor=config.feed_external_sensor,
        mode_change_keeps_schedule=config.mode_change_keeps_schedule,
        reconcile_drift=config.reconcile_drift,
        feed_load_mean=config.feed_load_mean,
        enable_holidays=config.enable_holidays,
        propagate_window_open=config.propagate_window_open,
        window_open_max_duration_s=config.window_open_max_duration_s,
        tick_interval_s=config.tick_interval_s,
        max_writes_per_tick=config.max_writes_per_tick,
    )
    coordinator.start()

    stop = stop_event or threading.Event()
    try:
        stop.wait()
    finally:
        coordinator.stop()
        if own_transport and isinstance(transport, PahoMqttTransport):
            transport.disconnect()


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``danfoss-room-coordinator``."""
    logging.basicConfig(level=logging.INFO)
    args = build_arg_parser().parse_args(argv)
    config = load_coordinator_config(args)
    _LOGGER.info(
        "room coordinator: source %s -> base %s on %s:%d (store %s)",
        config.source_prefix,
        config.base_topic,
        config.mqtt_host,
        config.mqtt_port,
        config.store_path,
    )
    if config.armed_rooms:
        _LOGGER.warning(
            "writes ARMED for room(s) %s "
            "(sensor=%s, reconcile=%s, load=%s, holidays=%s, window=%s)",
            ", ".join(config.armed_rooms),
            config.feed_external_sensor,
            config.reconcile_drift,
            config.feed_load_mean,
            config.enable_holidays,
            config.propagate_window_open,
        )
    else:
        _LOGGER.info("all head writes closed (no armed rooms): read-only, Stage 1 behaviour")
    if args.dry_run:
        return 0

    stop_event = threading.Event()

    def _request_stop(*_a: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, _request_stop)

    run_coordinator(config, stop_event=stop_event)
    return 0
