"""Local tunnel manager: survey and write of the preheat state (attribute 0x0025).

Isolates the blocking I/O (tinytuya): the HA layer calls it in the executor. One thread per gateway,
best-effort (an unreachable head -> None, never an exception propagated to the global survey). The
link factory is injectable (``open_link``) for tests (a networkless double).
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..keys import GatewayKey
from . import tunnel

_LOGGER = logging.getLogger(__name__)

# Upper bound of a preheat survey: a fixed margin plus the worst case of one head (two 12 s
# attempts, ``tunnel.request``) for the busiest gateway. Past it the survey is stopped and the heads
# not read yet are simply absent from the result (their switch stays unavailable).
SURVEY_BASE_TIMEOUT = 40.0
SURVEY_HEAD_TIMEOUT = 30.0
# Settings survey: two requests for a head that answers (``tunnel.read_settings``).
SETTINGS_HEAD_TIMEOUT = 60.0


@dataclass(frozen=True)
class Head:
    """A head addressable through the tunnel: its devId, its Zigbee cid (node_id) and gateway."""

    dev_id: str
    node_id: str
    gateway_id: str


class LocalTunnelManager:
    """Opens tinytuya links to the gateways to read/write the real preheat state."""

    def __init__(
        self,
        gateways: dict[str, GatewayKey],
        open_link: Callable[[GatewayKey], Any] = tunnel.LocalLink,
    ) -> None:
        self._gateways = gateways
        self._open_link = open_link

    @property
    def gateway_ids(self) -> set[str]:
        return set(self._gateways)

    def _survey(
        self,
        heads: list[Head],
        read: Callable[[Any, Head], Any],
        on_read: Callable[[str, Any], None],
        stop: threading.Event | None,
        head_timeout: float,
    ) -> dict[str, Any]:
        """Read each head with ``read(link, head)``, one gateway per thread. Best-effort.

        ``on_read(dev_id, value)`` is called from the worker thread as soon as a head is read
        (None: no answer, or its gateway is unreachable). ``stop`` aborts the survey: it returns
        at once with what was read so far, and each worker gives up after the head it is on (a
        tunnel request is not interruptible). The survey also stops by itself past a deadline of
        ``SURVEY_BASE_TIMEOUT`` plus ``head_timeout`` per head of the busiest gateway. No worker
        outlives the call for longer than one request: nothing is written into the returned
        mapping afterwards.
        """
        stop = stop if stop is not None else threading.Event()
        res: dict[str, Any] = {}
        lock = threading.Lock()
        by_gw: dict[str, list[Head]] = defaultdict(list)
        for h in heads:
            if h.gateway_id in self._gateways:
                by_gw[h.gateway_id].append(h)

        def publish(dev_id: str, value: Any) -> None:
            with lock:
                if stop.is_set():
                    return
                res[dev_id] = value
            with contextlib.suppress(Exception):  # a listener never breaks the survey
                on_read(dev_id, value)

        def worker(gw_id: str, mine: list[Head]) -> None:
            try:
                link = self._open_link(self._gateways[gw_id])
            except Exception as err:  # noqa: BLE001 - gateway unreachable: heads = None
                _LOGGER.debug("local tunnel: gateway %s unreachable (%s)", gw_id, err)
                for h in mine:
                    publish(h.dev_id, None)
                return
            try:
                for h in mine:
                    if stop.is_set():
                        return
                    try:
                        value = read(link, h)
                    except Exception:  # noqa: BLE001 - isolate each head
                        value = None
                    publish(h.dev_id, value)
            finally:
                link.close()

        threads = [
            threading.Thread(target=worker, args=(g, m), daemon=True) for g, m in by_gw.items()
        ]
        busiest = max((len(m) for m in by_gw.values()), default=0)
        deadline = time.monotonic() + SURVEY_BASE_TIMEOUT + head_timeout * busiest
        for t in threads:
            t.start()
        for t in threads:
            while t.is_alive() and not stop.is_set() and time.monotonic() < deadline:
                t.join(timeout=0.2)
        with lock:
            stop.set()  # timed out or aborted: late workers stop and publish nothing more
            return dict(res)

    def survey_preheat(
        self,
        heads: list[Head],
        on_result: Callable[[str, bool | None], None] | None = None,
        stop: threading.Event | None = None,
        on_attributes: Callable[[str, dict[str, Any] | None], None] | None = None,
    ) -> dict[str, bool | None]:
        """Read preheat (0x0025 bit 1) per head, one gateway per thread. Best-effort.

        Slow by nature: the heads of a gateway are read one after the other (a few seconds each when
        they answer, up to 24 s when they do not). ``on_result(dev_id, value)`` is called from the
        worker thread as soon as a head is read, so the caller can publish it without waiting for
        the whole site; ``on_attributes(dev_id, attributes)`` is called just before it with the
        standard attributes the same frame carried (``tunnel.read_standard``), None for a head
        that did not answer. ``stop`` and the deadline: see ``_survey``.
        """

        def preheat(attrs: dict[str, Any] | None) -> bool | None:
            return None if attrs is None else attrs.get("preheat")

        def on_read(dev_id: str, attrs: dict[str, Any] | None) -> None:
            if on_attributes is not None:
                with contextlib.suppress(Exception):
                    on_attributes(dev_id, attrs)
            if on_result is not None:
                on_result(dev_id, preheat(attrs))

        raw = self._survey(
            heads,
            lambda link, h: tunnel.read_standard(link, h.dev_id, h.node_id),
            on_read,
            stop,
            SURVEY_HEAD_TIMEOUT,
        )
        return {dev_id: preheat(attrs) for dev_id, attrs in raw.items()}

    def survey_settings(
        self,
        heads: list[Head],
        on_result: Callable[[str, dict[str, Any] | None], None] | None = None,
        stop: threading.Event | None = None,
    ) -> dict[str, dict[str, Any] | None]:
        """Read the Danfoss settings attributes per head (``tunnel.read_settings``). Best-effort.

        Two frames per head that answers, one for a head that does not: heavier than the preheat
        survey, meant to run rarely. Same threading, ``on_result``, ``stop`` and deadline rules
        as ``survey_preheat``; None for a head that did not answer.
        """
        return self._survey(
            heads,
            lambda link, h: tunnel.read_settings(link, h.dev_id, h.node_id),
            on_result if on_result is not None else (lambda dev_id, attrs: None),
            stop,
            SETTINGS_HEAD_TIMEOUT,
        )

    def survey_heads(self, heads: list[Head]) -> dict[str, dict[str, Any]]:
        """Full read-only survey of each head through the tunnel, summarized per head.

        One gateway per thread, best-effort (an unreachable head -> ``{"unreachable": True}``,
        never an exception propagated). Returns a mapping ``dev_id -> summary`` (comfort slots,
        comfort/setback setpoints, decoded attributes); see ``tunnel.summarize_head``.
        """
        raw = tunnel.survey(self._gateways, heads)
        return {dev_id: tunnel.summarize_head(entry) for dev_id, entry in raw.items()}

    def read_preheat(self, head: Head) -> bool | None:
        """Read ONE head's real preheat (0x0025 bit 1) through the tunnel. None without a response.

        Used right after a preheat write to reflect the head's actual state without waiting for the
        next full survey (~8 s per head, minutes for a whole site). Raises LocalUnavailable on an
        unknown gateway.
        """
        attrs = self.read_standard(head)
        return None if attrs is None else attrs.get("preheat")

    def read_standard(self, head: Head) -> dict[str, Any] | None:
        """Read ONE head's standard attributes (one frame, ``tunnel.read_standard``).

        None without a response. Raises LocalUnavailable on an unknown gateway.
        """
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.read_standard(link, head.dev_id, head.node_id)
        finally:
            link.close()

    def read_settings(self, head: Head) -> dict[str, Any] | None:
        """Read ONE head's Danfoss settings attributes (``tunnel.read_settings``: two frames, one
        for a head that does not answer). None without a response. Raises LocalUnavailable on an
        unknown gateway. Used to read a setting back after a write (orientation, 0x4014)."""
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.read_settings(link, head.dev_id, head.node_id)
        finally:
            link.close()

    def read_programming_mode(
        self, head: Head, *, duration: float = 8, attempts: int = 1
    ) -> int | None:
        """Read ONE head's raw 0x0025 (bit 0 schedule, bit 1 preheat). None without a response.

        A short, single-attempt read by default: it serves as a guard before a schedule write
        (is the head really following a schedule?), where a head that does not answer must not
        hold the call for long. Raises LocalUnavailable on an unknown gateway.
        """
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.read_programming_mode(
                link, head.dev_id, head.node_id, duration=duration, attempts=attempts
            )
        finally:
            link.close()

    def write_preheat(self, head: Head, active: bool) -> bool:
        """Enable/disable preheat (reads 0x0025, writes the bit, schedule preserved).

        True if the head accepts (status 0). Raises LocalError/LocalUnavailable otherwise.
        """
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            mode = tunnel.read_programming_mode(link, head.dev_id, head.node_id)
            if mode is None:
                raise tunnel.LocalError("attribute 0x0025 unreadable (head unreachable)")
            status = tunnel.write_preheat(link, head.dev_id, head.node_id, active, mode)
            return status == 0
        finally:
            link.close()

    def set_override(self, head: Head, celsius: float) -> int:
        """One-off "now" setpoint override via DP 171 (contract §4.4). Returns the published raw
        value (hundredths of °C). Raises LocalError (value out of range) / LocalUnavailable."""
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.write_setpoint(link, head.dev_id, head.node_id, celsius)
        finally:
            link.close()

    def read_schedule(self, head: Head) -> dict[str, list[dict[str, Any]]]:
        """Read the head's 7-day weekly schedule through the tunnel (read-only).

        Returns ``{weekday: [{"h": "HH:MM", "c": setpoint}]}``. Raises LocalUnavailable on an
        unknown gateway."""
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.read_schedule(link, head.dev_id, head.node_id)
        finally:
            link.close()

    def write_window_detection(self, head: Head, enabled: bool) -> Any:
        """Write open-window detection (DP 116) through the local tunnel. Returns the published
        value. Raises LocalError/LocalUnavailable. The authoritative read is attribute 0x4051
        (``tunnel.read_window_detection`` / the local survey)."""
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.write_setting_dp(
                link, head.dev_id, head.node_id, "window_detection", bool(enabled)
            )
        finally:
            link.close()

    def write_setting(self, head: Head, setting: str, value: Any) -> int | None:
        """Write ONE Danfoss setting of ``tunnel.TUNNEL_SETTINGS`` (radiator covered, regulation
        speed, setpoint offset). The value is validated before anything is sent.

        Returns the head's status (0 = accepted), None without a response: the caller reads the
        head back either way. Raises LocalError (invalid value) / LocalUnavailable.
        """
        records = tunnel.setting_records(setting, value)  # validate before opening the link
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            return tunnel.write_attributes(
                link, head.dev_id, head.node_id, records, manufacturer=True
            )
        finally:
            link.close()

    def write_valve_exercise(
        self, head: Head, day: str | None = None, hour: str | None = None
    ) -> tuple[int | None, str, str]:
        """Change the valve exercise day OR time (0x4010 / 0x4011), keeping the other one.

        The two attributes are written together (``tunnel.write_descale``, the descale service's
        path), so the one not given is first read from the head itself on the same link, never
        taken from a cached value. Returns ``(status, day, hour)`` as written; status 0 = accepted,
        None without a response. Raises LocalError when the head does not answer the first read,
        when its day is undefined and only the time is given, or on an invalid value.
        """
        if (day is None) == (hour is None):
            raise tunnel.LocalError("give the valve exercise day or its time, one at a time")
        if day is not None:
            tunnel.descale_records(day, "00:00")  # validate before opening the link
        if hour is not None:
            tunnel.minutes(hour)
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            current = tunnel.read_valve_exercise(link, head.dev_id, head.node_id)
            if current is None or not {"descale_day", "descale_time"} <= current.keys():
                raise tunnel.LocalError("valve exercise unreadable (no answer, or no value)")
            new_day = day if day is not None else str(current["descale_day"])
            new_hour = hour if hour is not None else str(current["descale_time"])
            if new_day not in tunnel.DESCALE_DAYS[:7]:
                raise tunnel.LocalError(
                    f"the head's valve exercise day is {new_day!r}: set the day first"
                )
            status = tunnel.write_descale(link, head.dev_id, head.node_id, new_day, new_hour)
            m = tunnel.minutes(new_hour) % 1440  # as encoded by ``tunnel.descale_records``
            return status, new_day, f"{m // 60:02d}:{m % 60:02d}"
        finally:
            link.close()

    def descale(self, head: Head, day: str, hour: str) -> bool:
        """Set the weekly valve descaling day/time (0x4010/0x4011).

        ``day`` = a tunnel weekday name ("monday"..."sunday"); ``hour`` = "HH:MM".
        True if the head accepts (status 0). Raises LocalError (bad day/no response) or
        LocalUnavailable (unknown gateway).
        """
        if head.gateway_id not in self._gateways:
            raise tunnel.LocalUnavailable(f"unknown gateway: {head.gateway_id}")
        link = self._open_link(self._gateways[head.gateway_id])
        try:
            status = tunnel.write_descale(link, head.dev_id, head.node_id, day, hour)
            return status == 0
        finally:
            link.close()
