"""Standalone room coordinator daemon (``docs/z2m-cloud-parity.md`` §6, Stage 1 — read only).

A sibling of ``danfoss2mqtt`` for heads paired to a **Zigbee2MQTT** coordinator with no Danfoss
gateway and no cloud. It subscribes to the Zigbee2MQTT prefix (read only), keeps its own room store
(:mod:`pydanfoss_cloud.room_store`), and publishes **under its own prefix** — never inside
Zigbee2MQTT's namespace:

- ``<base>/bridge/state`` (retained, last-will ``offline``),
- ``<base>/bridge/rooms`` (retained): the aggregated room objects (§5.1),
- ``<base>/bridge/heat_demand`` (retained): rooms calling for heat (§5.7),
- ``<base>/bridge/request/<action>`` -> ``<base>/bridge/response/<action>``: room store operations.

**Stage 1 is read only with respect to the heads**: the daemon never publishes any
``<prefix>/<name>/set``. A ``room/*`` request edits the room **store** (a room *definition*), and
the aggregates are derived from what the heads **report**. Setpoint fan-out, sensor feed, schedule
and the rest are later stages, each behind its own supervised real-hardware trial.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import signal
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge_daemon import (
    AVAIL_OFFLINE,
    AVAIL_ONLINE,
    MqttTransport,
    PahoMqttTransport,
)
from .decode import DanfossDevice
from .room_store import RoomStore, RoomStoreError, normalize_address
from .room_view import DEFAULT_HEAT_DEMAND_VALID_S, build_heat_demand, build_room_view
from .z2m_contract import decode_z2m_descriptor, decode_z2m_state

_LOGGER = logging.getLogger(__name__)

DEFAULT_SOURCE_PREFIX = "zigbee2mqtt"
DEFAULT_BASE_TOPIC = "danfoss-rooms"

# Room request actions; every one mutates the STORE only, never a head.
_ROOM_ACTIONS = frozenset(
    {"room/create", "room/rename", "room/set-members", "room/set-sensor", "room/delete"}
)


def _as_text(raw: str | bytes) -> str:
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


class RoomCoordinator:
    """Read-only room aggregator over a Zigbee2MQTT prefix, transport-agnostic (testable).

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
        # normalized address -> reported device
        self._fleet: dict[str, DanfossDevice] = {}
        # friendly_name -> normalized address, and back
        self._friendly_to_addr: dict[str, str] = {}
        self._addr_to_friendly: dict[str, str] = {}

    @property
    def base_topic(self) -> str:
        return self._base

    # --- lifecycle -----------------------------------------------------------------------------

    def start(self) -> None:
        """Announce online, subscribe to the source prefix (read only) and to room requests."""
        self._tx.publish(f"{self._base}/bridge/state", AVAIL_ONLINE, retain=True)
        self._tx.subscribe(f"{self._source}/#", self._on_source_message)
        # ``#`` (not ``+``): room actions are two segments, e.g. ``bridge/request/room/create``.
        self._tx.subscribe(f"{self._base}/bridge/request/#", self._on_request)
        self.republish()

    def stop(self) -> None:
        """Clear the heat-demand message and announce offline."""
        self._tx.publish(
            f"{self._base}/bridge/heat_demand",
            json.dumps(build_heat_demand({}, valid_for_s=self._valid)),
            retain=True,
        )
        self._tx.publish(f"{self._base}/bridge/state", AVAIL_OFFLINE, retain=True)

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
            return  # grouped topics or ``/set`` echoes: not a plain device state
        self._ingest_state(suffix, raw)

    def _ingest_devices(self, raw: str | bytes) -> None:
        try:
            entries = json.loads(_as_text(raw))
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(entries, list):
            return

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
            stale_friendly = self._addr_to_friendly.pop(addr, None)
            if stale_friendly is not None:
                self._friendly_to_addr.pop(stale_friendly, None)

        self.republish()

    def _ingest_state(self, friendly: str, raw: str | bytes) -> None:
        addr = self._friendly_to_addr.get(friendly)
        dev = self._fleet.get(addr) if addr else None
        if dev is None:
            return
        try:
            payload = json.loads(_as_text(raw))
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(payload, dict):
            return
        decode_z2m_state(dev, payload)
        self.republish()

    def _ingest_availability(self, friendly: str, raw: str | bytes) -> None:
        addr = self._friendly_to_addr.get(friendly)
        dev = self._fleet.get(addr) if addr else None
        if dev is None:
            return
        online = _parse_online(_as_text(raw))
        if online is not None:
            dev.online = online
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
        """Recompute and publish ``bridge/rooms`` and ``bridge/heat_demand`` from reported state."""
        rooms = self._store.rooms()
        views: list[dict[str, Any]] = []
        heat_members: dict[str, list[DanfossDevice]] = {}
        names: dict[str, str] = {}
        for room in rooms:
            members = self._members_of(room.members)
            sensor = self._fleet.get(normalize_address(room.sensor)) if room.sensor else None
            views.append(build_room_view(room, members, sensor))
            heat_members[room.room_id] = members
            names[room.room_id] = room.name

        self._tx.publish(f"{self._base}/bridge/rooms", json.dumps(views), retain=True)
        self._tx.publish(
            f"{self._base}/bridge/heat_demand",
            json.dumps(build_heat_demand(heat_members, names, valid_for_s=self._valid)),
            retain=True,
        )

    # --- room requests (store only) -------------------------------------------------------------

    def _on_request(self, topic: str, raw: str | bytes) -> None:
        prefix = f"{self._base}/bridge/request/"
        if not topic.startswith(prefix):
            return
        action = topic[len(prefix) :]
        reply = self._handle_request(action, raw)
        self._tx.publish(f"{self._base}/bridge/response/{action}", json.dumps(reply), retain=False)

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
        except RoomStoreError as err:
            reply: dict[str, Any] = {"status": "error", "error": str(err)}
        else:
            reply = {"status": "ok", **data}
            if action == "refresh" or action in _ROOM_ACTIONS:
                self.republish()
        if transaction is not None:
            reply["transaction"] = transaction
        return reply

    def _apply_request(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "refresh":
            return {"refreshed": True}
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
            room = self._store.rename(self._room_id(payload), str(payload.get("name") or ""))
            return {"room": room.to_dict()}
        if action == "room/set-members":
            room = self._store.set_members(self._room_id(payload), payload.get("members") or [])
            return {"room": room.to_dict()}
        if action == "room/set-sensor":
            room = self._store.set_sensor(self._room_id(payload), payload.get("sensor"))
            return {"room": room.to_dict()}
        if action == "room/delete":
            rid = self._room_id(payload)
            self._store.delete(rid)
            return {"room_id": rid}
        raise RoomStoreError(f"unsupported request {action!r}")

    @staticmethod
    def _room_id(payload: dict[str, Any]) -> str:
        rid = str(payload.get("room_id") or "").strip()
        if not rid:
            raise RoomStoreError("room_id is required")
        return rid


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
        mqtt_tls=bool(_pick("mqtt_tls", "mqtt_tls", "DANFOSS_MQTT_TLS", False)),
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
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="danfoss-room-coordinator",
        description="Standalone read-only room coordinator for Zigbee2MQTT Danfoss heads.",
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
