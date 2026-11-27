"""Standalone ``danfoss2mqtt`` bridge daemon & CLI (Batch 2.6 + Step 3).

Zero Home Assistant dependency: connects ``pydanfoss_cloud`` (``DanfossDevice`` fleet + rooms) to a
standard Mosquitto MQTT broker (via ``paho-mqtt``) and speaks the Zigbee2MQTT dialect defined in
``api/z2m_contract.py``.

Can run as:
- a standalone CLI process (``danfoss2mqtt`` console script in ``pyproject.toml``),
- a Docker container / Home Assistant Add-on (``addons/danfoss2mqtt/``) alongside ``ebusd`` and
  ``zigbee2mqtt``,
- or embedded in tests using an injected ``MqttTransport`` double (no network).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import secrets
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .client import (
    CloudClient,
    CloudError,
    CloudWriteError,
    ScheduleValidationError,
    UnknownTargetError,
)
from .cloud_dp199 import HeadOverlay, decode_dp199
from .const import (
    MODE_GROUP_SETPOINT_DP,
    SETPOINT_MAX,
    SETPOINT_MIN,
    SETPOINT_STEP,
    WRITE_VERIFIED_PRODUCTS,
)
from .decode import DanfossDevice
from .keys import async_fetch_gateway_keys
from .mqtt_client import (
    CloudMqttLink,
    GatewayRef,
    MqttCredentials,
    _new_paho_client,
    build_credentials,
)
from .rooms import RoomState, aggregate_room
from .writes import encode_setpoint
from .z2m_contract import clean_friendly_name, encode_z2m_descriptor, encode_z2m_state

_LOGGER = logging.getLogger(__name__)

AVAIL_ONLINE = "online"
AVAIL_OFFLINE = "offline"
DEFAULT_POLL_INTERVAL = 60


class MqttTransport(Protocol):
    """Minimal MQTT client interface (implemented by paho-mqtt wrapper or test double)."""

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None: ...

    def subscribe(self, topic: str, callback: Callable[[str, str], None]) -> None: ...


@dataclass(frozen=True)
class BridgeConfig:
    """Runtime configuration for the standalone ``danfoss2mqtt`` bridge."""

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: bool = False
    base_topic: str = "danfoss2mqtt"
    client_id: str = "danfoss2mqtt"
    # Optional Danfoss Cloud credentials (required for live polling, omitted for --dry-run).
    username: str | None = None
    password: str | None = None
    country_code: str = "33"
    region: str = "eu"
    sign_key: str | None = None
    device_id: str | None = None
    ch_key: str | None = None
    poll_interval: int = DEFAULT_POLL_INTERVAL
    # Open a LISTEN-ONLY Tuya cloud-MQTT link so DP 199 pushes feed the heads' read-state overlay
    # (``cloud_dp199``). OFF by default: it opens a second connection to the user's real Tuya
    # account, and the live path is not yet validated on real hardware. Never publishes.
    cloud_mqtt_listen: bool = False

    @property
    def has_cloud_credentials(self) -> bool:
        return bool(self.username and self.password and self.sign_key and self.ch_key)


def ensure_device_id(state_dir: Path) -> str:
    """Return a stable 44-hex ``device_id`` persisted in ``state_dir/device_id`` (0600)."""
    id_file = state_dir / "device_id"
    if id_file.is_file():
        existing = id_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    state_dir.mkdir(parents=True, exist_ok=True)
    generated = secrets.token_hex(22)
    id_file.write_text(f"{generated}\n", encoding="utf-8")
    with contextlib.suppress(OSError):
        id_file.chmod(0o600)
    return generated


def _topic_matches(pattern: str, topic: str) -> bool:
    """Simple MQTT wildcard matcher supporting ``+`` and ``#`` (fallback when paho is mocked)."""
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    for i, part in enumerate(p_parts):
        if part == "#":
            return True
        if i >= len(t_parts):
            return False
        if part != "+" and part != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)


class PahoMqttTransport:
    """``paho-mqtt`` implementation of ``MqttTransport`` for Mosquitto brokers.

    Supports paho-mqtt 1.6.x and 2.x (via ``_new_paho_client``), sets a Last Will & Testament on
    ``<base_topic>/bridge/state`` = ``offline`` before connecting, and automatically re-subscribes
    to all registered topic filters after any broker reconnect.
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        *,
        base_topic: str = "danfoss2mqtt",
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        client_id: str = "danfoss2mqtt",
        paho_module: Any | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._base = base_topic.rstrip("/") or "danfoss2mqtt"
        self._username = username
        self._password = password
        self._tls = tls
        self._client_id = client_id
        self._paho_module = paho_module
        self._client: Any | None = None
        self._connected = False
        self._lock = threading.Lock()
        self._subscriptions: list[tuple[str, Callable[[str, str], None]]] = []

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self, keepalive: int = 60) -> None:
        """Open the MQTT connection and start paho's background network loop."""
        mqtt = self._paho_module
        if mqtt is None:
            import paho.mqtt.client as paho_mqtt

            mqtt = paho_mqtt
            self._paho_module = mqtt

        client = _new_paho_client(mqtt, self._client_id)
        if self._username:
            client.username_pw_set(self._username, self._password)
        if self._tls:
            client.tls_set()
        client.will_set(f"{self._base}/bridge/state", AVAIL_OFFLINE, qos=1, retain=True)
        if hasattr(client, "reconnect_delay_set"):
            client.reconnect_delay_set(min_delay=2, max_delay=60)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self._client = client
        client.connect(self._host, self._port, keepalive=keepalive)
        client.loop_start()

    def disconnect(self) -> None:
        """Stop the background loop and close the MQTT connection cleanly."""
        client = self._client
        if client is None:
            return
        try:
            client.loop_stop()
        finally:
            try:
                client.disconnect()
            finally:
                self._connected = False
                self._client = None

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        client = self._client
        if client is None:
            raise RuntimeError("PahoMqttTransport is not connected")
        client.publish(topic, payload, qos=1 if retain else 0, retain=retain)

    def subscribe(self, topic: str, callback: Callable[[str, str], None]) -> None:
        with self._lock:
            self._subscriptions.append((topic, callback))
            client = self._client
            connected = self._connected
        if client is not None and connected:
            client.subscribe(topic, qos=1)

    def _on_connect(self, client: Any, _userdata: Any, _flags: Any, rc: int) -> None:
        if rc != 0:
            _LOGGER.warning("danfoss2mqtt: broker refused connection (rc=%s)", rc)
            return
        self._connected = True
        with self._lock:
            topics = [topic for topic, _ in self._subscriptions]
        for topic in topics:
            client.subscribe(topic, qos=1)
        _LOGGER.info("danfoss2mqtt: connected to Mosquitto %s:%d", self._host, self._port)

    def _on_disconnect(self, _client: Any, _userdata: Any, rc: int) -> None:
        self._connected = False
        if rc != 0:
            _LOGGER.warning("danfoss2mqtt: unexpected broker disconnect (rc=%s)", rc)

    def _on_message(self, _client: Any, _userdata: Any, msg: Any) -> None:
        topic = str(getattr(msg, "topic", ""))
        raw = getattr(msg, "payload", b"")
        text = (
            raw.decode("utf-8", errors="replace")
            if isinstance(raw, (bytes, bytearray))
            else str(raw)
        )
        matcher = getattr(self._paho_module, "topic_matches_sub", _topic_matches)
        with self._lock:
            callbacks = [cb for sub, cb in self._subscriptions if matcher(sub, topic)]
        for cb in callbacks:
            try:
                cb(topic, text)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("danfoss2mqtt: callback error on topic %s", topic)


class StandaloneDanfoss2MqttDaemon:
    """Pure-Python ``danfoss2mqtt`` bridge daemon with no Home Assistant dependency."""

    def __init__(
        self,
        transport: MqttTransport,
        base_topic: str = "danfoss2mqtt",
        *,
        on_device_set: Callable[[DanfossDevice, dict[str, Any]], None] | None = None,
        on_bridge_request: Callable[[str, dict[str, Any]], dict[str, Any] | None] | None = None,
    ) -> None:
        self._transport = transport
        self._base = base_topic.rstrip("/") or "danfoss2mqtt"
        self._on_device_set = on_device_set
        self._on_bridge_request = on_bridge_request
        self._devices: dict[str, DanfossDevice] = {}
        self._by_friendly: dict[str, DanfossDevice] = {}
        # dev.id -> its unique friendly name (topic segment), fixed by the last ``publish_fleet``.
        self._friendly_of: dict[str, str] = {}
        self._by_node: dict[str, DanfossDevice] = {}
        self._local_attrs: dict[str, dict[str, Any]] = {}
        # Cloud-MQTT DP 199 overlay, keyed by dev.id. Opportunistic reads decoded from the push
        # (``cloud_dp199``), layered on top of the HTTP poll baseline and the local-tunnel attrs.
        self._cloud_overlay: dict[str, HeadOverlay] = {}

    @property
    def base_topic(self) -> str:
        return self._base

    def start(self) -> None:
        """Announce bridge online and subscribe to ``+/set`` and ``bridge/request/+``."""
        self._transport.publish(f"{self._base}/bridge/state", AVAIL_ONLINE, retain=True)
        self._transport.subscribe(f"{self._base}/+/set", self._handle_set)
        self._transport.subscribe(f"{self._base}/bridge/request/+", self._handle_bridge_request)

    def stop(self) -> None:
        """Mark bridge offline on clean shutdown."""
        self._transport.publish(f"{self._base}/bridge/state", AVAIL_OFFLINE, retain=True)

    def publish_fleet(
        self,
        devices: list[DanfossDevice],
        rooms: list[RoomState] | None = None,
        local_attrs: dict[str, dict[str, Any]] | None = None,
        holidays: dict[str, Any] | None = None,
        device_home: dict[str, str] | None = None,
        relays: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        """Publish ``bridge/devices``, ``bridge/rooms``, ``bridge/holidays``, ``bridge/relays`` and
        all device states.

        All these topics are retained. ``rooms``/``holidays``/``relays`` left as ``None`` leave the
        matching topic untouched this cycle (its retained value stands); an empty list/dict is
        published as such so the topic appears on the broker.

        ``relays`` maps a boiler relay's device id to the rooms eligible to drive it,
        ``[{"room_id", "name", "linked"}]`` (the relay's own on/off is on its device topic).
        """
        self._devices = {d.id: d for d in devices}
        if local_attrs is not None:
            self._local_attrs = dict(local_attrs)

        descriptors: list[dict[str, Any]] = []
        self._by_friendly.clear()
        self._by_node.clear()
        self._friendly_of = unique_friendly_names(
            [d for d in devices if d.is_head or d.is_sensor or d.is_relay]
        )
        for dev in devices:
            if dev.node_id:
                self._by_node[dev.node_id] = dev
            if dev.is_head or dev.is_sensor or dev.is_relay:
                desc = encode_z2m_descriptor(dev)
                desc["friendly_name"] = self._friendly_of[dev.id]
                if device_home and dev.id in device_home:
                    desc.setdefault("danfoss", {})["home_id"] = str(device_home[dev.id])
                self._by_friendly[desc["friendly_name"]] = dev
                descriptors.append(desc)

        self._transport.publish(
            f"{self._base}/bridge/devices",
            json.dumps(descriptors),
            retain=True,
        )

        if rooms is not None:
            rooms_payload = [
                {
                    "group_id": r.group_id,
                    "name": r.name,
                    "pid": r.pid,
                    "home_id": r.home_gid,
                    "members": list(r.members),
                    "head_count": r.head_count,
                    "online": r.online,
                    "temperature": r.temperature,
                    "setpoint": r.setpoint,
                    "setpoints": dict(r.setpoints),
                    "mode": r.mode,
                    "heating": r.heating,
                }
                for r in rooms
            ]
            self._transport.publish(
                f"{self._base}/bridge/rooms",
                json.dumps(rooms_payload),
                retain=True,
            )

        if holidays is not None:
            self._transport.publish(
                f"{self._base}/bridge/holidays",
                json.dumps(holidays),
                retain=True,
            )

        if relays is not None:
            self._transport.publish(
                f"{self._base}/bridge/relays",
                json.dumps(relays),
                retain=True,
            )

        for dev in devices:
            if dev.is_head or dev.is_sensor or dev.is_relay:
                self.publish_device(dev)

    def publish_device(self, dev: DanfossDevice) -> None:
        """Publish availability and state for a single ``DanfossDevice``.

        The cloud-MQTT DP 199 overlay (``apply_cloud_push``) is layered here, over the polled
        baseline: its device fields win over the poll, and its decoded attrs are merged with the
        local-tunnel attrs (tunnel wins on a key present in both — the on-demand reader is
        authoritative). Nothing is layered for a head with no push yet.
        """
        overlay = self._cloud_overlay.get(dev.id)
        if overlay is not None:
            dev = overlay.apply_to(dev)
        friendly = self._friendly_for(dev)
        self._by_friendly[friendly] = dev
        if dev.node_id:
            self._by_node[dev.node_id] = dev
        self._transport.publish(
            f"{self._base}/{friendly}/availability",
            AVAIL_ONLINE if dev.online else AVAIL_OFFLINE,
            retain=True,
        )
        local_attrs = self._merged_local_attrs(dev.id, overlay)
        state = encode_z2m_state(dev, local_attrs=local_attrs)
        if dev.is_relay and not state:
            return
        self._transport.publish(
            f"{self._base}/{friendly}",
            json.dumps(state),
            retain=True,
        )

    def _friendly_for(self, dev: DanfossDevice) -> str:
        """Topic segment of ``dev``: the unique name fixed by the last ``publish_fleet``.

        A device published on its own before any fleet publish gets its plain name, unless another
        device already holds it: two devices must never share a topic (one would overwrite the
        other's state, and a ``/set`` would be routed to the wrong one).
        """
        known = self._friendly_of.get(dev.id)
        if known is not None:
            return known
        name = clean_friendly_name(dev.name, dev.id)
        holder = self._by_friendly.get(name)
        if holder is not None and holder.id != dev.id:
            name = _disambiguated(name, dev, set(self._by_friendly))
        self._friendly_of[dev.id] = name
        return name

    def _merged_local_attrs(
        self, dev_id: str, overlay: HeadOverlay | None
    ) -> dict[str, Any] | None:
        """Local-tunnel attrs for ``dev_id`` with the cloud overlay's attrs underneath them.

        The local tunnel is the on-demand reader (fresher by request), so a key it supplies wins
        over the same key from the opportunistic cloud push. Returns ``None`` when neither source
        has anything, so ``encode_z2m_state`` adds no tunnel-grade fields.
        """
        tunnel_attrs = self._local_attrs.get(dev_id)
        if overlay is None or not overlay.local_attrs:
            return tunnel_attrs
        return {**overlay.local_attrs, **(tunnel_attrs or {})}

    def apply_cloud_push(self, cid: str, dps: dict[str, Any]) -> DanfossDevice | None:
        """Decode a cloud-MQTT DP 199 push for head ``cid`` and overlay its decoded, dated values.

        ``cid`` is the head's Zigbee node id (``DanfossDevice.node_id``); ``dps`` the report's DP
        map. A no-op (returns ``None``) unless ``dps`` carries a ``"199"`` that decodes to at least
        one surfaced attribute for a head this bridge knows. On success the head's read state is
        overlaid and republished in the Z2M dialect, and the updated device is returned. The
        HTTP-poll and local-tunnel paths are untouched: this only fills the overlay when a push
        arrives (the push is opportunistic — DP 199 carries an attribute only when something reads
        it on the LAN).
        """
        if not cid:
            return None
        raw = dps.get("199") if isinstance(dps, dict) else None
        if raw is None:
            return None
        dev = self._by_node.get(cid)
        if dev is None:
            _LOGGER.debug("danfoss2mqtt: DP 199 push for unknown head %s - ignored", cid)
            return None
        decoded = decode_dp199(raw)
        if decoded is None:
            return None
        overlay = self._cloud_overlay.setdefault(dev.id, HeadOverlay())
        if not overlay.merge(decoded, now=time.time()):
            return None
        self.publish_device(dev)
        return overlay.apply_to(dev)

    def on_cloud_frame(self, gateway_id: str, frame: Any) -> None:
        """Route a decoded cloud-MQTT inbound frame to the DP 199 overlay (``apply_cloud_push``).

        Matches the ``mqtt_client.CloudMqttLink.on_frame`` signature, so a live listen-only link
        feeds the overlay with no glue at the call site: ``link.on_frame = daemon.on_cloud_frame``.
        Runs on the MQTT client's network thread; it only touches the daemon's own state and
        republishes through the transport. ``gateway_id`` is unused here: ``apply_cloud_push``
        routes by the head's ``cid`` (its Zigbee node id), which the frame's ``data`` carries. A
        frame with no ``cid``/``dps`` (e.g. a poll-control check-in, or a publish echo) is ignored.
        """
        data = getattr(frame, "data", None)
        if not isinstance(data, dict):
            return
        cid = data.get("cid")
        dps = data.get("dps")
        if not isinstance(cid, str) or not isinstance(dps, dict):
            return
        self.apply_cloud_push(cid, dps)

    def _handle_set(self, topic: str, raw_payload: str) -> None:
        if self._on_device_set is None:
            return
        prefix = f"{self._base}/"
        if not topic.startswith(prefix) or not topic.endswith("/set"):
            return
        friendly = topic[len(prefix) : -len("/set")]
        dev = self._by_friendly.get(friendly)
        if dev is None:
            _LOGGER.warning("danfoss2mqtt daemon: unknown target %s", friendly)
            return
        try:
            payload = json.loads(raw_payload)
            if not isinstance(payload, dict):
                raise ValueError("JSON object expected")
        except ValueError as err:
            _LOGGER.warning("danfoss2mqtt daemon: invalid JSON on %s: %s", topic, err)
            return
        self._on_device_set(dev, payload)

    def publish_bridge_response(
        self, action: str, reply: dict[str, Any], *, transaction: Any = None
    ) -> None:
        """Publish an RPC reply on ``<base>/bridge/response/<action>``."""
        out = dict(reply)
        if transaction is not None and "transaction" not in out:
            out["transaction"] = transaction
        self._transport.publish(
            f"{self._base}/bridge/response/{action}", json.dumps(out), retain=False
        )

    def _handle_bridge_request(self, topic: str, raw_payload: str) -> None:
        prefix = f"{self._base}/bridge/request/"
        if not topic.startswith(prefix):
            return
        action = topic[len(prefix) :]
        try:
            payload = json.loads(raw_payload) if raw_payload else {}
            if not isinstance(payload, dict):
                raise ValueError("JSON object expected")
        except ValueError as err:
            self.publish_bridge_response(
                action, {"status": "error", "error": f"invalid JSON: {err}"}
            )
            return

        tx = payload.get("transaction")
        if self._on_bridge_request is None:
            reply: dict[str, Any] = {"status": "error", "error": "unsupported request"}
        else:
            try:
                data = self._on_bridge_request(action, payload)
                if data is None:
                    # Deferred/async handler will publish its own response via
                    # ``publish_bridge_response`` once the underlying call completes.
                    return
                reply = {"status": "ok", "data": data}
            except Exception as err:  # noqa: BLE001
                reply = {"status": "error", "error": str(err)}
        self.publish_bridge_response(action, reply, transaction=tx)


# Zigbee2MQTT ``/set`` fields the standalone cloud bridge has NO proven path 1a for from the
# CloudClient alone (``docs/interface-contract.md``, real-hardware W-3 2026-11-19):
#   - child lock (DP 30) applies only over Tuya cloud MQTT, not HTTP ``settings.change``;
#   - window detection (DP 116) is a local-tunnel-only write;
#   - preheat / programming mode (Zigbee attr 0x0025), external room sensor, load balancing and the
#     weekly schedule reach the head only through the local tunnel.
# Accepting them on ``/set`` and dropping them silently is a lie; we log instead.
_CLOUD_UNSUPPORTED_SET_KEYS = frozenset(
    {
        "keypad_lockout",
        "child_lock",
        "window_open_feature",
        "viewing_direction",
        "max_heat_setpoint_limit",
        "min_heat_setpoint_limit",
        "programming_operation_mode",
        "external_measured_room_sensor",
        "window_open_external",
        "load_room_mean",
        "weekly_schedule",
    }
)

# Fields the bridge answers to but that carry no write (state echoes / mode label).
_CLOUD_IGNORED_SET_KEYS = frozenset({"system_mode"})


def _group_setpoint_dps(setpoint_c: float, mode: str | None) -> dict[str, int]:
    """Build the mode-aware group DPs for a ``/set`` ``occupied_heating_setpoint`` (°C).

    In the Zigbee2MQTT dialect ``occupied_heating_setpoint`` is the head's ACTIVE setpoint, so the
    write must target the setting of its CURRENT mode/slot, never comfort unconditionally: comfort
    (DP 118) in ``at_home``, setback (DP 109 + 119) in ``leaving_home``. Mirrors the room climate's
    ``DanfossClimate.async_set_temperature`` in ha-danfoss-ally ("per current mode"). Raises
    ``CloudWriteError`` for any other mode, which has no proven path-1a group method, so the caller
    refuses rather than silently moving the comfort setpoint (real hardware, 2026-11-22: writing it
    on a head in a setback slot changed comfort while the active setpoint stayed put).
    """
    dps_nums = MODE_GROUP_SETPOINT_DP.get(mode or "")
    if not dps_nums:
        raise CloudWriteError(
            "setpoint editable only in comfort (at_home) or setback (leaving_home) mode; "
            f"current mode: {mode!r}"
        )
    encoded = encode_setpoint(setpoint_c, SETPOINT_MIN, SETPOINT_MAX, SETPOINT_STEP)
    return {str(dp): encoded for dp in dps_nums}


async def _build_rooms(client: CloudClient, devices: list[DanfossDevice]) -> list[RoomState] | None:
    """Aggregate the account's rooms (Tuya groups) from the freshly polled ``devices``.

    Returns ``None`` (leave ``bridge/rooms`` untouched this cycle) if the room read fails, so a
    transient rooms hiccup never loses the device publish. Heads absent from this poll are simply
    not aggregated; an empty account still yields ``[]`` so the topic is published.
    """
    try:
        groups = await client.async_list_room_groups()
    except CloudError:
        _LOGGER.warning("danfoss2mqtt: could not read rooms this cycle", exc_info=True)
        return None
    by_id = {d.id: d for d in devices}
    return [aggregate_room(room, [by_id[m] for m in room.members if m in by_id]) for room in groups]


async def _build_holidays(client: CloudClient) -> dict[str, Any] | None:
    """Per-home holiday state keyed by home gid (``bridge/holidays`` contract §3), or ``None``.

    The Companion ingests ``bridge/holidays`` as a dict ``{gid: {...}}``; ``async_list_holidays``
    returns a per-home list, remapped here. ``None`` on failure leaves the topic untouched.
    """
    try:
        items = await client.async_list_holidays()
    except CloudError:
        _LOGGER.warning("danfoss2mqtt: could not read holidays this cycle", exc_info=True)
        return None
    return {str(h["gid"]): h for h in items if h.get("gid") is not None}


def _disambiguated(name: str, dev: DanfossDevice, taken: set[str]) -> str:
    """A name for ``dev`` not in ``taken``: ``"<name> (<category>)"``, then with the id's tail."""
    candidate = f"{name} ({dev.category})"
    if candidate in taken:
        candidate = f"{candidate} {dev.id[-4:]}"
    if candidate in taken:
        candidate = f"{name} ({dev.id})"
    return candidate


def unique_friendly_names(devices: list[DanfossDevice]) -> dict[str, str]:
    """``dev.id -> friendly_name``, unique across the fleet (the name is the MQTT topic segment).

    In the Danfoss app a room sensor is usually named after its room, and so is the single head of
    a one-head room: both then had the same topic, each overwriting the other's retained state, and
    a ``/set`` was routed to whichever was published last. A name used once is kept as is. When
    several devices share a name, the head keeps it (its topic is the one that takes writes, and
    stays stable) and the others get ``"<name> (<category>)"``; devices still colliding (same
    category) get the tail of their id. The order is deterministic (heads first, then by id), so a
    name does not move between two polls.
    """
    by_name: dict[str, list[DanfossDevice]] = {}
    for dev in devices:
        by_name.setdefault(clean_friendly_name(dev.name, dev.id), []).append(dev)
    out: dict[str, str] = {}
    taken: set[str] = {name for name, devs in by_name.items() if len(devs) == 1}
    for name, devs in by_name.items():
        if len(devs) == 1:
            out[devs[0].id] = name
    for name in sorted(n for n, devs in by_name.items() if len(devs) > 1):
        for i, dev in enumerate(sorted(by_name[name], key=lambda d: (not d.is_head, d.id))):
            chosen = name if i == 0 and dev.is_head else _disambiguated(name, dev, taken)
            if chosen in taken:
                chosen = _disambiguated(name, dev, taken)
            out[dev.id] = chosen
            taken.add(chosen)
    return out


# The relay -> room linkage is account configuration, changed by hand in the app and rarely: it is
# re-read at most this often (one request per relay), not on every poll cycle.
RELAY_LINKS_TTL_SECONDS = 3600.0


class RelayLinks:
    """Boiler relays' room linkage for ``bridge/relays``, read sparingly from the cloud.

    One ``async_relay_rooms`` request per relay, at most every ``RELAY_LINKS_TTL_SECONDS`` (or at
    once when a relay appears). A relay whose read fails keeps its previous rooms and is retried on
    the next cycle; a relay gone from the fleet is dropped. Read only.
    """

    def __init__(self, ttl: float = RELAY_LINKS_TTL_SECONDS) -> None:
        self._ttl = ttl
        self._rooms: dict[str, list[dict[str, Any]]] = {}
        self._read_at: dict[str, float] = {}

    async def read(
        self, client: CloudClient, devices: list[DanfossDevice]
    ) -> dict[str, list[dict[str, Any]]] | None:
        """Current linkage keyed by relay id, or ``None`` when nothing is known yet.

        ``None`` leaves ``bridge/relays`` untouched (no relay read succeeded so far); a fleet
        without any relay yields ``{}`` so the topic is published empty.
        """
        relay_ids = [d.id for d in devices if d.is_relay]
        for gone in set(self._rooms) - set(relay_ids):
            self._rooms.pop(gone, None)
            self._read_at.pop(gone, None)
        now = time.monotonic()
        for relay_id in relay_ids:
            last = self._read_at.get(relay_id)
            if last is not None and now - last < self._ttl:
                continue
            try:
                rooms = await client.async_relay_rooms(relay_id)
            except CloudError:
                _LOGGER.warning("danfoss2mqtt: could not read a relay's rooms this cycle")
                continue
            self._rooms[relay_id] = [
                {
                    "room_id": str(r.get("room_id")),
                    "name": r.get("name"),
                    "linked": bool(r.get("linked")),
                }
                for r in rooms
                if isinstance(r, dict) and r.get("room_id") is not None
            ]
            self._read_at[relay_id] = now
        if relay_ids and not self._rooms:
            return None
        return {rid: list(rooms) for rid, rooms in self._rooms.items()}


async def build_cloud_mqtt_listener(
    config: BridgeConfig,
    client: CloudClient,
    daemon: StandaloneDanfoss2MqttDaemon,
) -> CloudMqttLink | None:
    """Build a LISTEN-ONLY cloud-MQTT link whose decoded frames feed the DP 199 overlay.

    The link's ``on_frame`` is ``daemon.on_cloud_frame``, so every authenticated inbound frame is
    routed to ``apply_cloud_push`` (DP 199 -> read-state overlay). It is always ``listen_only`` —
    the bridge only consumes pushes on this channel, it never publishes over it.

    Credentials are derived from the bridge client's OWN session (``mqtt_identity`` / ``session``),
    logging in once if that session is not yet complete, and from the config's region/sign/ch/device
    material; a ``creds_provider`` rebuilds them from the current session on each reconnect, so a
    rotated session is picked up. Gateway crypto comes from ``async_fetch_gateway_keys``.

    Returns ``None`` (logged, never raised) when the material is incomplete — an incomplete session,
    no gateway with a local key, or missing sign/ch/device config. Does NOT open the connection
    (``link.start``); the caller starts it off the event loop.
    """
    if not (config.sign_key and config.ch_key and config.device_id):
        _LOGGER.warning("cloud-mqtt listener: sign_key/ch_key/device_id missing; skipped")
        return None

    def _complete(ident: dict[str, str | None], sess: dict[str, str | None]) -> bool:
        return bool(
            ident.get("uid")
            and ident.get("partner_identity")
            and sess.get("sid")
            and sess.get("ecode")
        )

    ident = client.mqtt_identity
    sess = client.session
    if not _complete(ident, sess):
        try:
            await client.async_login()
        except CloudError:
            _LOGGER.warning("cloud-mqtt listener: login failed; skipped", exc_info=True)
            return None
        ident = client.mqtt_identity
        sess = client.session
    if not _complete(ident, sess):
        _LOGGER.warning("cloud-mqtt listener: session incomplete (uid/partnerIdentity); skipped")
        return None

    try:
        gateways = await async_fetch_gateway_keys(client)
    except CloudError:
        _LOGGER.warning("cloud-mqtt listener: gateway key fetch failed; skipped", exc_info=True)
        return None
    if not gateways:
        _LOGGER.warning("cloud-mqtt listener: no gateway with a local key; skipped")
        return None

    def _fresh_creds() -> MqttCredentials:
        i = client.mqtt_identity
        s = client.session
        return build_credentials(
            region=config.region,
            sign_key=config.sign_key or "",
            sid=s.get("sid") or "",
            ecode=s.get("ecode") or "",
            uid=i.get("uid") or "",
            partner_identity=i.get("partner_identity") or "",
            device_id=config.device_id or "",
            ch_key=config.ch_key or "",
        )

    refs = {gw_id: GatewayRef(dev_id=gw_id, local_key=k.local_key) for gw_id, k in gateways.items()}
    return CloudMqttLink(
        credentials=_fresh_creds(),
        gateways=refs,
        creds_provider=_fresh_creds,
        on_frame=daemon.on_cloud_frame,
        listen_only=True,
    )


async def async_run_bridge(
    config: BridgeConfig,
    *,
    transport: MqttTransport | None = None,
    client: CloudClient | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run the ``danfoss2mqtt`` polling + command loop until ``stop_event`` is set."""
    own_transport = transport is None
    if transport is None:
        paho_transport = PahoMqttTransport(
            host=config.mqtt_host,
            port=config.mqtt_port,
            base_topic=config.base_topic,
            username=config.mqtt_username,
            password=config.mqtt_password,
            tls=config.mqtt_tls,
            client_id=config.client_id,
        )
        paho_transport.connect()
        transport = paho_transport

    own_client = client is None
    if client is None:
        if not config.has_cloud_credentials:
            raise ValueError("Danfoss Cloud credentials are required to start the live bridge")
        client = CloudClient(
            username=config.username or "",
            password=config.password or "",
            country_code=config.country_code,
            sign_key=config.sign_key or "",
            device_id=config.device_id or secrets.token_hex(22),
            ch_key=config.ch_key or "",
            region=config.region,
        )

    loop = asyncio.get_running_loop()
    refresh_event = asyncio.Event()
    stop = stop_event or asyncio.Event()
    pending_writes: set[asyncio.Task[None]] = set()

    async def _apply_set(dev: DanfossDevice, payload: dict[str, Any]) -> None:
        applied = False

        # Only the room-level setpoint has a proven path 1a from the cloud (group publish +
        # schedule rewrite); ``async_set_room_setpoint`` resolves the head's room and refuses an
        # isolated one.
        raw_setpoint = payload.get("occupied_heating_setpoint")
        if raw_setpoint is not None:
            if dev.product_id not in WRITE_VERIFIED_PRODUCTS:
                _LOGGER.warning(
                    "danfoss2mqtt: setpoint write not verified for product %s (%s); skipping",
                    dev.product_id,
                    dev.id,
                )
            else:
                try:
                    dps = _group_setpoint_dps(float(raw_setpoint), dev.mode)
                    group_id = await client.async_set_room_setpoint(dev.id, dps)
                    _LOGGER.info(
                        "danfoss2mqtt: setpoint %.1f °C written on room %s in %s mode (head %s)",
                        float(raw_setpoint),
                        group_id,
                        dev.mode,
                        dev.id,
                    )
                    applied = True
                except (CloudError, ValueError) as err:
                    _LOGGER.warning("danfoss2mqtt: setpoint write refused for %s: %s", dev.id, err)

        unsupported = sorted(k for k in payload if k in _CLOUD_UNSUPPORTED_SET_KEYS)
        if unsupported:
            _LOGGER.warning(
                "danfoss2mqtt: /set field(s) %s for %s have no proven Danfoss cloud path from the "
                "standalone bridge (child lock via Tuya cloud MQTT; window detection, preheat, "
                "external sensor, load balancing and schedule via the local tunnel) - ignored",
                unsupported,
                dev.id,
            )
        unknown = sorted(
            k
            for k in payload
            if k not in _CLOUD_UNSUPPORTED_SET_KEYS
            and k not in _CLOUD_IGNORED_SET_KEYS
            and k != "occupied_heating_setpoint"
        )
        if unknown:
            _LOGGER.debug("danfoss2mqtt: unrecognized /set field(s) %s for %s", unknown, dev.id)

        # Re-poll only after a real write; a no-op must not spin the cloud.
        if applied:
            refresh_event.set()

    def _on_device_set(dev: DanfossDevice, payload: dict[str, Any]) -> None:
        def _spawn() -> None:
            # Keep a strong reference until completion: the loop only weakly references tasks.
            task = asyncio.create_task(_apply_set(dev, payload))
            pending_writes.add(task)
            task.add_done_callback(pending_writes.discard)

        loop.call_soon_threadsafe(_spawn)

    async def _apply_bridge_request(action: str, payload: dict[str, Any]) -> None:
        tx = payload.get("transaction")
        target = payload.get("target")
        if not isinstance(target, dict) or target.get("type") != "room" or not target.get("id"):
            daemon.publish_bridge_response(
                action,
                {
                    "status": "error",
                    "code": "unknown_room",
                    "error": f"{action} requires a room target ({{type: 'room', id: ...}})",
                },
                transaction=tx,
            )
            return
        room_target = {"type": "room", "id": str(target["id"])}
        try:
            if action == "get_schedule":
                data = await client.async_get_schedule(room_target)
            elif action == "set_schedule":
                days = payload.get("days")
                if not isinstance(days, dict):
                    raise ValueError("set_schedule requires a 'days' mapping")
                await client.async_set_schedule_days(room_target, days)
                refresh_event.set()
                data = {"target": room_target, "written": True}
            else:  # restore_schedule
                await client.async_restore_schedule(room_target)
                refresh_event.set()
                data = {"target": room_target, "restored": True}
            daemon.publish_bridge_response(action, {"status": "ok", "data": data}, transaction=tx)
        except ScheduleValidationError as err:
            first = err.errors[0]
            daemon.publish_bridge_response(
                action,
                {
                    "status": "error",
                    "code": first["code"],
                    "error": first["message"],
                    "errors": err.errors,
                    "day": first.get("day"),
                },
                transaction=tx,
            )
        except UnknownTargetError as err:
            daemon.publish_bridge_response(
                action,
                {"status": "error", "code": "unknown_target", "error": str(err)},
                transaction=tx,
            )
        except CloudError as err:
            daemon.publish_bridge_response(
                action,
                {"status": "error", "code": "cloud_refused", "error": str(err)},
                transaction=tx,
            )
        except ValueError as err:
            daemon.publish_bridge_response(
                action,
                {"status": "error", "code": "invalid_request", "error": str(err)},
                transaction=tx,
            )

    def _on_bridge_request(action: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        if action == "refresh":
            loop.call_soon_threadsafe(refresh_event.set)
            return {"refreshed": True}
        if action in ("get_schedule", "set_schedule", "restore_schedule"):

            def _spawn_req() -> None:
                task = asyncio.create_task(_apply_bridge_request(action, payload))
                pending_writes.add(task)
                task.add_done_callback(pending_writes.discard)

            loop.call_soon_threadsafe(_spawn_req)
            return None
        raise ValueError(f"unsupported bridge request: {action}")

    daemon = StandaloneDanfoss2MqttDaemon(
        transport,
        base_topic=config.base_topic,
        on_device_set=_on_device_set,
        on_bridge_request=_on_bridge_request,
    )
    daemon.start()

    cloud_link: CloudMqttLink | None = None
    if config.cloud_mqtt_listen:
        try:
            cloud_link = await build_cloud_mqtt_listener(config, client, daemon)
            if cloud_link is not None:
                await loop.run_in_executor(None, cloud_link.start)
                _LOGGER.info(
                    "danfoss2mqtt: cloud-MQTT DP 199 listener started (listen-only, %d gateway(s))",
                    len(cloud_link.gateways),
                )
        except Exception:  # noqa: BLE001 - a listener failure must never break the poll loop
            _LOGGER.exception("danfoss2mqtt: cloud-MQTT listener setup failed")
            cloud_link = None

    relay_links = RelayLinks()
    try:
        while not stop.is_set():
            try:
                devices = await client.async_list_devices()
                rooms = await _build_rooms(client, devices)
                holidays = await _build_holidays(client)
                relays = await relay_links.read(client, devices)
                raw_home = getattr(client, "device_home", None)
                dev_home = raw_home if isinstance(raw_home, dict) else None
                daemon.publish_fleet(
                    devices,
                    rooms=rooms,
                    holidays=holidays,
                    device_home=dev_home,
                    relays=relays,
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception("danfoss2mqtt: poll cycle failed")

            refresh_event.clear()
            stop_task = asyncio.create_task(stop.wait())
            refresh_task = asyncio.create_task(refresh_event.wait())
            done, pending = await asyncio.wait(
                {stop_task, refresh_task},
                timeout=max(5, config.poll_interval),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                task.result()
    finally:
        if cloud_link is not None:
            with contextlib.suppress(Exception):
                await loop.run_in_executor(None, cloud_link.stop)
        daemon.stop()
        if own_client:
            await client.async_close()
        if own_transport and isinstance(transport, PahoMqttTransport):
            transport.disconnect()


def load_bridge_config(
    args: argparse.Namespace,
    options_path: str | Path | None = None,
) -> BridgeConfig:
    """Build ``BridgeConfig`` from HA Add-on ``options.json``, env vars, and CLI args."""
    opts: dict[str, Any] = {}
    path = options_path or getattr(args, "options", None)
    if path:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            opts = raw

    def _pick(arg_name: str, opt_key: str, env_key: str, default: Any = None) -> Any:
        cli_val = getattr(args, arg_name, None)
        if cli_val is not None:
            return cli_val
        if opts.get(opt_key) not in (None, ""):
            return opts[opt_key]
        env_val = os.environ.get(env_key)
        if env_val not in (None, ""):
            return env_val
        return default

    username = _pick("username", "username", "DANFOSS_USERNAME")
    password = _pick("password", "password", "DANFOSS_PASSWORD")
    sign_key = _pick("sign_key", "sign_key", "DANFOSS_SIGN_KEY")
    ch_key = _pick("ch_key", "ch_key", "DANFOSS_CH_KEY")
    device_id = _pick("device_id", "device_id", "DANFOSS_DEVICE_ID")
    if not device_id and username and password and sign_key and ch_key:
        if path:
            state_dir = Path(path).resolve().parent
        elif os.environ.get("DANFOSS_STATE_DIR"):
            state_dir = Path(os.environ["DANFOSS_STATE_DIR"])
        else:
            state_dir = Path.home() / ".danfoss2mqtt"
        device_id = ensure_device_id(state_dir)

    return BridgeConfig(
        mqtt_host=str(_pick("mqtt_host", "mqtt_host", "DANFOSS_MQTT_HOST", "localhost")),
        mqtt_port=int(_pick("mqtt_port", "mqtt_port", "DANFOSS_MQTT_PORT", 1883)),
        mqtt_username=_pick("mqtt_username", "mqtt_username", "DANFOSS_MQTT_USERNAME"),
        mqtt_password=_pick("mqtt_password", "mqtt_password", "DANFOSS_MQTT_PASSWORD"),
        mqtt_tls=bool(_pick("mqtt_tls", "mqtt_tls", "DANFOSS_MQTT_TLS", False)),
        base_topic=str(_pick("base_topic", "base_topic", "DANFOSS_BASE_TOPIC", "danfoss2mqtt")),
        username=username,
        password=password,
        country_code=str(_pick("country_code", "country_code", "DANFOSS_COUNTRY_CODE", "33")),
        region=str(_pick("region", "region", "DANFOSS_REGION", "eu")),
        sign_key=sign_key,
        device_id=device_id,
        ch_key=ch_key,
        poll_interval=int(
            _pick("poll_interval", "poll_interval", "DANFOSS_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)
        ),
        cloud_mqtt_listen=bool(
            _pick("cloud_mqtt_listen", "cloud_mqtt_listen", "DANFOSS_CLOUD_MQTT_LISTEN", False)
        ),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="danfoss2mqtt",
        description="Standalone Danfoss Ally -> Mosquitto (Zigbee2MQTT dialect) bridge daemon.",
    )
    parser.add_argument(
        "--options",
        default=os.environ.get("DANFOSS_OPTIONS_FILE"),
        help="Path to Home Assistant Add-on options.json (optional)",
    )
    parser.add_argument("--mqtt-host", default=None, help="Mosquitto broker hostname")
    parser.add_argument("--mqtt-port", type=int, default=None, help="Mosquitto broker port")
    parser.add_argument("--mqtt-username", default=None, help="Mosquitto username")
    parser.add_argument("--mqtt-password", default=None, help="Mosquitto password")
    parser.add_argument("--mqtt-tls", action="store_true", default=None, help="Enable MQTT TLS")
    parser.add_argument(
        "--base-topic", default=None, help="Base MQTT topic (default: danfoss2mqtt)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=None,
        help="Cloud poll interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--cloud-mqtt-listen",
        dest="cloud_mqtt_listen",
        action="store_true",
        default=None,
        help="Open a listen-only Tuya cloud-MQTT link so DP 199 pushes feed the read-state overlay",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and exit without connecting",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``danfoss2mqtt``."""
    args = build_arg_parser().parse_args(argv)
    config = load_bridge_config(args)
    _LOGGER.info(
        "danfoss2mqtt configured for broker %s:%d (topic %s, poll=%ds)",
        config.mqtt_host,
        config.mqtt_port,
        config.base_topic,
        config.poll_interval,
    )
    if args.dry_run or not config.has_cloud_credentials:
        return 0

    async def _run_with_signals() -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)
        await async_run_bridge(config, stop_event=stop_event)

    asyncio.run(_run_with_signals())
    return 0
