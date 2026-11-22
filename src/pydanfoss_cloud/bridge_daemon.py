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
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .client import CloudClient, CloudError
from .const import (
    MODE_GROUP_SETPOINT_DP,
    SETPOINT_MAX,
    SETPOINT_MIN,
    SETPOINT_STEP,
    WRITE_VERIFIED_PRODUCTS,
)
from .decode import DanfossDevice
from .mqtt_client import _new_paho_client
from .rooms import RoomState
from .writes import encode_setpoint
from .z2m_contract import encode_z2m_descriptor, encode_z2m_state

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
        on_bridge_request: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self._transport = transport
        self._base = base_topic.rstrip("/") or "danfoss2mqtt"
        self._on_device_set = on_device_set
        self._on_bridge_request = on_bridge_request
        self._devices: dict[str, DanfossDevice] = {}
        self._by_friendly: dict[str, DanfossDevice] = {}
        self._local_attrs: dict[str, dict[str, Any]] = {}

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
    ) -> None:
        """Publish ``bridge/devices``, ``bridge/rooms``, and all device states to Mosquitto."""
        self._devices = {d.id: d for d in devices}
        if local_attrs is not None:
            self._local_attrs = dict(local_attrs)

        descriptors: list[dict[str, Any]] = []
        self._by_friendly.clear()
        for dev in devices:
            if dev.is_head or dev.is_sensor or dev.is_relay:
                desc = encode_z2m_descriptor(dev)
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

        for dev in devices:
            if dev.is_head or dev.is_sensor or dev.is_relay:
                self.publish_device(dev)

    def publish_device(self, dev: DanfossDevice) -> None:
        """Publish availability and state for a single ``DanfossDevice``."""
        desc = encode_z2m_descriptor(dev)
        friendly = desc["friendly_name"]
        self._by_friendly[friendly] = dev
        self._transport.publish(
            f"{self._base}/{friendly}/availability",
            AVAIL_ONLINE if dev.online else AVAIL_OFFLINE,
            retain=True,
        )
        state = encode_z2m_state(dev, local_attrs=self._local_attrs.get(dev.id))
        if dev.is_relay and not state:
            return
        self._transport.publish(
            f"{self._base}/{friendly}",
            json.dumps(state),
            retain=True,
        )

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

    def _handle_bridge_request(self, topic: str, raw_payload: str) -> None:
        prefix = f"{self._base}/bridge/request/"
        if not topic.startswith(prefix):
            return
        action = topic[len(prefix) :]
        resp_topic = f"{self._base}/bridge/response/{action}"
        try:
            payload = json.loads(raw_payload) if raw_payload else {}
            if not isinstance(payload, dict):
                raise ValueError("JSON object expected")
        except ValueError as err:
            self._transport.publish(
                resp_topic,
                json.dumps({"status": "error", "error": f"invalid JSON: {err}"}),
                retain=False,
            )
            return

        tx = payload.get("transaction")
        if self._on_bridge_request is None:
            reply: dict[str, Any] = {"status": "error", "error": "unsupported request"}
        else:
            try:
                data = self._on_bridge_request(action, payload)
                reply = {"status": "ok", "data": data}
            except Exception as err:  # noqa: BLE001
                reply = {"status": "error", "error": str(err)}
        if tx is not None:
            reply["transaction"] = tx
        self._transport.publish(resp_topic, json.dumps(reply), retain=False)


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


def _group_setpoint_dps(setpoint_c: float) -> dict[str, int]:
    """Build the comfort-setpoint group DPs for a ``/set`` ``occupied_heating_setpoint`` (°C)."""
    encoded = encode_setpoint(setpoint_c, SETPOINT_MIN, SETPOINT_MAX, SETPOINT_STEP)
    return {str(dp): encoded for dp in MODE_GROUP_SETPOINT_DP["at_home"]}


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
                    dps = _group_setpoint_dps(float(raw_setpoint))
                    group_id = await client.async_set_room_setpoint(dev.id, dps)
                    _LOGGER.info(
                        "danfoss2mqtt: setpoint %.1f °C written on room %s (head %s)",
                        float(raw_setpoint),
                        group_id,
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

    def _on_bridge_request(action: str, _payload: dict[str, Any]) -> dict[str, Any]:
        if action == "refresh":
            loop.call_soon_threadsafe(refresh_event.set)
            return {"refreshed": True}
        raise ValueError(f"unsupported bridge request: {action}")

    daemon = StandaloneDanfoss2MqttDaemon(
        transport,
        base_topic=config.base_topic,
        on_device_set=_on_device_set,
        on_bridge_request=_on_bridge_request,
    )
    daemon.start()

    try:
        while not stop.is_set():
            try:
                devices = await client.async_list_devices()
                daemon.publish_fleet(devices)
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
