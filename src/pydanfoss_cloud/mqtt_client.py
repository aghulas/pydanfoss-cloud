"""Async Tuya-cloud MQTT link for isolated-head DP publish (EXPERIMENTAL, gated OFF by default).

Builds on :mod:`mqtt_publish` (frame codec + credential derivation, reverse-engineered and validated
on real frames — see docs/cloud-mqtt-publish.md). This module adds the live layer: credentials for a
**dedicated R&D deviceId** (so it never shares the Tuya session with the Danfoss app, the iPhone app
or the MCP connector), one connection per config entry, a per-gateway ``SeqOrigin`` kept for the
whole session, subscription to ``smart/mb/in/<gw>`` and inbound-frame decoding, and a gated publish.

Transport: **paho-mqtt**, lazy-imported only when a connection is opened. Rationale: paho is already
bundled by Home Assistant (its core MQTT integration uses it), so there is no new dependency and no
second MQTT stack, and real-hardware validation confirmed it connects and coexists with the app from
the Pi. The loop runs in paho's background thread (renamed so the client id never reaches the HA
log); callbacks only touch plain counters/flags, read back on the event loop.

SAFETY: nothing here is wired to publish on its own. The publish method exists but a caller invokes
it only behind the gated option AND a real connection; there is no optimistic state. Secrets
(localKey, username, password, sid, ecode) and identifiers (client id, gateway devId) never reach
logs, error messages or ``repr``.

ACCOUNT SAFETY: this is the user's real account. Reconnection backs off exponentially
(``RECONNECT_MIN_DELAY`` to ``RECONNECT_MAX_DELAY``), credentials are rebuilt from the current
session before each reconnection, and a link the broker keeps refusing as not authorised stops for
good after ``MAX_AUTH_REFUSALS`` attempts instead of retrying forever.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import mqtt_publish as mp
from .client import derive_distinct_device_id

_LOGGER = logging.getLogger(__name__)

# Region -> MQTT broker host (mirrors the HTTP datacenter; the SDK uses the same m1.<region> host).
REGION_MQTT_HOST = {
    "eu": "m1.tuyaeu.com",
    "us": "m1.tuyaus.com",
    "cn": "m1.tuyacn.com",
    "in": "m1.tuyain.com",
}
DEFAULT_REGION = "eu"

# Marker for the dedicated R&D deviceId, distinct from the integration's own ("ha") and the app's.
RD_DEVICE_ID_MARKER = "mqttrd"

# The HTTP client id. The MQTT username uses this app id and the installation chKey (not the
# account deviceId).
DEFAULT_APP_ID = "7kypwcaqcvw3uvr7u95a"

# Reconnection backoff (seconds, exponential between the two). The first retry is not immediate and
# the ceiling is minutes, not seconds: a broker that keeps dropping us must not see a tight loop.
RECONNECT_MIN_DELAY = 5
RECONNECT_MAX_DELAY = 300
# CONNACK return codes that mean "these credentials are refused" (4 = bad user name or password,
# 5 = not authorised), as opposed to a transient broker condition.
AUTH_REFUSED_RCS = frozenset({4, 5})
# Consecutive refusals after which the link stops for good (until the entry is reloaded). With the
# backoff above that is a bit over a minute of attempts, each with credentials rebuilt from the
# then-current session (``on_auth_refused`` lets the owner renew that session at the first one).
MAX_AUTH_REFUSALS = 5

# Link states, for the diagnostics and the link sensor.
STATE_IDLE = "idle"  # never started
STATE_CONNECTING = "connecting"  # started, not connected (first connection or backoff)
STATE_CONNECTED = "connected"
STATE_REFUSED = "refused"  # stopped after MAX_AUTH_REFUSALS refusals
STATE_STOPPED = "stopped"  # closed by stop()


def _new_paho_client(mqtt: Any, client_id: str) -> Any:
    """Create a paho ``Client`` compatible with both paho-mqtt 1.6.x and 2.x. On 2.x the callback
    API version must be chosen explicitly; we keep the v1 callback signatures used below, so we pass
    ``CallbackAPIVersion.VERSION1`` when that enum exists (paho 2) and nothing on paho 1.6.1."""
    kwargs: dict[str, Any] = {
        "client_id": client_id,
        "clean_session": True,
        "protocol": mqtt.MQTTv311,
    }
    cb = getattr(mqtt, "CallbackAPIVersion", None)
    if cb is not None:
        kwargs["callback_api_version"] = cb.VERSION1
    return mqtt.Client(**kwargs)


def region_host(region: str | None) -> str:
    """Broker host for a datacenter code; falls back to EU for an unknown/empty region."""
    return REGION_MQTT_HOST.get((region or DEFAULT_REGION).lower(), REGION_MQTT_HOST["eu"])


@dataclass(frozen=True)
class MqttCredentials:
    """What an MQTT connect needs. All fields except host/port are secret-derived."""

    host: str
    # Kept out of ``repr``: the client id embeds the deviceId, the others are secret-derived.
    client_id: str = field(repr=False)
    username: str = field(repr=False)
    password: str = field(repr=False)
    port: int = mp.BROKER_PORT

    def redacted(self) -> dict[str, Any]:
        """Loggable view: host/port only, never the credentials."""
        return {"host": self.host, "port": self.port, "client_id": "<redacted>"}


def build_credentials(
    *,
    region: str,
    sign_key: str,
    sid: str,
    ecode: str,
    uid: str,
    partner_identity: str,
    device_id: str,
    ch_key: str,
    app_id: str = DEFAULT_APP_ID,
    package: str = "com.danfoss.ally",
) -> MqttCredentials:
    """Derive the broker credentials. ``device_id`` is the account's captured id; we connect under a
    DEDICATED derived id (``RD_DEVICE_ID_MARKER``) so the clientId cannot collide with the app
    or the integration's own session. Username/password derivations are byte-exact vs the app
    (docs §6 step 3): username uses ``app_id``/``ch_key`` (not the deviceId), so this id
    is safe here.
    """
    rd_device_id = derive_distinct_device_id(device_id, RD_DEVICE_ID_MARKER)
    return MqttCredentials(
        host=region_host(region),
        client_id=mp.mqtt_client_id(package, rd_device_id, uid),
        username=mp.mqtt_username(partner_identity, app_id, ch_key, sid, ecode),
        password=mp.mqtt_password(sign_key, ecode),
    )


@dataclass(frozen=True)
class GatewayRef:
    """A gateway we can talk to: its devId (topic), localKey (crypto) and protocol version."""

    # Kept out of ``repr``: a real device identifier and a secret.
    dev_id: str = field(repr=False)
    local_key: str = field(repr=False)
    pv: str = mp.PV_2_3


@dataclass
class CloudMqttLink:
    """Per-entry MQTT link. Pure routing/accounting here; I/O is in the async methods below.

    ``gateways`` maps gateway devId -> GatewayRef. ``on_frame(gateway_id, frame)`` is called per
    decoded inbound frame. A ``SeqOrigin`` is kept per gateway for the whole session (the receiver
    drops repeated (seq, origin), so it must not reset mid-session)."""

    credentials: MqttCredentials = field(repr=False)
    gateways: dict[str, GatewayRef] = field(repr=False)
    on_frame: Callable[[str, mp.Frame], None] | None = None
    # Optional: returns fresh credentials (e.g. after the integration refreshed the sid). Used on
    # reconnect so a rotated session is picked up instead of retrying stale credentials.
    creds_provider: Callable[[], MqttCredentials] | None = None
    # Optional: called (on the MQTT client's thread) at the first refusal of a streak of
    # not-authorised CONNACKs. The integration uses it to make an HTTP call, so a session
    # invalidated elsewhere is noticed and renewed at once instead of at the next poll; the
    # following reconnection then picks the renewed credentials up through ``creds_provider``.
    on_auth_refused: Callable[[], None] | None = None
    # Listen-only link (coverage measure): subscribes and decodes, and has NO send path —
    # ``can_publish`` is False, ``plan_dp`` and ``publish_dp`` raise. Set once, at construction.
    listen_only: bool = False
    # Plain counters, updated on the MQTT client's thread and read for the diagnostics.
    received: int = 0  # messages received on the inbound topics
    decoded: int = 0  # of which decoded and authenticated
    connects: int = 0  # accepted connections (1 + the reconnections)
    disconnects: int = 0
    auth_refusals: int = 0  # consecutive not-authorised CONNACKs (reset by an accepted one)
    last_connack_rc: int | None = None
    _state: str = STATE_IDLE
    _seq: dict[str, mp.SeqOrigin] = field(default_factory=dict, repr=False)  # keyed by devId
    _connected: bool = False
    _client: Any = field(default=None, repr=False)
    # Pending write confirmations: (node_id, dp_str, value, loop, future). Resolved true when an
    # inbound frame echoes that DP for that cid (the gateway's protocol-4 report). Guarded by
    # ``_waiters_lock`` across the asyncio loop thread and paho's network thread.
    _waiters: list[tuple[str, str, Any, Any, Any]] = field(default_factory=list, repr=False)
    _waiters_lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    # --- pure logic (unit-tested offline) ---

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def state(self) -> str:
        """One of the ``STATE_*`` values. ``connected`` is the only state that can publish."""
        return STATE_CONNECTED if self._connected else self._state

    def diagnostics(self) -> dict[str, Any]:
        """State and counters for the diagnostics. No credential, no identifier: the broker host
        is the public regional one, and the gateways are only counted."""
        return {
            "state": self.state,
            "connected": self._connected,
            "listen_only": self.listen_only,
            "host": self.credentials.host,
            "gateways": len(self.gateways),
            "connects": self.connects,
            "disconnects": self.disconnects,
            "auth_refusals": self.auth_refusals,
            "last_connack_rc": self.last_connack_rc,
            "received": self.received,
            "decoded": self.decoded,
        }

    def _on_inbound(self, gateway_id: str, frame: mp.Frame) -> None:
        """Dispatch a decoded inbound frame: resolve any matching write-confirmation waiter, then
        call ``on_frame``. Runs on paho's thread, so futures are set via their own loop."""
        data: dict[Any, Any] = frame.data if isinstance(frame.data, dict) else {}
        cid = data.get("cid")
        raw_dps = data.get("dps")
        dps: dict[Any, Any] = raw_dps if isinstance(raw_dps, dict) else {}
        with self._waiters_lock:
            snapshot = list(self._waiters)
        for node_id, dp_str, value, loop, fut in snapshot:
            if cid == node_id and dp_str in dps and dps[dp_str] == value and not fut.done():
                loop.call_soon_threadsafe(fut.set_result, True)
        if self.on_frame is not None:
            self.on_frame(gateway_id, frame)

    async def await_confirmation(
        self, node_id: str, dp: int, value: Any, *, timeout_s: float
    ) -> bool:
        """Wait up to ``timeout`` s for an inbound frame confirming ``dp==value`` on ``node_id``.
        Returns False on timeout (the caller then falls back to a cloud re-read)."""
        import asyncio

        loop = asyncio.get_running_loop()
        fut: Any = loop.create_future()
        waiter = (node_id, str(dp), value, loop, fut)
        with self._waiters_lock:
            self._waiters.append(waiter)
        try:
            return bool(await asyncio.wait_for(fut, timeout_s))
        except TimeoutError:
            return False
        finally:
            with self._waiters_lock:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

    def seq_for(self, gateway_id: str) -> mp.SeqOrigin:
        if gateway_id not in self._seq:
            self._seq[gateway_id] = mp.SeqOrigin()
        return self._seq[gateway_id]

    def can_publish(self, gateway_id: str) -> bool:
        """True only if the link is connected and that gateway's crypto material is known.
        Callers use this for entity availability — never assume a path that cannot actually send."""
        return self._connected and not self.listen_only and gateway_id in self.gateways

    def plan_dp(self, gateway_id: str, node_id: str, dps: dict[str | int, Any]) -> mp.PublishPlan:
        """Build the publish plan for a head (node_id) behind ``gateway_id``. Raises if the gateway
        is unknown or the link is listen-only. Does NOT send."""
        if self.listen_only:
            raise mp.FrameError("listen-only link: no publish")
        gw = self.gateways.get(gateway_id)
        if gw is None:
            # No identifier in the message: it surfaces in the UI and the log.
            raise mp.FrameError("unknown gateway (no local key for this head's gateway)")
        return mp.build_dp_publish(
            gateway_id=gw.dev_id,
            gateway_pv=gw.pv,
            local_key=gw.local_key,
            node_id=node_id,
            dps=dps,
            counter=self.seq_for(gateway_id),
        )

    def decode_inbound(self, topic: str, payload: bytes) -> tuple[str, mp.Frame] | None:
        """Decode a message on ``smart/mb/in/<gw>`` with that gateway's localKey. Returns
        (gateway_id, frame), or None if the topic/key is unknown or the frame fails to auth."""
        gateway_id = topic.rsplit("/", 1)[-1]
        gw = self.gateways.get(gateway_id)
        if gw is None:
            return None
        try:
            return gateway_id, mp.decode_frame(gw.local_key, payload)
        except mp.FrameError:
            return None

    def inbound_topics(self) -> list[str]:
        return [mp.topic_in(gw.dev_id) for gw in self.gateways.values()]

    # --- I/O (paho — bundled by HA; only on a gated connect) ---

    def start(self, *, ca_certs: str | None = None) -> None:
        """Open the live connection (paho, TLS), subscribe to every gateway's inbound topic, and
        dispatch decoded frames to ``on_frame``. Non-blocking (loop in a thread). The connection is
        read+write capable, but ``publish_dp`` is reached only behind the opt-in write gate, and
        refuses outright on a ``listen_only`` link."""
        import ssl

        import paho.mqtt.client as mqtt

        if ca_certs is None:
            try:
                import certifi

                ca_certs = certifi.where()
            except ImportError:
                ca_certs = None

        c = self.credentials

        def on_connect(client: Any, _u: Any, _f: Any, rc: int) -> None:
            self.last_connack_rc = rc
            self._connected = rc == 0
            if rc == 0:
                if self.connects or self.auth_refusals:
                    _LOGGER.info("cloud-mqtt link: reconnected")
                else:
                    _LOGGER.info("cloud-mqtt link: connected")
                self.connects += 1
                self.auth_refusals = 0
                self._state = STATE_CONNECTING  # what it falls back to on a drop
                for topic in self.inbound_topics():
                    client.subscribe(topic, qos=1)
                return
            if rc not in AUTH_REFUSED_RCS:
                _LOGGER.debug("cloud-mqtt link: CONNACK rc=%s (%s)", rc, mqtt.connack_string(rc))
                return
            self.auth_refusals += 1
            if self.auth_refusals == 1:
                _LOGGER.warning(
                    "cloud-mqtt link: connection refused (%s); retrying with the current session",
                    mqtt.connack_string(rc),
                )
                if self.on_auth_refused is not None:
                    try:
                        self.on_auth_refused()
                    except Exception as err:  # noqa: BLE001 - never break the MQTT thread
                        _LOGGER.debug(
                            "cloud-mqtt link: refusal hook failed: %s", type(err).__name__
                        )
            if self.auth_refusals >= MAX_AUTH_REFUSALS:
                # Stop for good rather than present refused credentials forever on a real account.
                self._state = STATE_REFUSED
                _LOGGER.error(
                    "cloud-mqtt link: stopped after %s refused connections; reload the integration "
                    "to try again",
                    self.auth_refusals,
                )
                client.disconnect()  # ends paho's reconnect loop

        def on_message(_client: Any, _u: Any, msg: Any) -> None:
            self.received += 1
            dec = self.decode_inbound(str(msg.topic), bytes(msg.payload))
            if dec is not None:
                self.decoded += 1
                self._on_inbound(*dec)

        def on_disconnect(client: Any, _u: Any, rc: int) -> None:
            if self._connected:
                self.disconnects += 1
            self._connected = False
            if self._state in (STATE_REFUSED, STATE_STOPPED):
                return  # closing: no reconnection follows, nothing to refresh
            # Refresh credentials before paho auto-reconnects, so a rotated sid is picked up rather
            # than retried stale. Best-effort on the loop thread.
            if self.creds_provider is not None:
                try:
                    fresh = self.creds_provider()
                    self.credentials = fresh
                    client.username_pw_set(fresh.username, fresh.password)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning(
                        "cloud-mqtt link: credential refresh failed: %s", type(err).__name__
                    )

        client = _new_paho_client(mqtt, c.client_id)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        client.username_pw_set(c.username, c.password)
        client.tls_set(ca_certs=ca_certs, cert_reqs=ssl.CERT_REQUIRED)
        # Exponential backoff on reconnect.
        client.reconnect_delay_set(min_delay=RECONNECT_MIN_DELAY, max_delay=RECONNECT_MAX_DELAY)
        self._client = client
        self._state = STATE_CONNECTING
        client.connect_async(c.host, c.port, keepalive=60)
        client.loop_start()
        # Keep the client id (it embeds the deviceId) out of the HA log's threadName.
        thread = getattr(client, "_thread", None)
        if thread is not None:
            thread.name = "danfoss-cloud-mqtt-link"

    def stop(self) -> None:
        """Close the connection and end the client's thread. Idempotent. ``disconnect`` first: it
        tells the broker we are leaving and ends paho's reconnect loop, so ``loop_stop`` (which
        joins the thread) returns promptly even from a backoff wait. Blocking: run in an executor.
        """
        self._connected = False
        if self._state != STATE_IDLE:
            self._state = STATE_STOPPED
        with self._waiters_lock:
            pending = list(self._waiters)
            self._waiters.clear()
        for _, _, _, loop, fut in pending:
            if not fut.done():
                loop.call_soon_threadsafe(fut.set_result, False)
        client = self._client
        self._client = None
        if client is None:
            return
        for close in (client.disconnect, client.loop_stop):
            try:
                close()
            except Exception as err:  # noqa: BLE001 - closing must not raise
                _LOGGER.debug("cloud-mqtt link: close step failed: %s", type(err).__name__)

    async def publish_dp(
        self, gateway_id: str, node_id: str, dps: dict[str | int, Any]
    ) -> mp.PublishPlan:
        """Publish a head DP over the live connection. GATED: a caller reaches this only behind the
        opt-in option and a real write request; raises if not connected or if the link is
        listen-only. Returns the plan sent. ``async`` for the caller's convenience; the paho publish
        itself is non-blocking."""
        if self.listen_only:
            raise mp.FrameError("listen-only link: no publish")
        if not self._connected or self._client is None:
            raise mp.FrameError("MQTT link not connected")
        plan = self.plan_dp(gateway_id, node_id, dps)
        self._client.publish(plan.topic, payload=plan.payload, qos=plan.qos, retain=plan.retain)
        return plan
