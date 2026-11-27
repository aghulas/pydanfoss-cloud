"""Head settings written by the standalone bridge (contract §4.2 channels). Synthetic data only."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydanfoss_cloud import bridge_daemon
from pydanfoss_cloud.bridge_daemon import StandaloneDanfoss2MqttDaemon, write_head_settings
from pydanfoss_cloud.client import CloudError, CloudWriteError
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.head_settings import (
    GROUP_SETTING_KEYS,
    MQTT_SETTING_KEYS,
    HeadSettingsGate,
    allowed_setting_keys,
    setting_dps,
)
from pydanfoss_cloud.rooms import RoomState


def _head(dev_id: str = "dev_iso", **kw: Any) -> DanfossDevice:
    base: dict[str, Any] = dict(
        id=dev_id,
        name=f"Tete {dev_id}",
        category="head",
        product_id="03wvrwpv",
        node_id=f"node_{dev_id}",
        mesh_id="gw_1",
        online=True,
        setpoints={"pause": 5.0, "comfort": 21.0},
        child_lock=False,
    )
    base.update(kw)
    return DanfossDevice(**base)


OPEN = HeadSettingsGate(enabled=True)


# --- pure logic --------------------------------------------------------------------------------


def test_gate_closed_by_default_and_trial_confines_to_one_head() -> None:
    head = _head()
    assert allowed_setting_keys(head, gate=HeadSettingsGate(), grouped=True, mqtt_ready=True) == (
        frozenset()
    )
    trial = HeadSettingsGate(enabled=True, trial_head="dev_iso")
    assert allowed_setting_keys(head, gate=trial, grouped=False, mqtt_ready=True) == (
        MQTT_SETTING_KEYS
    )
    other = _head("dev_other")
    assert allowed_setting_keys(other, gate=trial, grouped=False, mqtt_ready=True) == frozenset()


def test_keys_follow_the_proven_channel() -> None:
    head = _head()
    # Grouped: the room's group publish, no lower bound (no group route for DP 27).
    assert allowed_setting_keys(head, gate=OPEN, grouped=True, mqtt_ready=False) == (
        GROUP_SETTING_KEYS
    )
    assert "min_heat_setpoint_limit" not in GROUP_SETTING_KEYS
    # Isolated: cloud MQTT only, and only while the link can reach the gateway.
    assert allowed_setting_keys(head, gate=OPEN, grouped=False, mqtt_ready=True) == (
        MQTT_SETTING_KEYS
    )
    assert allowed_setting_keys(head, gate=OPEN, grouped=False, mqtt_ready=False) == frozenset()
    # Not a head, or an unverified model: nothing.
    relay = DanfossDevice(id="r", name="R", category="relay")
    assert allowed_setting_keys(relay, gate=OPEN, grouped=False, mqtt_ready=True) == frozenset()
    odd = _head(product_id="unknown")
    assert allowed_setting_keys(odd, gate=OPEN, grouped=False, mqtt_ready=True) == frozenset()


def test_setting_dps_translation_and_validation() -> None:
    head = _head()
    assert setting_dps({"keypad_lockout": "lock"}, head) == {30: True}
    assert setting_dps({"keypad_lockout": "unlock"}, head) == {30: False}
    assert setting_dps({"child_lock": True}, head) == {30: True}
    assert setting_dps({"window_open_feature": False}, head) == {116: False}
    assert setting_dps({"max_heat_setpoint_limit": 28.0}, head) == {18: 280}
    assert setting_dps({"min_heat_setpoint_limit": 5}, head) == {27: 50}
    # A lower bound above the pause setpoint carries the pause setpoint along, as the app does.
    assert setting_dps({"min_heat_setpoint_limit": 12.0}, head) == {27: 120, 120: 120}
    for bad in (
        {"keypad_lockout": "maybe"},
        {"keypad_lockout": "lock", "child_lock": False},
        {"window_open_feature": "yes"},
        {"max_heat_setpoint_limit": 40.0},  # out of range
        {"max_heat_setpoint_limit": 28.3},  # off the 0.5 °C step
        {"max_heat_setpoint_limit": True},
        {"min_heat_setpoint_limit": 15.0, "max_heat_setpoint_limit": 15.0},
    ):
        with pytest.raises(CloudWriteError):
            setting_dps(bad, head)
    with pytest.raises(CloudWriteError, match="pause"):
        setting_dps({"min_heat_setpoint_limit": 10.0}, _head(setpoints={}))


# --- daemon: advertised keys -------------------------------------------------------------------


class _MemTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self.subs: dict[str, Any] = {}

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: Any) -> None:
        self.subs[topic] = callback


class _Link:
    """Stand-in for the publishing cloud-MQTT link."""

    def __init__(self, *, echo: bool = True, connected: bool = True) -> None:
        self.echo = echo
        self.connected = connected
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def can_publish(self, gateway_id: str) -> bool:
        return self.connected and gateway_id == "gw_1"

    async def await_confirmation(self, node_id: str, dp: int, value: Any, *, timeout_s: float):
        return self.echo

    async def publish_dp(self, gateway_id: str, node_id: str, dps: dict[str, Any]) -> None:
        self.sent.append((gateway_id, node_id, dps))


def _room(members: tuple[str, ...]) -> RoomState:
    return RoomState(
        group_id="g1", name="Salle", pid="03wvrwpv", head_count=len(members), members=members
    )


def _descriptors(transport: _MemTransport) -> dict[str, dict[str, Any]]:
    payload = next(p for t, p, _ in reversed(transport.published) if t.endswith("bridge/devices"))
    return {d["friendly_name"]: d for d in json.loads(payload)}


def test_set_keys_are_advertised_per_head() -> None:
    transport = _MemTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport, base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    grouped, isolated = _head("dev_grp"), _head("dev_iso")
    # Closed gate: only the setpoint, as before.
    daemon.publish_fleet([grouped, isolated], rooms=[_room(("dev_grp",))])
    desc = _descriptors(transport)
    assert desc["Tete dev_grp"]["danfoss"]["set_keys"] == ["occupied_heating_setpoint"]
    assert desc["Tete dev_iso"]["danfoss"]["set_keys"] == ["occupied_heating_setpoint"]
    # Open gate, no publishing link: grouped heads only.
    daemon.head_settings_gate = OPEN
    daemon.publish_fleet([grouped, isolated], rooms=[_room(("dev_grp",))])
    desc = _descriptors(transport)
    assert "keypad_lockout" in desc["Tete dev_grp"]["danfoss"]["set_keys"]
    assert "min_heat_setpoint_limit" not in desc["Tete dev_grp"]["danfoss"]["set_keys"]
    assert desc["Tete dev_iso"]["danfoss"]["set_keys"] == ["occupied_heating_setpoint"]
    # With a connected link the isolated head opens too, lower bound included.
    daemon.write_link = _Link()  # type: ignore[assignment]
    daemon.publish_fleet([grouped, isolated], rooms=None)  # rooms unknown this cycle: kept
    desc = _descriptors(transport)
    assert "min_heat_setpoint_limit" in desc["Tete dev_iso"]["danfoss"]["set_keys"]
    assert "min_heat_setpoint_limit" not in desc["Tete dev_grp"]["danfoss"]["set_keys"]


# --- daemon: writes ----------------------------------------------------------------------------


def _daemon(*, grouped: tuple[str, ...] = (), link: _Link | None = None, gate=OPEN):
    daemon = StandaloneDanfoss2MqttDaemon(_MemTransport(), base_topic="danfoss2mqtt")  # type: ignore[arg-type]
    daemon.head_settings_gate = gate
    daemon.write_link = link  # type: ignore[assignment]
    daemon._grouped = set(grouped)
    return daemon


def _client(**kw: Any) -> SimpleNamespace:
    base = dict(
        async_resolve_group=AsyncMock(return_value=("g1", "03wvrwpv")),
        async_group_publish=AsyncMock(return_value=True),
        async_get_device_dps=AsyncMock(return_value=None),
    )
    base.update(kw)
    return SimpleNamespace(**base)


async def test_grouped_head_goes_through_the_room_group_publish() -> None:
    client, head = _client(), _head("dev_grp")
    daemon = _daemon(grouped=("dev_grp",))
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        daemon,
        head,
        {"keypad_lockout": "lock", "max_heat_setpoint_limit": 28.0},
    )
    assert written and handled == {"keypad_lockout", "max_heat_setpoint_limit"}
    client.async_group_publish.assert_awaited_once_with("g1", {"30": True, "18": 280})


async def test_isolated_head_goes_through_cloud_mqtt_confirmed_by_echo() -> None:
    link, client, head = _Link(), _client(), _head("dev_iso")
    daemon = _daemon(link=link)
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        daemon,
        head,
        {"keypad_lockout": "lock"},
    )
    assert written and handled == {"keypad_lockout"}
    assert link.sent == [("gw_1", "node_dev_iso", {"30": True})]
    client.async_group_publish.assert_not_awaited()
    client.async_get_device_dps.assert_not_awaited()


async def test_isolated_head_without_echo_is_confirmed_by_the_cloud_or_refused(
    caplog: pytest.LogCaptureFixture,
) -> None:
    head = _head("dev_iso")
    ok = _client(async_get_device_dps=AsyncMock(return_value={"30": True}))
    handled, written = await write_head_settings(
        ok,  # type: ignore[arg-type]
        _daemon(link=_Link(echo=False)),
        head,
        {"keypad_lockout": "lock"},
    )
    assert written
    stale = _client(async_get_device_dps=AsyncMock(return_value={"30": False}))
    handled, written = await write_head_settings(
        stale,  # type: ignore[arg-type]
        _daemon(link=_Link(echo=False)),
        head,
        {"keypad_lockout": "lock"},
    )
    assert not written and handled == {"keypad_lockout"}
    assert "not confirmed" in caplog.text


async def test_nothing_is_written_when_the_head_does_not_accept_the_key() -> None:
    client, link = _client(), _Link()
    # Gate closed.
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        _daemon(link=link, gate=HeadSettingsGate()),
        _head("dev_iso"),
        {"keypad_lockout": "lock"},
    )
    assert (handled, written) == (set(), False)
    # Trial on another head.
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        _daemon(link=link, gate=HeadSettingsGate(enabled=True, trial_head="dev_x")),
        _head("dev_iso"),
        {"keypad_lockout": "lock"},
    )
    assert (handled, written) == (set(), False)
    # Grouped head: the lower bound has no group route.
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        _daemon(grouped=("dev_grp",)),
        _head("dev_grp"),
        {"min_heat_setpoint_limit": 6.0},
    )
    assert (handled, written) == (set(), False)
    assert link.sent == []
    client.async_group_publish.assert_not_awaited()


async def test_a_refused_value_or_a_cloud_error_writes_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    link, client = _Link(), _client()
    handled, written = await write_head_settings(
        client,  # type: ignore[arg-type]
        _daemon(link=link),
        _head("dev_iso"),
        {"max_heat_setpoint_limit": 99.0},
    )
    assert handled == {"max_heat_setpoint_limit"} and not written and link.sent == []
    failing = _client(async_group_publish=AsyncMock(side_effect=CloudError("boom")))
    handled, written = await write_head_settings(
        failing,  # type: ignore[arg-type]
        _daemon(grouped=("dev_grp",)),
        _head("dev_grp"),
        {"keypad_lockout": "lock"},
    )
    assert not written
    assert "refused" in caplog.text and "failed" in caplog.text


def test_options_open_the_gate_only_when_asked(tmp_path) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"base_topic": "danfoss2mqtt"}), encoding="utf-8")
    args = bridge_daemon.build_arg_parser().parse_args(["--options", str(options)])
    config = bridge_daemon.load_bridge_config(args)
    assert config.head_settings_writes is False and config.head_settings_trial_head is None
    options.write_text(
        json.dumps({"head_settings_writes": True, "head_settings_trial_head": " dev_iso "}),
        encoding="utf-8",
    )
    config = bridge_daemon.load_bridge_config(args)
    assert config.head_settings_writes is True and config.head_settings_trial_head == "dev_iso"
    options.write_text(json.dumps({"head_settings_writes": "nope"}), encoding="utf-8")
    assert bridge_daemon.load_bridge_config(args).head_settings_writes is False
