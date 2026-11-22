"""Unit tests for the DP 199 cloud-MQTT push decoder and the bridge overlay (``cloud_dp199``).

All frames here are SYNTHETIC: they are built from the public ZCL layout, never captured on real
hardware, and carry no real device identifier. Battery/temperature values are plain measurements.
"""

from __future__ import annotations

import base64

from pydanfoss_cloud import cloud_dp199
from pydanfoss_cloud.bridge_daemon import StandaloneDanfoss2MqttDaemon
from pydanfoss_cloud.cloud_dp199 import HeadOverlay, decode_dp199
from pydanfoss_cloud.decode import DanfossDevice
from pydanfoss_cloud.local import tunnel

# --- synthetic DP 199 frame builders ----------------------------------------------------------

MFR = tunnel.MANUFACTURER  # 0x1246


def _encode_value(typ: int, value: int) -> bytes:
    n = tunnel._LENGTH[typ]
    return bytes([typ]) + int(value).to_bytes(n, "little", signed=typ in tunnel._SIGNED)


def report_frame(
    cluster: int,
    records: list[tuple[int, int, int]],
    *,
    manufacturer: int | None = None,
    seq: int = 0x40,
) -> bytes:
    """ZCL Report Attributes (0x0A). ``records`` = [(attribute, zcl_type, raw_value)]."""
    if manufacturer is not None:
        zcl = bytes([0x04]) + manufacturer.to_bytes(2, "little") + bytes([seq, 0x0A])
    else:
        zcl = bytes([0x00, seq, 0x0A])
    for attr, typ, val in records:
        zcl += attr.to_bytes(2, "little") + _encode_value(typ, val)
    return tunnel.header(cluster) + zcl


def read_response_frame(
    records: list[tuple[int, int, int, int]],
    *,
    cluster: int = tunnel.CLUSTER_THERMOSTAT,
    manufacturer: int | None = None,
    seq: int = 0x40,
) -> bytes:
    """ZCL Read Attributes Response (0x01). ``records`` = [(attribute, status, zcl_type, value)];
    a non-zero status omits the type+value (unsupported attribute)."""
    if manufacturer is not None:
        zcl = bytes([0x04]) + manufacturer.to_bytes(2, "little") + bytes([seq, 0x01])
    else:
        zcl = bytes([0x00, seq, 0x01])
    for attr, status, typ, val in records:
        rec = attr.to_bytes(2, "little") + bytes([status])
        if status == 0:
            rec += _encode_value(typ, val)
        zcl += rec
    return tunnel.header(cluster) + zcl


def b64(frame: bytes) -> str:
    return base64.b64encode(frame).decode()


# --- decode: Report Attributes ----------------------------------------------------------------


def test_report_local_temperature_thermostat() -> None:
    # 0x0000 LocalTemperature, int16 hundredths: 2045 -> 20.45 °C.
    decoded = decode_dp199(report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0000, 0x29, 2045)]))
    assert decoded is not None
    assert decoded.command == cloud_dp199.CMD_REPORT_ATTRIBUTES
    assert decoded.manufacturer is None
    assert decoded.attributes == {"local_temperature": 20.45}
    assert decoded.raw == {}


def test_report_manufacturer_window_adaptation_load() -> None:
    frame = report_frame(
        tunnel.CLUSTER_THERMOSTAT,
        [
            (0x4000, 0x30, 1),  # window_state enum8 (raw enum kept, not asserted as a bool)
            (0x404D, 0x18, 2),  # adaptation_state bitmap8 -> adaptation_done bit set
            (0x404A, 0x29, -4),  # load_estimate int16 (signed)
        ],
        manufacturer=MFR,
    )
    decoded = decode_dp199(frame)
    assert decoded is not None
    assert decoded.manufacturer == MFR
    assert decoded.attributes["window_state"] == 1
    assert decoded.attributes["adaptation_state"] == 2
    assert decoded.attributes["adaptation_done"] is True
    assert decoded.attributes["load_estimate"] == -4


def test_report_battery_power_cluster_half_percent() -> None:
    # Power cluster 0x0001, 0x0021 BatteryPercentageRemaining in HALF percent: 126 -> 63 %.
    decoded = decode_dp199(report_frame(cloud_dp199.CLUSTER_POWER, [(0x0021, 0x20, 126)]))
    assert decoded is not None
    assert decoded.cluster == cloud_dp199.CLUSTER_POWER
    assert decoded.attributes == {"battery": 63.0}


def test_report_diagnostics_sw_error_to_faults() -> None:
    # Diagnostics cluster 0x0B05, manufacturer, 0x4000 bitmap16: bit 13 set -> E14 (low battery).
    frame = report_frame(
        cloud_dp199.CLUSTER_DIAGNOSTICS, [(0x4000, 0x21, 1 << 13)], manufacturer=MFR
    )
    decoded = decode_dp199(frame)
    assert decoded is not None
    assert "sw_error_code" not in decoded.attributes
    assert [f["code"] for f in decoded.faults] == ["E14"]


def test_report_diagnostics_all_clear() -> None:
    frame = report_frame(cloud_dp199.CLUSTER_DIAGNOSTICS, [(0x4000, 0x21, 0)], manufacturer=MFR)
    decoded = decode_dp199(frame)
    assert decoded is not None
    assert decoded.faults == []


# --- decode: Read Attributes Response (the survey answer seen from the cloud) ------------------


def test_read_attributes_response_programming_mode_and_setpoints() -> None:
    frame = read_response_frame(
        [
            (0x0012, 0, 0x29, 2100),  # current_setpoint hundredths -> 21.0
            (0x0015, 0, 0x29, 500),  # min_setpoint -> 5.0
            (0x0016, 0, 0x29, 3500),  # max_setpoint -> 35.0
            (0x0025, 0, 0x18, 3),  # programming_mode: schedule + preheat
            (0x0030, 0, 0x30, 1),  # setpoint_source: externally
        ]
    )
    decoded = decode_dp199(frame)
    assert decoded is not None
    assert decoded.command == cloud_dp199.CMD_READ_ATTRIBUTES_RESPONSE
    assert decoded.attributes["current_setpoint"] == 21.0
    assert decoded.attributes["min_setpoint"] == 5.0
    assert decoded.attributes["max_setpoint"] == 35.0
    assert decoded.attributes["programming_mode"] == 3
    assert decoded.attributes["preheat"] is True
    assert decoded.attributes["setpoint_source"] == 1


def test_read_response_unsupported_attribute_is_dropped_not_guessed() -> None:
    frame = read_response_frame(
        [(0x0012, 0, 0x29, 2100), (0x0025, 0x86, 0x00, 0)]  # second: UNSUPPORTED_ATTRIBUTE
    )
    decoded = decode_dp199(frame)
    assert decoded is not None
    assert decoded.attributes == {"current_setpoint": 21.0}
    assert "programming_mode" not in decoded.attributes


# --- decode: honesty rules --------------------------------------------------------------------


def test_unknown_attribute_kept_raw_not_named() -> None:
    # 0x0099 is in no table: surfaced raw, hex-keyed, never named/guessed.
    decoded = decode_dp199(report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0099, 0x20, 5)]))
    assert decoded is not None
    assert decoded.attributes == {}
    assert decoded.raw == {"0x0099": 5}


def test_unhandled_command_returns_none() -> None:
    # A Default Response (0x0B) is neither a read response nor a report: nothing to decode.
    frame = tunnel.header(tunnel.CLUSTER_THERMOSTAT) + bytes([0x00, 0x40, 0x0B, 0x02, 0x00])
    assert decode_dp199(frame) is None


def test_unknown_cluster_surfaces_everything_raw() -> None:
    # A report on a cluster with no naming table: every attribute falls through to raw.
    decoded = decode_dp199(report_frame(0x0402, [(0x0000, 0x29, 123)]))
    assert decoded is not None
    assert decoded.attributes == {}
    assert decoded.raw == {"0x0000": 123}


def test_overlay_apply_to_without_device_fields_returns_same_object() -> None:
    base = DanfossDevice(id="d1", name="Office", category="head")
    overlay = HeadOverlay()
    assert overlay.apply_to(base) is base


def test_poll_control_checkin_returns_none() -> None:
    # Cluster 0x0020 poll control, a cluster-specific command (fc bit 0 set): nothing to read.
    frame = tunnel.header(0x0020) + bytes([0x01, 0x40, 0x00])
    assert decode_dp199(frame) is None


def test_non_zcl_bytes_return_none() -> None:
    assert decode_dp199(b"\x00\x01\x02") is None
    assert decode_dp199("not valid base64 !!!") is None


def test_accepts_base64_string_like_the_channel() -> None:
    frame = report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0000, 0x29, 2045)])
    assert decode_dp199(b64(frame)) == decode_dp199(frame)


# --- HeadOverlay ------------------------------------------------------------------------------


def test_overlay_merge_projects_device_fields_and_local_attrs() -> None:
    overlay = HeadOverlay()
    changed = overlay.merge(
        decode_dp199(
            read_response_frame(
                [
                    (0x0012, 0, 0x29, 2100),
                    (0x0025, 0, 0x18, 3),
                    (0x0030, 0, 0x30, 1),
                ]
            )
        ),
        now=1000.0,
    )
    assert changed is True
    assert overlay.device_fields["setpoint"] == 21.0
    assert overlay.device_fields["schedule_active"] is True
    assert overlay.device_fields["preheat"] is True
    assert overlay.local_attrs["setpoint_source"] == 1
    assert overlay.updated == 1000.0


def test_overlay_is_opportunistic_quiet_attribute_keeps_previous() -> None:
    overlay = HeadOverlay()
    overlay.merge(
        decode_dp199(report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0000, 0x29, 2045)])), now=1.0
    )
    overlay.merge(
        decode_dp199(read_response_frame([(0x0012, 0, 0x29, 2100)])),
        now=2.0,
    )
    # The temperature push is not resent by the setpoint frame, but the overlay keeps it.
    assert overlay.device_fields["temperature"] == 20.45
    assert overlay.device_fields["setpoint"] == 21.0
    assert overlay.updated == 2.0


def test_overlay_apply_to_does_not_mutate_base_device() -> None:
    base = DanfossDevice(id="d1", name="Office", category="head", temperature=18.0)
    overlay = HeadOverlay()
    overlay.merge(
        decode_dp199(report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0000, 0x29, 2045)])), now=1.0
    )
    merged = overlay.apply_to(base)
    assert merged.temperature == 20.45
    assert base.temperature == 18.0  # unchanged
    assert merged is not base


def test_overlay_merge_raw_only_frame_does_not_request_republish() -> None:
    overlay = HeadOverlay()
    changed = overlay.merge(
        decode_dp199(report_frame(tunnel.CLUSTER_THERMOSTAT, [(0x0099, 0x20, 5)])), now=5.0
    )
    assert changed is False
    assert overlay.raw == {"0x0099": 5}
    assert overlay.updated == 5.0  # a raw-only read is still dated


# --- bridge integration -----------------------------------------------------------------------


class _RecordingTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []
        self.subs: dict[str, object] = {}

    def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def subscribe(self, topic: str, callback: object) -> None:
        self.subs[topic] = callback


def _head_with_node() -> DanfossDevice:
    return DanfossDevice(
        id="dev_1",
        name="Office",
        category="head",
        product_id="03wvrwpv",
        node_id="abcd",
        online=True,
        temperature=19.0,
        setpoint=20.0,
        mode="at_home",
    )


def _last_state(transport: _RecordingTransport, friendly: str) -> dict[str, object]:
    import json

    payloads = [p for t, p, _ in transport.published if t == f"danfoss2mqtt/{friendly}"]
    assert payloads, f"no state published for {friendly}"
    return json.loads(payloads[-1])


def test_bridge_apply_cloud_push_overlays_and_republishes() -> None:
    transport = _RecordingTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport)
    daemon.publish_fleet([_head_with_node()])

    frame = read_response_frame(
        [
            (0x0012, 0, 0x29, 2250),  # 22.5 °C
            (0x0025, 0, 0x18, 3),  # schedule + preheat
            (0x0030, 0, 0x30, 1),  # setpoint source: externally
        ]
    )
    updated = daemon.apply_cloud_push("abcd", {"199": b64(frame)})
    assert updated is not None

    state = _last_state(transport, "Office")
    assert state["occupied_heating_setpoint"] == 22.5
    assert state["programming_operation_mode"] == "schedule_with_preheat"
    assert state["setpoint_change_source"] == 1


def test_bridge_apply_cloud_push_ignored_for_unknown_or_empty() -> None:
    transport = _RecordingTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport)
    daemon.publish_fleet([_head_with_node()])
    before = len(transport.published)

    known = b64(report_frame(0x0201, [(0x0000, 0x29, 1)]))
    raw_only = b64(report_frame(0x0201, [(0x0099, 0x20, 5)]))
    assert daemon.apply_cloud_push("ffff", {"199": known}) is None  # unknown head
    assert daemon.apply_cloud_push("abcd", {"24": 200}) is None  # no DP 199
    assert daemon.apply_cloud_push("abcd", {"199": raw_only}) is None  # nothing surfaced
    assert len(transport.published) == before  # nothing republished


def test_bridge_local_tunnel_attr_wins_over_cloud_push() -> None:
    transport = _RecordingTransport()
    daemon = StandaloneDanfoss2MqttDaemon(transport)
    # A local-tunnel survey already supplied setpoint_source=0 for this head.
    daemon.publish_fleet([_head_with_node()], local_attrs={"dev_1": {"setpoint_source": 0}})

    frame = read_response_frame([(0x0030, 0, 0x30, 1)])  # cloud push says setpoint_source=1
    daemon.apply_cloud_push("abcd", {"199": b64(frame)})

    state = _last_state(transport, "Office")
    # Tunnel (on-demand reader) wins on a key present in both sources.
    assert state["setpoint_change_source"] == 0
