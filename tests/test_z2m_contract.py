"""Tests for the v2.x Zigbee2MQTT <-> DanfossDevice companion contract codec."""

from __future__ import annotations

import pytest

from pydanfoss_cloud import (
    DanfossDevice,
    classify_z2m_descriptor,
    decode_z2m_descriptor,
    decode_z2m_state,
    encode_z2m_descriptor,
    encode_z2m_set_command,
    encode_z2m_state,
    programming_mode_from_z2m,
    programming_mode_to_z2m,
)


def _sample_head() -> DanfossDevice:
    return DanfossDevice(
        id="dev-head-1",
        name="A-0 Classe/1",
        category="head",
        product_id="03wvrwpv",
        node_id="a1b2c3d4",
        mesh_id="gw-a0",
        online=True,
        temperature=20.4,
        setpoint=21.0,
        setpoints={"comfort": 21.0, "setback": 17.0, "pause": 12.0},
        mode="at_home",
        state="Heat",
        battery=88.0,
        valve_opening=45.0,
        load_estimate=420,
        heat_request=True,
        window_open=False,
        window_detection=True,
        child_lock=True,
        max_setpoint=28.0,
        min_setpoint=6.0,
        schedule_active=True,
        preheat=True,
        mounting=False,
        viewing_direction=False,
        orientation="Vertical",
        faults=[{"code": "E12", "label": "radio"}],
    )


def test_head_roundtrip_parity_between_danfoss2mqtt_and_zigbee2mqtt() -> None:
    """A head encoded by danfoss2mqtt and a native zigbee2mqtt 014G2461 decode to the same model."""
    src = _sample_head()
    desc_d2m = encode_z2m_descriptor(src)
    state_d2m = encode_z2m_state(
        src,
        local_attrs={
            "setpoint_source": "schedule",
            "radiator_covered": True,
            "heat_available": True,
            "regulation_speed": 5,
            "setpoint_offset": -0.5,
            "descale_day": "thursday",
            "descale_time": "11:00",
            "adaptation_run": "found",
            "window_state": "closed",
        },
    )

    assert desc_d2m["friendly_name"] == "A-0 Classe_1"
    assert desc_d2m["definition"]["model"] == "014G2461"
    assert state_d2m["programming_operation_mode"] == "schedule_with_preheat"
    assert state_d2m["keypad_lockout"] == "lock"
    assert state_d2m["thermostat_orientation"] is True
    assert state_d2m["algorithm_scale_factor"] == 5

    # 1) Decode from danfoss2mqtt descriptor + state
    dec_d2m = decode_z2m_descriptor(desc_d2m)
    assert dec_d2m is not None
    decode_z2m_state(dec_d2m, state_d2m, online=True)

    # 2) Decode from a native Zigbee2MQTT 014G2461 descriptor + state (no "danfoss" extension)
    native_z2m_desc = {
        "friendly_name": "A-0 Classe_1",
        "ieee_address": "0x00158d0001234567",
        "type": "EndDevice",
        "definition": {
            "vendor": "Danfoss",
            "model": "014G2461",
            "description": "Ally thermostat",
        },
    }
    native_z2m_state = {
        "local_temperature": 20.4,
        "occupied_heating_setpoint": 21.0,
        "pi_heating_demand": 45,
        "load_estimate": 420,
        "running_state": "heat",
        "battery": 88,
        "window_open_internal": "closed",
        "window_open_feature": True,
        "keypad_lockout": "lock",
        "max_heat_setpoint_limit": 28.0,
        "min_heat_setpoint_limit": 6.0,
        "heat_required": True,
        "mounted_mode_active": False,
        "programming_operation_mode": "schedule_with_preheat",
        "viewing_direction": False,
        "thermostat_orientation": True,
    }
    dec_z2m = decode_z2m_descriptor(native_z2m_desc)
    assert dec_z2m is not None
    decode_z2m_state(dec_z2m, native_z2m_state, online=True)

    # Both produce identical normalized operational fields in the Companion
    for attr in (
        "category",
        "online",
        "temperature",
        "setpoint",
        "valve_opening",
        "load_estimate",
        "heating",
        "battery",
        "window_open",
        "window_detection",
        "child_lock",
        "max_setpoint",
        "min_setpoint",
        "heat_request",
        "mounting",
        "schedule_active",
        "preheat",
        "viewing_direction",
        "orientation",
    ):
        assert getattr(dec_d2m, attr) == getattr(dec_z2m, attr), f"Mismatch on {attr}"

    # And the danfoss2mqtt extension preserves cloud-only setpoints, mode, and faults
    assert dec_d2m.setpoints == {"comfort": 21.0, "setback": 17.0, "pause": 12.0}
    assert dec_d2m.mode == "at_home"
    assert dec_d2m.faults == [{"code": "E12", "label": "radio"}]


def test_room_sensor_014g2480_and_relay_roundtrip() -> None:
    """Room sensor (014G2480 / mxzo5rhf) and boiler relay round-trip cleanly."""
    sensor = DanfossDevice(
        id="s1",
        name="A-0 Sonde",
        category="sensor",
        product_id="mxzo5rhf",
        online=True,
        temperature=19.8,
        humidity=47.5,
        battery=95.0,
    )
    s_desc = encode_z2m_descriptor(sensor)
    s_state = encode_z2m_state(sensor)
    assert s_desc["definition"]["model"] == "014G2480"
    assert s_state == {"temperature": 19.8, "humidity": 47.5, "battery": 95.0}

    # Native Z2M 014G2480 descriptor
    dec_s = decode_z2m_descriptor(
        {
            "friendly_name": "A-0 Sonde",
            "ieee_address": "0x00158d0009999999",
            "definition": {"vendor": "Danfoss", "model": "014G2480"},
        }
    )
    assert dec_s is not None and dec_s.is_sensor
    decode_z2m_state(dec_s, s_state)
    assert dec_s.temperature == 19.8 and dec_s.humidity == 47.5 and dec_s.battery == 95.0

    # Boiler relay
    relay = DanfossDevice(
        id="r1",
        name="Boiler Relay",
        category="relay",
        product_id="ypieax1h",
        online=True,
        relay_on=True,
    )
    r_desc = encode_z2m_descriptor(relay)
    r_state = encode_z2m_state(relay)
    assert r_state == {"state": "ON", "relay_on": True}
    dec_r = decode_z2m_descriptor(r_desc)
    assert dec_r is not None and dec_r.is_relay
    decode_z2m_state(dec_r, {"state": "OFF"})
    assert dec_r.relay_on is False

    # Relay with unknown state encodes empty dict
    assert encode_z2m_state(DanfossDevice(id="r2", name="R", category="relay", relay_on=None)) == {}


def test_non_danfoss_z2m_devices_are_ignored_and_edge_cases() -> None:
    """Non-Danfoss devices on Zigbee2MQTT are skipped; fallback vendor classification works."""
    non_danfoss = {"friendly_name": "Sonoff", "definition": {"vendor": "SONOFF"}}
    assert decode_z2m_descriptor(non_danfoss) is None
    assert (
        classify_z2m_descriptor(
            {"definition": {"vendor": "Danfoss", "description": "Room humidity sensor"}}
        )
        == "sensor"
    )
    assert (
        classify_z2m_descriptor(
            {"definition": {"vendor": "Danfoss", "description": "Boiler relay unit"}}
        )
        == "relay"
    )
    assert (
        classify_z2m_descriptor(
            {"definition": {"vendor": "Danfoss", "description": "Radiator valve"}}
        )
        == "head"
    )

    # Programming mode conversions
    assert programming_mode_to_z2m(None, None) is None
    assert programming_mode_to_z2m(False, True) == "setpoint"
    assert programming_mode_to_z2m(True, False) == "schedule"
    assert programming_mode_from_z2m("setpoint") == (False, False)
    assert programming_mode_from_z2m("schedule") == (True, False)
    assert programming_mode_from_z2m("eco") == (True, True)
    assert programming_mode_from_z2m("unknown") == (None, None)
    assert programming_mode_from_z2m(123) == (None, None)

    # Scheduled setpoint fallback & boolean window_open_internal / keypad_lockout
    head = DanfossDevice(id="h2", name="H2", category="head")
    decode_z2m_state(
        head,
        {
            "occupied_heating_setpoint_scheduled": 19.5,
            "window_open_internal": True,
            "keypad_lockout": False,
        },
    )
    assert head.setpoint == 19.5 and head.window_open is True and head.child_lock is False


def test_encode_z2m_set_command_validation() -> None:
    """Companion command builder produces canonical <prefix>/<name>/set JSON and checks ranges."""
    cmd = encode_z2m_set_command(
        setpoint=20.5,
        max_setpoint=27.0,
        min_setpoint=15.0,
        child_lock=True,
        window_detection=False,
        preheat=True,
        external_sensor_temp_c=19.75,
        window_open_external=False,
        load_room_mean=42,
    )
    assert cmd == {
        "occupied_heating_setpoint": 20.5,
        "max_heat_setpoint_limit": 27.0,
        "min_heat_setpoint_limit": 15.0,
        "keypad_lockout": "lock",
        "window_open_feature": False,
        "programming_operation_mode": "schedule_with_preheat",
        "external_measured_room_sensor": 1975,
        "window_open_external": False,
        "load_room_mean": 42,
    }

    with pytest.raises(ValueError, match="out of range"):
        encode_z2m_set_command(setpoint=40.0)
    with pytest.raises(ValueError, match="out of range"):
        encode_z2m_set_command(setpoint=20.3)
    with pytest.raises(ValueError, match="empty"):
        encode_z2m_set_command()


def test_decode_z2m_2x_string_enums_from_real_hardware() -> None:
    """Real Zigbee2MQTT 2.x payloads send string enums for orientation, display, and lockout."""
    head = DanfossDevice(id="0x0011223344556677", name="Room A", category="head", online=True)
    decode_z2m_state(
        head,
        {
            "local_temperature": 15.62,
            "occupied_heating_setpoint": 14,
            "pi_heating_demand": 78,
            "battery": 66,
            "keypad_lockout": "unlock",
            "window_open_feature": True,
            "viewing_direction": "normal",
            "thermostat_orientation": "horizontal",
            "programming_operation_mode": "schedule_with_preheat",
            "running_state": "heat",
        },
    )
    assert head.viewing_direction is False
    assert head.orientation == "Horizontal"
    assert head.child_lock is False
    assert head.preheat is True
    assert head.heating is True

    decode_z2m_state(
        head,
        {
            "keypad_lockout": "lock1",
            "viewing_direction": "flipped",
            "thermostat_orientation": "vertical",
        },
    )
    assert head.child_lock is True
    assert head.viewing_direction is True
    assert head.orientation == "Vertical"


def test_window_open_internal_is_published_as_zigbee2mqtt_text() -> None:
    """Regression (real install, 2026-11-27): the cloud push gives the raw window state (1 =
    closed) and the bridge published ``window_open_internal: 1``. Zigbee2MQTT publishes the name."""
    from pydanfoss_cloud.z2m_contract import window_open_internal_text

    assert [window_open_internal_text(i) for i in range(5)] == [
        "quarantine",
        "closed",
        "hold",
        "open",
        "external_open",
    ]
    assert window_open_internal_text("Open") == "open"
    for junk in (None, True, 9, "ajar", 1.0):
        assert window_open_internal_text(junk) is None

    src = _sample_head()
    assert encode_z2m_state(src, local_attrs={"window_state": 1})["window_open_internal"] == (
        "closed"
    )
    assert encode_z2m_state(src, local_attrs={"window_state": 3})["window_open_internal"] == "open"
    assert "window_open_internal" not in encode_z2m_state(src, local_attrs={"window_state": 7})
    # Decoded back, a published "open" reads as an open window; "closed" as a closed one.
    head = _sample_head()
    decode_z2m_state(head, {"window_open_internal": "open"}, online=True)
    assert head.window_open is True
