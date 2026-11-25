"""Danfoss Ally access layer - reusable library, with no Home Assistant dependency.

This folder IS the `pydanfoss-cloud` library (see README.md here): the repo's `pyproject.toml`
packages it under the name `pydanfoss_cloud` without moving the files. The integration "vendors" it
by relative import (`from .api import …`) to stay self-contained for HACS; the MCP connector can
depend on it via PyPI once published, then the integration will switch to manifest.json ->
requirements.

Reuse (see docs/reuse.md):
  - client and app signature     : ~/dev/danfoss-ally-signing-re
  - key fetching, DPs            : ~/dev/danfoss-ally-mcp (tuya/pilote_tuya.py)
  - Zigbee tunnel DP 198/199     : ~/dev/danfoss-ally-mcp (local.py)
"""

from __future__ import annotations

from . import schedule, z2m_contract
from .client import (
    CloudAuthError,
    CloudClient,
    CloudError,
    CloudWriteError,
    ScheduleValidationError,
    UnknownTargetError,
    derive_distinct_device_id,
)
from .cloud_dp199 import Dp199Decode, HeadOverlay, decode_dp199
from .decode import (
    DanfossDevice,
    apply_dps_overlay,
    decode_device,
    decode_operation,
    head_state,
    holiday_running,
    holiday_temperature,
)
from .keys import GatewayKey, async_fetch_gateway_keys
from .messages import DanfossMessage, parse_messages
from .room_coordinator import RoomCoordinator
from .room_store import CoordinatorHoliday, CoordinatorRoom, RoomStore, RoomStoreError
from .room_view import build_heat_demand, build_holidays_view, build_room_view
from .schedule import Schedule, count_transitions, validate_days
from .write_job import (
    HEAD_APPLIED,
    HEAD_MISMATCH,
    HEAD_UNREACHABLE,
    JOB_APPLIED,
    JOB_FAILED,
    JOB_PARTIAL,
    JOB_UNCONFIRMED,
    HeadWriteResult,
    HolidayValidationError,
    ScheduleWriteValidationError,
    SetpointJob,
    SetpointValidationError,
    SettingsValidationError,
    property_for_source,
    run_setpoint_job,
    validate_setpoint,
)
from .writes import (
    MQTT_CANDIDATE_DPS,
    MQTT_CLOUD_CONFIRMED_DPS,
    MQTT_ENUM_VALUES,
    MQTT_TENTHS_RANGE,
    MQTT_TUNNEL_CONFIRMED_DPS,
    MQTT_WRITE_DPS,
    encode_setpoint,
    mqtt_dp_open,
    mqtt_write_value,
    write_allowed,
)
from .z2m_contract import (
    classify_z2m_descriptor,
    clean_friendly_name,
    decode_z2m_descriptor,
    decode_z2m_state,
    encode_z2m_descriptor,
    encode_z2m_set_command,
    encode_z2m_state,
    programming_mode_from_z2m,
    programming_mode_to_z2m,
)

__all__ = [
    "CloudAuthError",
    "CloudClient",
    "CloudError",
    "CloudWriteError",
    "CoordinatorHoliday",
    "CoordinatorRoom",
    "DanfossDevice",
    "DanfossMessage",
    "Dp199Decode",
    "GatewayKey",
    "HeadOverlay",
    "HeadWriteResult",
    "HolidayValidationError",
    "RoomCoordinator",
    "RoomStore",
    "RoomStoreError",
    "Schedule",
    "ScheduleWriteValidationError",
    "SetpointJob",
    "SetpointValidationError",
    "SettingsValidationError",
    "ScheduleValidationError",
    "UnknownTargetError",
    "apply_dps_overlay",
    "async_fetch_gateway_keys",
    "build_heat_demand",
    "build_holidays_view",
    "build_room_view",
    "classify_z2m_descriptor",
    "clean_friendly_name",
    "count_transitions",
    "decode_device",
    "decode_dp199",
    "decode_operation",
    "decode_z2m_descriptor",
    "decode_z2m_state",
    "derive_distinct_device_id",
    "MQTT_CANDIDATE_DPS",
    "MQTT_CLOUD_CONFIRMED_DPS",
    "MQTT_ENUM_VALUES",
    "MQTT_TUNNEL_CONFIRMED_DPS",
    "MQTT_TENTHS_RANGE",
    "MQTT_WRITE_DPS",
    "encode_setpoint",
    "encode_z2m_descriptor",
    "encode_z2m_set_command",
    "encode_z2m_state",
    "mqtt_dp_open",
    "mqtt_write_value",
    "parse_messages",
    "programming_mode_from_z2m",
    "programming_mode_to_z2m",
    "property_for_source",
    "run_setpoint_job",
    "validate_setpoint",
    "HEAD_APPLIED",
    "HEAD_MISMATCH",
    "HEAD_UNREACHABLE",
    "JOB_APPLIED",
    "JOB_FAILED",
    "JOB_PARTIAL",
    "JOB_UNCONFIRMED",
    "head_state",
    "holiday_running",
    "holiday_temperature",
    "schedule",
    "validate_days",
    "write_allowed",
    "z2m_contract",
]
