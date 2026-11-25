# pydanfoss-cloud

Pure-Python library for **Danfoss Ally** (unofficial cloud path 1a, local Zigbee tunnel, Tuya cloud MQTT 2.3 frames, Zigbee2MQTT dialect contract, and standalone `danfoss2mqtt` bridge daemon).

Zero Home Assistant dependency. Powers:
- [`aghulas/danfoss2mqtt`](https://github.com/aghulas/danfoss2mqtt) — Home Assistant Add-on & standalone Docker bridge
- [`aghulas/ha-danfoss-ally`](https://github.com/aghulas/ha-danfoss-ally) — Home Assistant integration & MQTT Companion v2
- [`aghulas/danfoss-ally-mcp`](https://github.com/aghulas/danfoss-ally-mcp) — MCP server for Danfoss Ally

## Installation

```bash
pip install pydanfoss-cloud               # core (cloud HTTP client, decode, writes, z2m_contract)
pip install "pydanfoss-cloud[mqtt]"       # + paho-mqtt (danfoss2mqtt daemon & Tuya cloud MQTT)
pip install "pydanfoss-cloud[local]"      # + tinytuya (local Zigbee tunnel via Ally gateway)
```

## CLI (`danfoss2mqtt`)

```bash
danfoss2mqtt --mqtt-host localhost --base-topic danfoss2mqtt --dry-run
```

## Room coordinator (`danfoss-room-coordinator`)

A standalone, read-only aggregator for heads on a Zigbee2MQTT coordinator with no Danfoss gateway
and no cloud. It keeps its own room store, publishes `bridge/rooms` and `bridge/heat_demand` under
its **own** prefix, and answers `bridge/request/room/*` (`docs/z2m-cloud-parity.md` §6).

```bash
danfoss-room-coordinator --source-prefix zigbee2mqtt --base-topic danfoss-rooms --store-path rooms.json
```

### Write stages 2–7 (closed by default)

Every write stage is **closed by default**:

- **Room allowlist (`armed_rooms`, empty by default)**: no head receives any `/set` unless its
  `room_id` is listed (`--armed-room <room_id>`, `DANFOSS_ARMED_ROOMS`, or `options.json`).
  Requests targeting an unarmed room (`room/set-setpoint`, `room/set-schedule`, `room/set-mode`,
  `room/set-comfort-setback`, `room/restore-schedule`, `room/set-settings`) are refused with
  `status: "closed"`.
- **Per-stage opt-in flags (all `False` by default)**:
  - `--feed-external-sensor` (`DANFOSS_FEED_EXTERNAL_SENSOR`): Stage 3 event-driven room sensor
    feed (both `radiator_covered` modes, 30-min silence -> `-8000`).
  - `--mode-change-keeps-schedule` (`DANFOSS_MODE_CHANGE_KEEPS_SCHEDULE`): Stage 4 flag once a
    real-hardware trial confirms `programming_operation_mode=setpoint` keeps the ZCL schedule
    (when `False`, returning to `schedule` rewrites all 7 days).
  - `--reconcile-drift` (`DANFOSS_RECONCILE_DRIFT`): Stage 5 hourly drift reconciliation (capped at
    3 consecutive attempts, then flagged `reconcile_blocked`).
  - `--feed-load-mean` (`DANFOSS_FEED_LOAD_MEAN`): Stage 6 multi-head `load_room_mean` (coalesced
    with the sensor feed into one `/set` per head, `-8000` on clean stop).
  - `--enable-holidays` (`DANFOSS_ENABLE_HOLIDAYS`): Stage 7 holiday edge automation (reconciled
    at startup and on ticks; publishes `bridge/holidays`).
  - `--propagate-window-open` (`DANFOSS_PROPAGATE_WINDOW_OPEN`): Stage 7 cross-room
    `window_open_external` propagation with max-duration cap.

Arm a room only for a supervised real-hardware trial, by its `room_id`:

```bash
danfoss-room-coordinator --armed-room room-abcd1234   # repeatable
# or options.json: {"armed_rooms": ["room-abcd1234"]}
# or env:          DANFOSS_ARMED_ROOMS=room-abcd1234
```

A user change writes `occupied_heating_setpoint`; a coordinator-initiated change writes
`occupied_heating_setpoint_scheduled`. Each head is reported `applied` / `mismatch` / `unreachable`.

## Security

No credentials, installation secrets (`sign_key`, `ch_key`), or `device_id` are ever committed to this repository. When omitted, `danfoss2mqtt` generates and persists its own dedicated 44-hex `device_id` locally (`0600`).

## License

MIT — see [`LICENSE`](LICENSE).
