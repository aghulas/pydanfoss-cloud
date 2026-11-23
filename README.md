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

### Setpoint fan-out (closed by default)

A room setpoint change (`bridge/request/room/set-setpoint`) is a **fan-out of one `/set` per head**,
each confirmed on that head's own state topic (`docs/z2m-cloud-parity.md` §5.2). It is
**closed by default**: nothing is written unless the room is explicitly **armed**. An unarmed room
is refused and nothing is sent — the coordinator stays read-only.

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
