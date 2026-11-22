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

## Security

No credentials, installation secrets (`sign_key`, `ch_key`), or `device_id` are ever committed to this repository. When omitted, `danfoss2mqtt` generates and persists its own dedicated 44-hex `device_id` locally (`0600`).

## License

MIT — see [`LICENSE`](LICENSE).
