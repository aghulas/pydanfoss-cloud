# pydanfoss-cloud

Access library for the **unofficial Danfoss Ally cloud API** (path 1a, the mobile app's one — with
no developer app and no shared quota), extracted from the Home Assistant integration
[`danfoss_ally_cloud`](https://github.com/aghulas/ha-danfoss-ally). **No Home Assistant dependency.**

It provides:

- `CloudClient` — email/password login (+ installation secrets), list of heads, guarded writes
  (`tuya.m.custom.wkf.settings.change`, with the head's gateway as `gw_id`), room writes, schedules,
  holidays, rooms, robust re-authentication, optional distinct `deviceId`
  (`derive_distinct_device_id`) so a client does not share its Tuya session with the app.
- `decode` / `DanfossDevice` — decoding of the Tuya `dps` into a normalised model (product schema
  `03wvrwpv`), decoding of the E1–E15 fault codes.
- `writes` — value validation before sending and a per-model guard-rail.
- `keys` — fetching the gateways' `localKey` + LAN IP from the cloud.
- `local/` — local Zigbee tunnel (DP 198→199, tinytuya): attribute read/write (preheat 0x0025),
  weekly schedule.
- `mqtt_publish` — Tuya cloud MQTT codec, **no network**: protocol-2.3 frames (AES-GCM with the
  gateway localKey), topics, DP-publish payload, broker credential derivation.
- `mqtt_client` — `CloudMqttLink`, the live link built on that codec (paho-mqtt, extra `mqtt`):
  inbound report decoding, DP publish, confirmation by the gateway's echo. One connection per Tuya
  session; the write path carries the DPs proven on real hardware only (`writes.MQTT_WRITE_DPS`).

Wire format and derivations: `docs/cloud-mqtt-publish.md` in the repository.

## Packaging

**Single** source: this folder (`custom_components/danfoss_ally_cloud/api/`). It is packaged under
the name `pydanfoss_cloud` by the repo's `pyproject.toml`, **without moving files**:

- the Home Assistant integration "vendors" this code (relative import `from .api import …`), which
  keeps the HACS install self-contained;
- other clients (the MCP connector `danfoss-ally-mcp`) depend on the PyPI release.

```
pip install pydanfoss-cloud            # core: client, decode, writes, keys, MQTT codec
pip install 'pydanfoss-cloud[local]'   # + tinytuya for the local Zigbee tunnel
pip install 'pydanfoss-cloud[mqtt]'    # + paho-mqtt for the live cloud-MQTT link
pip install -e '.[local,mqtt]'         # development install from the repository
```

## Dependencies

`pycryptodome`, `requests` (core); `tinytuya` (extra `local`, Zigbee tunnel); `paho-mqtt` 1.6.1 or
2.x (extra `mqtt`, imported only when a connection is opened). Python ≥ 3.11.

## Changes

- **0.4.2** — `z2m_contract`: decode real Zigbee2MQTT 2.x string enums (`thermostat_orientation`
  `horizontal`/`vertical`, `viewing_direction` `normal`/`flipped`, `keypad_lockout` `lock1`).
  `bridge_daemon`: auto-generate and persist a dedicated 44-hex `device_id` when omitted.
- **0.4.1** — `bridge_daemon`: route `occupied_heating_setpoint` on `/set` through the proven
  `CloudClient.async_set_room_setpoint` path (guarded by `WRITE_VERIFIED_PRODUCTS`), explicitly
  log unsupported non-cloud `/set` fields instead of silently dropping them, keep strong task
  references for in-flight writes, and re-poll only after an actual write.
- **0.4.0** — v2.x Bridge + Companion foundation (`docs/companion-contract.md`):
  `z2m_contract` (bidirectional Zigbee2MQTT dialect encoder/decoder for Danfoss Ally heads,
  `014G2480` room sensors, and boiler relays) and `bridge_daemon` (`StandaloneDanfoss2MqttDaemon`,
  `PahoMqttTransport`, `BridgeConfig`, `async_run_bridge`, and the `danfoss2mqtt` CLI console
  script with `/data/options.json` support for the Home Assistant Add-on).
- **0.3.1** — additive, no change to existing behaviour. `writes`: `MQTT_CANDIDATE_DPS` (DPs
  implemented on the cloud-MQTT write path but not proven on real hardware), `mqtt_dp_open()`, and
  an optional third argument `supervised` to `mqtt_write_value()` (the candidates opened for a
  supervised trial; without it only the proven DPs pass, as before). `CloudClient`:
  `device_list_complete` and `device_list_homes` say whether the last `async_list_devices` had an
  answer from every home (the list itself is unchanged and still tolerant of a degraded home).
  `writes.MQTT_TENTHS_RANGE` and the setpoint bounds as candidates (DP 18 upper, DP 27 lower,
  DP 120 pause setpoint, tenths of °C within the product schema's range); `const.DP_LOWER_TEMP`.
  The isolated-head setpoints as candidates too: DP 118 (comfort) with its companion DP 16,
  DP 119 (setback). After their supervised writes on real hardware (2026-11-19), DP 116, 18, 27
  and 120 moved from `MQTT_CANDIDATE_DPS` to `MQTT_WRITE_DPS`: they pass `mqtt_write_value()`
  without `supervised`. DP 118, 16 and 119 followed after their own supervised writes
  (2026-11-19/20). Preheat (DP 1) as a candidate, the only one left, and
  `writes.MQTT_TUNNEL_CONFIRMED_DPS`: the DPs whose write the gateway's echo and the cloud read
  cannot confirm (DP 1), to be confirmed by a local-tunnel read of the head.
- **0.3.0** — `gw_id` on `async_write_setting` (the head's gateway; it defaulted to the head
  itself); distinct `deviceId` (`derive_distinct_device_id`, `CloudClient(distinct_device_id=…)`);
  `mqtt_publish` and `mqtt_client` (extra `mqtt`); holiday, room, relay and unbind calls; active
  setpoint and heating state in `decode`. No rename of the 0.2.0 public names.
- **0.2.0** — public API renamed to English; first PyPI release.

## Disclaimer

Undocumented API, obtained by observing the app's protocol: it can change or be cut off. Secrets
(session token, `localKey`, credentials): never logged or committed.
