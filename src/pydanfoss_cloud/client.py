"""Client for the Danfoss Ally cloud API (the non-public one, used by the app) - path 1a.

Async wrapper around the vendored Tuya stack (``_tuya``). Auth via the app account (email/password
login) + installation material captured once (sign_key, deviceId, chKey). Exposes: login, homes,
devices, per-device DPs, room groups, weekly schedules, room management, relay binding, holidays,
and sub-device unbind.

Re-authentication is robust: Tuya can invalidate the session when the app reconnects; on a session
error we log in once more then replay the request.

SECRETS: sign_key, sid, ecode, chKey and the password are NEVER logged.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime

from . import schedule
from ._tuya.http import ENDPOINTS, TuyaApiError, TuyaHttpClient, login
from .const import RES_TYPE_DEVICE, RES_TYPE_GROUP
from .decode import DanfossDevice, decode_device, holiday_running
from .messages import MSG_LIST_API, MSG_LIST_VERSION, DanfossMessage, parse_messages

_LOGGER = logging.getLogger(__name__)

# Tuya error codes that signal a session to renew.
_SESSION_ERRORS = frozenset(
    {
        "SIGN_INVALID",
        "APP_SESSION_EXPIRED",
        "SESSION_EXPIRED",
        "USER_SESSION_INVALID",
        "RING_SIGN_ERROR",
        "ILLEGAL_CLIENT_ID",
    }
)


class CloudError(RuntimeError):
    """Error on the path-1a cloud channel (login refused, session, cloud refusal)."""


class CloudAuthError(CloudError):
    """Credentials or installation secrets refused (login impossible)."""


class CloudWriteError(CloudError):
    """Write refused: invalid value, model not allowed, or cloud refusal."""


class UnknownTargetError(CloudError):
    """Target (room or head) not found in the homes (contract code `unknown_target`)."""


class ScheduleValidationError(CloudWriteError):
    """Invalid schedule (R1): refused BEFORE any send. Carries the contract errors (§5.4)."""

    def __init__(self, errors: list[dict[str, str]]) -> None:
        self.errors = errors
        codes = ", ".join(e.get("code", "?") for e in errors)
        super().__init__(f"invalid schedule: {codes}")


def _is_session_error(err: TuyaApiError) -> bool:
    code = (str(err.code) if err.code is not None else "").upper()
    if code in _SESSION_ERRORS:
        return True
    msg = (err.msg or "").lower()
    return "session" in msg or "sign invalid" in msg or "token is expired" in msg


DISTINCT_DEVICE_ID_MARKER = "ha"


def derive_distinct_device_id(device_id: str, marker: str = DISTINCT_DEVICE_ID_MARKER) -> str:
    """Return a stable ``deviceId`` distinct from ``device_id`` (same length, hex tail).

    The Tuya cloud keys the path-1a session on the request ``deviceId`` param, NOT on the account
    nor the chKey (proven on real hardware 2026-11-18): two clients sharing one ``deviceId`` share a
    SINGLE session and log each other out. Giving the integration its OWN stable ``deviceId`` — same
    ``sign_key`` and ``chKey`` — lets it coexist with the Danfoss app and the MCP connector (see
    docs/architecture.md). The server accepts any well-formed value distinct from the others (the
    signature is recomputed with the unchanged ``sign_key``); we derive it deterministically from
    the captured id so it stays STABLE across restarts (never randomise per request) and unique per
    installation. Only the trailing characters are rewritten, so the result still reads as a sibling
    device id.
    """
    if not device_id:
        return device_id
    digest = hashlib.sha256(f"{device_id}:{marker}".encode()).hexdigest()
    n = min(8, len(device_id))
    derived = device_id[:-n] + digest[:n]
    if derived == device_id:  # astronomically unlikely; guarantee it differs
        derived = device_id[:-n] + digest[n : 2 * n]
    return derived


class CloudClient:
    """Cloud access via the mobile app's API (path 1a)."""

    def __init__(
        self,
        username: str,
        password: str,
        country_code: str,
        sign_key: str,
        device_id: str,
        ch_key: str,
        region: str = "eu",
        *,
        sid: str | None = None,
        ecode: str | None = None,
        distinct_device_id: bool = False,
    ) -> None:
        self._username = username
        self._password = password
        self._country_code = country_code
        self._sign_key = sign_key
        # Coexistence (contract §4 / docs/architecture.md): when ``distinct_device_id`` is set, the
        # client uses a stable deviceId derived from the captured one (same sign_key/chKey) so its
        # Tuya session no longer collides with the app's or the MCP connector's. Off = the captured
        # id verbatim (legacy behaviour, kept for debugging/rollback).
        self._device_id = derive_distinct_device_id(device_id) if distinct_device_id else device_id
        self._ch_key = ch_key
        self._endpoint = ENDPOINTS.get(region.lower(), ENDPOINTS["eu"])
        self._sid = sid
        # Completeness of the last ``async_list_devices`` (see the properties below).
        self._device_list_complete = False
        self._device_list_homes: dict[str, int] = {}
        self._device_home: dict[str, str] = {}
        self._ecode = ecode
        self._uid: str | None = None
        self._partner_identity: str | None = None
        self._client: TuyaHttpClient | None = None
        self._lock = asyncio.Lock()

    @property
    def session(self) -> dict[str, str | None]:
        """Current session material (sid/ecode) - to persist in the HA entry."""
        return {"sid": self._sid, "ecode": self._ecode}

    @property
    def mqtt_identity(self) -> dict[str, str | None]:
        """uid + partnerIdentity from the last login, for the Tuya cloud MQTT username/user-topic
        (docs/cloud-mqtt-publish.md §5). Populated by ``async_login``; None before first login."""
        return {"uid": self._uid, "partner_identity": self._partner_identity}

    def _build_client(self) -> TuyaHttpClient:
        return TuyaHttpClient(
            self._sign_key,
            self._sid or "",
            self._ecode or "",
            self._ch_key,
            self._device_id,
            endpoint=self._endpoint,
        )

    def _login_sync(self) -> dict:
        return login(
            self._username,
            self._password,
            self._country_code,
            self._sign_key,
            self._device_id,
            self._ch_key,
            endpoint=self._endpoint,
        )

    async def async_login(self) -> None:
        """Open a session (email/password login) and prepare the HTTP client."""
        try:
            res = await asyncio.to_thread(self._login_sync)
        except TuyaApiError as err:
            raise CloudAuthError(f"login refused: {err.code} {err.msg}") from err
        if not res.get("sid") or not res.get("ecode"):
            raise CloudAuthError("login without sid/ecode: invalid credentials or secrets.")
        self._sid = res["sid"]
        self._ecode = res["ecode"]
        self._uid = res.get("uid")
        self._partner_identity = res.get("partnerIdentity")
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
        self._client = self._build_client()

    async def _ensure_client(self) -> None:
        if self._client is None:
            if self._sid and self._ecode:
                self._client = self._build_client()
            else:
                await self.async_login()

    async def _request_once(self, api: str, version: str, body, extra_params):
        """A single attempt of an ``m.*`` request in the executor, under the lock.

        Lets ``TuyaApiError`` propagate raw: the orchestration (re-login + replay) happens
        OUTSIDE the lock, because ``asyncio.Lock`` is not reentrant (a replay while holding the
        lock would deadlock forever).
        """
        async with self._lock:
            await self._ensure_client()
            client = self._client
            assert client is not None
            return await asyncio.to_thread(
                client.request, api, version, body, extra_params=extra_params
            )

    async def _request(self, api: str, version: str, body=None, extra_params=None):
        """``m.*`` request with a single re-login on a session error, then replay."""
        try:
            return await self._request_once(api, version, body, extra_params)
        except TuyaApiError as err:
            if not _is_session_error(err):
                raise CloudError(f"Tuya cloud: {err.code} {err.msg}") from err
            _LOGGER.debug("invalid cloud session (%s): re-login", err.code)
            await self.async_login()
            try:
                return await self._request_once(api, version, body, extra_params)
            except TuyaApiError as err2:
                raise CloudError(f"Tuya cloud: {err2.code} {err2.msg}") from err2

    async def async_get_homes(self) -> list[dict]:
        """List of the account's homes (tuya.m.location.list). Returns [{gid, name}]."""
        r = await self._request("tuya.m.location.list", "2.0", None)
        out: list[dict] = []
        for h in r if isinstance(r, list) else []:
            gid = h.get("groupId") or h.get("gid") or h.get("homeId")
            if gid is not None:
                out.append({"gid": gid, "name": h.get("name")})
        return out

    async def async_list_messages(self) -> list[DanfossMessage]:
        """Messages of the app's message center, newest first (read only).

        ``smartlife.m.msg.list`` v2.0 (docs/message-center.md): the whole list, no body (the
        version ignores msgType/offset/limit). Never marks a message read, never deletes one.
        Session errors re-login and replay like every other ``m.*`` read.
        """
        r = await self._request(MSG_LIST_API, MSG_LIST_VERSION, None)
        return parse_messages(r)

    async def async_list_devices(self) -> list[DanfossDevice]:
        """All heads (and other sub-devices) of the account's homes, decoded.

        Tolerant on purpose: a home whose answer is not a list contributes no device and does not
        raise, so one degraded answer does not take the whole fleet down. The price is that the
        result can be INCOMPLETE without an error; ``device_list_complete`` and
        ``device_list_homes`` say so, for callers that must not act on a partial list (removing
        what is "gone", for instance).
        """
        devices: list[DanfossDevice] = []
        seen: set[str] = set()
        homes = await self.async_get_homes()
        complete = bool(homes)
        per_home: dict[str, int] = {}
        device_home: dict[str, str] = {}
        for home in homes:
            raw = await self._request(
                "tuya.m.my.group.device.list", "1.0", None, extra_params={"gid": home["gid"]}
            )
            if not isinstance(raw, list):
                complete = False
                continue
            count = 0
            for device in raw:
                dev = decode_device(device)
                if dev.id:
                    count += 1
                    device_home.setdefault(dev.id, str(home["gid"]))
                    if dev.id not in seen:
                        seen.add(dev.id)
                        devices.append(dev)
            per_home[str(home["gid"])] = count
        self._device_list_complete = complete
        self._device_list_homes = per_home
        self._device_home = device_home
        return devices

    @property
    def device_list_complete(self) -> bool:
        """True if, in the last ``async_list_devices``, at least one home was returned and EVERY
        home answered its device list with a list. False before the first call, and whenever a
        home's answer was missing or malformed (the returned devices are then a subset). It cannot
        tell a home that is really empty from one answered as an empty list: compare
        ``device_list_homes`` with an earlier read for that."""
        return self._device_list_complete

    @property
    def device_list_homes(self) -> dict[str, int]:
        """Per home gid, the number of devices its list held in the last ``async_list_devices``
        (only the homes that answered with a list)."""
        return dict(self._device_list_homes)

    @property
    def device_home(self) -> dict[str, str]:
        """devId -> gid of the home whose device list held it, in the last ``async_list_devices``.

        The per-home device list is the account's own attribution of a device to a home (exact, not
        inferred). A device listed by several homes keeps the first one."""
        return dict(self._device_home)

    async def async_list_raw(self) -> list[dict]:
        """Raw devices of all homes (devId, name, category, localKey, meshId, nodeId).

        Useful for the local fallback: the gateways' localKey is in the `localKey` field (path 1a).
        """
        out: list[dict] = []
        seen: set[str] = set()
        for home in await self.async_get_homes():
            raw = await self._request(
                "tuya.m.my.group.device.list", "1.0", None, extra_params={"gid": home["gid"]}
            )
            for d in raw if isinstance(raw, list) else []:
                dev_id = d.get("devId") if isinstance(d, dict) else None
                if dev_id and dev_id not in seen:
                    seen.add(dev_id)
                    out.append(d)
        return out

    async def async_device_homes(self, dev_ids: list[str]) -> dict[str, list[str]]:
        """Map each devId to the home gid(s) whose device list contains it (``[]`` if none).

        Path 1a: one device-list read per home. A device normally appears in exactly one home; it
        appears in several only if the backend does not filter the list by ``gid`` — the caller then
        treats the home as un-resolvable rather than guessing the first one (which would create the
        room in the wrong home and leave it empty). Used to pick the home a new room is created in
        (the account may have several homes; a config entry does not disambiguate them).
        """
        wanted = set(dev_ids)
        homes_of: dict[str, list[str]] = {d: [] for d in dev_ids}
        for home in await self.async_get_homes():
            gid = str(home["gid"])
            raw = await self._request(
                "tuya.m.my.group.device.list", "1.0", None, extra_params={"gid": home["gid"]}
            )
            for d in raw if isinstance(raw, list) else []:
                dev_id = d.get("devId") if isinstance(d, dict) else None
                if dev_id in wanted and gid not in homes_of[dev_id]:
                    homes_of[dev_id].append(gid)
        return homes_of

    async def async_get_device_dps(self, dev_id: str) -> dict | None:
        """Fresh DPs of ONE device via the per-device route (``smartlife.m.device.get`` v4.0).

        R-3 (freshness): the bulk ``tuya.m.my.group.device.list`` route lags for minutes after a
        command (stale valve / ``pi_heating_demand`` / ``work_state`` / active setpoint), while the
        per-device route reflects it quickly. This returns that device's ``dps`` map (string keys),
        or ``None`` when the route carries no ``dps`` — the caller then keeps the bulk value, so an
        unavailable/sparse response degrades safely to today's behaviour.

        ⚠ This per-device DP read is the single seam of the post-command refresh (coordinator
        ``_overlay_commanded``); that the ``device.get`` route is fresher than the bulk list for a
        HEAD is the R-3 hypothesis to confirm on real hardware (pending a validation session). If it
        proves not fresher, only this method's backing route changes (e.g. the official Danfoss REST
        per-device route), not the mechanism.

        Shape (observed on real hardware, 2026-11-20): version 4.0 carries the DPs under
        ``dataPointInfo.dps`` (with ``dataPointInfo.dpsTime``), NOT at the top level. Reading only a
        top-level ``dps`` returned None for every head, which silently disabled the post-command
        overlay and every confirmation that reads this route (DP 101). A top-level ``dps`` is still
        accepted when ``dataPointInfo`` carries none.
        """
        r = await self._request("smartlife.m.device.get", "4.0", {"devId": dev_id})
        if not isinstance(r, dict):
            return None
        info = r.get("dataPointInfo")
        dps = info.get("dps") if isinstance(info, dict) else None
        if not isinstance(dps, dict) or not dps:
            dps = r.get("dps")
        if not isinstance(dps, dict) or not dps:
            return None
        return {str(k): v for k, v in dps.items()}

    async def async_device_meta(self, dev_id: str) -> dict:
        """Gateway meta (smartlife.m.device.get v4.0 -> meta): lan_ip, Zigbee channel."""
        r = await self._request("smartlife.m.device.get", "4.0", {"devId": dev_id})
        meta = (r.get("meta") or {}) if isinstance(r, dict) else {}
        return {
            "name": r.get("name") if isinstance(r, dict) else None,
            "lan_ip": meta.get("lan_ip"),
            "channel": meta.get("zigChannel"),
        }

    # --- Axis B reads (holiday, boiler relay, room map): read-only, contract §4.5-§4.7 -----------

    async def async_get_vacation(self, home_id: str | int) -> dict:
        """Holiday on record for a home (tuya.m.solution.wkf.vacation.detail). {} if none.

        The record covers a holiday in progress **and** a planned one (``isRunning`` tells them
        apart, see ``holiday_running``).

        Read (contract §4.5). The holiday *write* routes were captured on 2026-11-18 (see the
        ``async_vacation_*`` methods below); they run through a runtime mini-program on the app but
        the plaintext bodies replay on the bare path-1a channel. R5 still holds: DP 2 ``holiday`` is
        only a display label — these ``solution.wkf.vacation.*`` routes are the real lever.
        """
        r = await self._request(
            "tuya.m.solution.wkf.vacation.detail", "1.0", {"homeId": str(home_id)}
        )
        return r if isinstance(r, dict) else {}

    # --- Axis A vacation writes (contract §4.5, §5.2). Captured 2026-11-18; GATED and pending a
    # real-hardware validation (a vacation write acts on the live heating). The caller enforces the
    # opt-in gate and admin rights; this layer only encodes the captured request bodies. ----------

    @staticmethod
    def _room_ids_csv(room_ids: list[str] | str) -> str:
        """The captured ``roomIds`` field is a comma-separated string of Tuya room ids."""
        if isinstance(room_ids, str):
            return room_ids
        return ",".join(str(r) for r in room_ids)

    async def async_vacation_leave(
        self,
        home_id: str | int,
        start_date: str,
        start_time: str,
        end_date: str,
        end_time: str,
        temperature_tenths: int,
        room_ids: list[str] | str,
        time_zone: str = "Europe/Paris",
    ) -> object:
        """Set / edit an *away* holiday (tuya.m.solution.wkf.vacation.plan.leave v1.0).

        The head holds ``temperature`` for the whole period. Editing = re-posting ``plan.leave``
        (idempotent per home). ``temperature`` is tenths of °C as a **string** (``150`` = 15.0 °C),
        ``startDate``/``endDate`` are ``YYYYMMDD``, ``startTime``/``endTime`` are ``HH:MM``.
        """
        return await self._request(
            "tuya.m.solution.wkf.vacation.plan.leave",
            "1.0",
            {
                "startDate": start_date,
                "startTime": start_time,
                "endDate": end_date,
                "endTime": end_time,
                "temperature": str(int(temperature_tenths)),
                "homeId": str(home_id),
                "roomIds": self._room_ids_csv(room_ids),
                "timeZoneId": time_zone,
            },
        )

    async def async_vacation_athome(
        self,
        home_id: str | int,
        start_date: str,
        end_date: str,
        room_ids: list[str] | str,
        time_zone: str = "Europe/Paris",
    ) -> object:
        """Set / edit an *at-home* holiday (tuya.m.solution.wkf.vacation.plan.athome v1.0).

        No temperature and no times: each head follows its **Saturday** schedule for the period
        (DP 2 ``holiday_sat``). The app blocks this client-side when no room has a Saturday program;
        the cloud may likewise refuse it.
        """
        return await self._request(
            "tuya.m.solution.wkf.vacation.plan.athome",
            "1.0",
            {
                "startDate": start_date,
                "endDate": end_date,
                "homeId": str(home_id),
                "roomIds": self._room_ids_csv(room_ids),
                "timeZoneId": time_zone,
            },
        )

    async def async_vacation_cancel(
        self, home_id: str | int, time_zone: str = "Europe/Paris"
    ) -> object:
        """Cancel a *planned* holiday before it starts (vacation.cancel v1.0)."""
        return await self._request(
            "tuya.m.solution.wkf.vacation.cancel",
            "1.0",
            {"homeId": str(home_id), "timeZoneId": time_zone},
        )

    async def async_vacation_leave_now(
        self,
        home_id: str | int,
        room_ids: list[str] | str,
        temperature_tenths: int,
        time_zone: str = "Europe/Paris",
    ) -> object:
        """End an *in-progress* holiday now (tuya.m.solution.wkf.vacation.leave.now v1.0).

        Re-applies ``temperature`` to the rooms immediately. Here ``temperature`` is tenths of °C as
        an **integer** (``150`` = 15.0 °C), unlike ``plan.leave`` where it is a string.
        """
        return await self._request(
            "tuya.m.solution.wkf.vacation.leave.now",
            "1.0",
            {
                "homeId": str(home_id),
                "roomIds": self._room_ids_csv(room_ids),
                "temperature": int(temperature_tenths),
                "timeZoneId": time_zone,
            },
        )

    async def async_list_holidays(self) -> list[dict]:
        """Per-home holiday state: [{gid, name, active, scheduled, detail}]. Read-only.

        ``vacation.detail`` holds the holiday on record, *in progress or planned* (validated on real
        hardware 2026-11-19: a planned holiday comes back with ``isRunning: false``). ``active`` is
        the ``isRunning`` flag (``None`` if the record lacks it), ``scheduled`` its complement for a
        non-empty record; the record itself is passed through unmodified as ``detail``. A per-home
        read failure degrades to no holiday (never raises for the set).
        """
        out: list[dict] = []
        for home in await self.async_get_homes():
            try:
                detail = await self.async_get_vacation(home["gid"])
            except CloudError:
                detail = {}
            running = holiday_running(detail)
            out.append(
                {
                    "gid": str(home["gid"]),
                    "name": home.get("name"),
                    "active": running,
                    "scheduled": (bool(detail) and not running) if running is not None else None,
                    "detail": detail or None,
                }
            )
        return out

    async def async_list_relays(self, gid: str | int) -> list[dict]:
        """Boiler relays (Tuya category ``kg``) of a home: [{dev_id, name, on, online, mesh_id}].

        Read-only (contract §4.6): DP ``1`` = on/off. There is **no** manual relay on/off in the app
        (confirmed 2026-11-18): the relay is server-driven from the heat demand of the linked rooms,
        so no DP ``1`` write exists to mirror. Only the state read and the room<->relay link
        (``async_relay_rooms`` + ``async_relay_bind``) are in contract.
        """
        raw = await self._request(
            "tuya.m.my.group.device.list", "1.0", None, extra_params={"gid": gid}
        )
        out: list[dict] = []
        for d in raw if isinstance(raw, list) else []:
            if not isinstance(d, dict) or d.get("category") != "kg":
                continue
            dps = d.get("dps") or {}
            zb = (d.get("moduleMap") or {}).get("zigbee") or {}
            out.append(
                {
                    "dev_id": d.get("devId"),
                    "name": (d.get("name") or "").strip(),
                    "on": bool(dps.get("1")),
                    "online": bool(zb.get("isOnline", d.get("isOnline", False))),
                    "mesh_id": d.get("meshId"),
                }
            )
        return out

    async def async_relay_rooms(self, dev_id: str) -> list[dict]:
        """Rooms eligible to drive a relay and whether they do (m.life.solution.wkf.reference.room).

        ``gwId == devId`` of the relay. A room is **linked** when ``enable`` is set and ``cancel``
        is not (unchecking a room in the app sets ``cancel``). Read-only. Returns
        [{room_id, name, linked}].
        """
        r = await self._request(
            "m.life.solution.wkf.reference.room", "1.0", {"devId": dev_id, "gwId": dev_id}
        )
        out: list[dict] = []
        for x in r if isinstance(r, list) else []:
            if not isinstance(x, dict):
                continue
            out.append(
                {
                    "room_id": x.get("roomId"),
                    "name": x.get("roomName"),
                    "linked": bool(x.get("enable")) and not bool(x.get("cancel")),
                }
            )
        return out

    async def async_list_rooms(self, gid: str | int) -> list[dict]:
        """Room map of a home (m.life.group.room.map.get): [{room_id, name}]. Read-only.

        A Tuya *room* (type 4) carries the human name and is the ``parentId`` of attach/detach; it
        is distinct from the *schedule-group* (type 5, ``async_list_groups``). Mapping group<->room
        is by name.
        """
        r = await self._request("m.life.group.room.map.get", "1.0", {"gid": gid})
        rooms = r.get("rooms") if isinstance(r, dict) else None
        out: list[dict] = []
        for x in rooms if isinstance(rooms, list) else []:
            if isinstance(x, dict) and x.get("id") is not None:
                out.append({"room_id": str(x.get("id")), "name": x.get("name")})
        return out

    async def async_room_id(self, home_gid: str | int, name: str) -> str | None:
        """Tuya room id (type 4) of a room by its name, in a home (``room.map.get``). Read-only.

        A schedule-group (type 5) and a Tuya room (type 4) share the same display name; the room
        management writes (create/attach/detach/rename/delete) address the room id, resolved here.
        Case/space-insensitive match. ``None`` if no room has that name.
        """
        target = (name or "").strip().lower()
        for room in await self.async_list_rooms(home_gid):
            if (room.get("name") or "").strip().lower() == target:
                return room["room_id"]
        return None

    async def async_relay_bind(self, dev_id: str, room_id: str | int, linked: bool) -> object:
        """Link / unlink a room to a boiler relay (m.life.solution.wkf.reference.edit, §4.6).

        ``gwId == devId`` of the relay. ``linked`` True = ``bind`` (the room drives the relay),
        False = ``unbind``. One room per call. Captured from the app; to validate on real hardware
        (it changes the boiler's heat demand).
        """
        return await self._request(
            "m.life.solution.wkf.reference.edit",
            "1.0",
            {
                "devId": dev_id,
                "gwId": dev_id,
                "roomId": room_id,
                "status": "bind" if linked else "unbind",
            },
        )

    # --- Room management writes (contract §4.7). Validated on real hardware by the MCP connector.
    # These mutate the live account structure (regrouping heads changes shared schedules): a caller
    # must gate them on real-hardware validation and admin rights.

    async def async_room_add(self, home_gid: str | int, name: str) -> str:
        """Create a Tuya room (smartlife.m.room.add v2.0 {gid, name}). Returns the new room id."""
        r = await self._request("smartlife.m.room.add", "2.0", {"gid": home_gid, "name": name})
        room_id = r.get("id") if isinstance(r, dict) else r
        if room_id in (None, ""):
            raise CloudWriteError("room creation returned no id")
        return str(room_id)

    async def async_room_update(self, room_id: str | int, name: str) -> None:
        """Rename a room (smartlife.m.room.update v1.0 {roomId, name})."""
        await self._request("smartlife.m.room.update", "1.0", {"roomId": room_id, "name": name})

    async def async_room_dismiss(self, room_id: str | int) -> None:
        """Delete a room (smartlife.m.custom.wfk.room.dismiss v1.0). Its heads become ungrouped."""
        await self._request("smartlife.m.custom.wfk.room.dismiss", "1.0", {"roomId": room_id})

    async def async_room_set_devices(self, room_id: str | int, dev_ids: list[str]) -> None:
        """Set a room's member heads (smartlife.m.custom.wkf.device.relations.selected.save v1.0).

        ⚠ REPLACES the whole member list: the caller sends ``current ∪ added`` (attach) or
        ``current − removed`` (detach), never a delta.
        """
        await self._request(
            "smartlife.m.custom.wkf.device.relations.selected.save",
            "1.0",
            {
                "bizs": [{"id": d, "type": 6} for d in dev_ids],
                "onlyParent": True,
                "parentId": room_id,
                "parentType": 4,
            },
        )

    async def async_device_reset(self, dev_id: str, reset_factory: bool = False) -> object:
        """UNBIND a head from the network (``smartlife.m.device.reset`` v3.0). ⚠ DESTRUCTIVE.

        This is the app's "disconnect" action: ``ThingHomeSdk.newDeviceInstance(devId)
        .removeDevice()`` (+ ``resetFactory``), i.e. a Zigbee ``leave`` emitted by the gateway. The
        head leaves the network and disappears from the account (its individual routes then 404).
        Captured and validated on real hardware 2026-11-18 (see
        ``~/dev/danfoss-ally-signing-re/NOTES.md``, section "GATE A/D"); the app sends it as
        ``thing.m.device.reset`` but the SIGNED/wire name is ``smartlife.m.device.reset`` (the SDK
        rewrites the ``thing.`` prefix to ``smartlife.`` before signing), which is the final name
        passed here — exactly as for ``smartlife.m.device.get`` / ``smartlife.m.room.*``.

        The two app buttons differ ONLY by ``reset_factory``:

        - ``reset_factory=False`` ("disconnect"): identity KEPT. Re-added on ITS gateway without a
          wipe, the head comes back by rediscovery with the SAME ``devId`` (name/identity restored).
        - ``reset_factory=True`` ("disconnect and wipe data"): factory reset. On re-pairing the head
          returns with a NEW ``devId``; identity and name are LOST — IRREVERSIBLE on the identity.

        ⚠ Re-pairing (add) is NOT covered by path 1a: opening the gateway's permit-join is an MQTT
        order, not an HTTP ``m.*`` call, and it needs a physical action on the head, on site.
        ⚠ Session conflict: path 1a shares the Tuya ``deviceId`` with the app; a real unbind assumes
        the Danfoss app is signed out (else ``USER_SESSION_INVALID``) — see ``distinct_device_id``.

        The caller is responsible for the safety gates (opt-in, admin, verified product, explicit
        confirmation): this layer only encodes the captured request body.
        """
        if not dev_id:
            raise CloudWriteError("devId required for the unbind.")
        return await self._request(
            "smartlife.m.device.reset",
            "3.0",
            {"devId": dev_id, "resetFactory": bool(reset_factory)},
        )

    async def async_write_setting(
        self, dev_id: str, pid: str, dps: dict, *, gw_id: str | int | None = None
    ) -> object:
        """Write setting DPs on a HEAD (tuya.m.custom.wkf.settings.change, resType 1).

        ``dps`` = {str(dp): value}. ``gw_id`` is the head's gateway (``meshId``); when omitted (or
        falsy) it falls back to ``dev_id``.

        ⚠ NOT a working path for the head settings DP 30 (child lock) / DP 116 (window detection).
        Real-hardware validation W-3 (2026-11-19): on an ISOLATED head the request is accepted but
        never applied (the test head stayed locked > 10 min), both with ``gwId = meshId`` (this
        integration) and ``gwId = devId`` (the MCP connector) - addressing is not the cause;
        consistent with the connector's 2026-11-15 survey (DP 116 applied on 0 of 7 isolated heads).
        An app capture the same day showed why: the app only uses this route for its "Settings"
        block ``{"1", "118", "119", "121"}`` (with ``gwId = devId``) and toggles DP 30 through a DP
        publish over Tuya cloud MQTT (``smart/mb/out/<gateway>``), not over HTTP. Contract §4.2.

        So the setting switches no longer call this: a grouped head is set through its room
        (``async_group_publish``), an isolated head through the local tunnel (window detection) or
        not at all (unavailable). Kept for the back-to-schedule label (``async_restore_schedule``).
        """
        if not dev_id or not pid:
            raise CloudWriteError("devId/pid required for the write.")
        body = {
            "devId": dev_id,
            "gwId": str(gw_id or dev_id),
            "pid": pid,
            "resId": dev_id,
            "resType": 1,
            "settings": json.dumps(dps, separators=(",", ":")),
        }
        return await self._request("tuya.m.custom.wkf.settings.change", "1.0", body)

    # --- DURABLE setpoint write at ROOM level (group), the "real" path 1a ------------------------
    # Replaces the wrong approach of PR #8 (mode DP on the head). See api/const.py
    # (MODE_GROUP_SETPOINT_DP): publish the setpoint on the GROUP, then rewrite one schedule day so
    # the heads pick up the value in their transitions.

    async def async_list_groups(self, gid: str | int) -> list[dict]:
        """Rooms (Tuya groups) of a home (m.life.my.group.device.group.list v4.3).

        Returns [{group_id, name, head_count, pid}]. A multi-head room is driven by its GROUP.
        """
        r = await self._request("m.life.my.group.device.group.list", "4.3", {"gid": gid})
        out: list[dict] = []
        for g in r if isinstance(r, list) else []:
            gid5 = g.get("id") or g.get("groupId")
            if gid5 is not None:
                out.append(
                    {
                        "group_id": gid5,
                        "name": g.get("name"),
                        "head_count": g.get("deviceNum"),
                        "pid": g.get("productId"),
                    }
                )
        return out

    async def async_group_members(self, gid: str | int) -> dict[str, list[str]]:
        """Member heads of each room (m.life.my.group.device.relation.list v3.2).

        Returns {group_id (str): [devId, ...]}: key "5" = schedule groups (type 5), their children
        "6" = member heads.
        """
        rel = await self._request("m.life.my.group.device.relation.list", "3.2", {"gid": gid})
        out: dict[str, list[str]] = {}
        for entry in rel.get("5", []) if isinstance(rel, dict) else []:
            group_id = str(entry.get("id"))
            children = (entry.get("children") or {}).get("6") or []
            out[group_id] = [c.get("id") for c in children if c.get("id")]
        return out

    async def async_resolve_group(self, dev_id: str) -> tuple[str, str | None] | None:
        """Find a head's room (group): (group_id, pid), or None if not grouped.

        An UNGROUPED head has no proven path-1a setpoint method (the connector then goes through the
        official Danfoss API): the caller must refuse rather than lie.
        """
        for home in await self.async_get_homes():
            gid = home["gid"]
            members = await self.async_group_members(gid)
            group_id = next((g for g, devs in members.items() if dev_id in devs), None)
            if group_id is None:
                continue
            pid = next(
                (
                    g["pid"]
                    for g in await self.async_list_groups(gid)
                    if str(g["group_id"]) == str(group_id)
                ),
                None,
            )
            return (group_id, pid)
        return None

    async def async_group_publish(self, group_id: str | int, dps: dict) -> object:
        """Publish DPs on a ROOM (group): s.m.dev.group.dp.publish {dps, groupId}."""
        return await self._request(
            "s.m.dev.group.dp.publish",
            "1.0",
            {"dps": json.dumps(dps, separators=(",", ":")), "groupId": int(group_id)},
        )

    async def async_read_week_timer(
        self,
        res_id: str | int,
        pid: str,
        *,
        res_type: int = RES_TYPE_GROUP,
        gw_id: str | int | None = None,
    ) -> list[dict]:
        """Raw weekly schedule (tuya.m.custom.wkf.week.timer.list): [{loops, loopData}]."""
        body = {
            "devId": str(res_id),
            "gwId": str(gw_id or res_id),
            "pid": pid,
            "resId": str(res_id),
            "resType": res_type,
        }
        r = await self._request("tuya.m.custom.wkf.week.timer.list", "1.0", body)
        data = r.get("data") if isinstance(r, dict) else r
        return data if isinstance(data, list) else []

    async def async_write_week_day(
        self,
        res_id: str | int,
        pid: str,
        loops: str,
        periods: list[dict],
        *,
        res_type: int = RES_TYPE_GROUP,
        gw_id: str | int | None = None,
    ) -> None:
        """Rewrite ONE schedule day: delete then create.

        ``periods`` = comfort windows "HH:MM". ``loops`` = single-day mask (bit 0 = Sunday).
        Empty ``periods`` = day off (no create). Two v1.0 calls, body
        {devId, gwId, loops, pid, resId, resType}.
        """
        base = {
            "devId": str(res_id),
            "gwId": str(gw_id or res_id),
            "pid": pid,
            "resId": str(res_id),
            "resType": res_type,
        }
        await self._request("tuya.m.custom.wkf.week.timer.delete", "1.0", {**base, "loops": loops})
        if periods:
            await self._request(
                "tuya.m.custom.wkf.week.timer.create",
                "1.0",
                {**base, "loops": loops, "periods": json.dumps(periods)},
            )

    async def async_set_group_setpoint(
        self, group_id: str | int, pid: str | None, dps: dict, *, repush: bool = True
    ) -> None:
        """DURABLE setpoint on a ROOM whose group is already known (the "real" path 1a).

        1. publish ``dps`` (e.g. {"118": 60} comfort, {"109": 60, "119": 60} setback) on the group;
        2. if ``repush`` and ``pid`` known, rewrite ONE schedule day verbatim so the heads pick up
           the new setpoint in their transitions (otherwise they keep the old one).

        ⚠ Room-wide effect: all heads of the group are affected, and the re-send touches the real
        heating schedule. To validate on real hardware before any use (see CLAUDE.md, memories).
        """
        await self.async_group_publish(group_id, dps)
        if repush and pid:
            data = await self.async_read_week_timer(group_id, pid)
            day = schedule.first_comfort_day(data)
            if day is not None:
                await self.async_write_week_day(
                    group_id,
                    pid,
                    day["loops"],
                    schedule.comfort_periods(day.get("loopData")),
                )

    async def async_set_room_setpoint(self, dev_id: str, dps: dict, *, repush: bool = True) -> str:
        """DURABLE setpoint for a head's room, resolving its group first.

        Resolves the head's room (group) - refuses if the head is not grouped - then delegates to
        ``async_set_group_setpoint``. Returns the written group's id. The room climate, which
        already knows its group, calls ``async_set_group_setpoint`` directly.
        """
        resolved = await self.async_resolve_group(dev_id)
        if resolved is None:
            raise CloudWriteError(
                f"head {dev_id} not grouped: path-1a setpoint not proven outside a room."
            )
        group_id, pid = resolved
        await self.async_set_group_setpoint(group_id, pid, dps, repush=repush)
        return str(group_id)

    # --- Weekly schedule editing (contract §3.2), built on the primitives above ------------------

    async def _resolve_target(self, target: dict) -> dict:
        """Resolve {type, id} into {res_id, res_type, pid, gw_id, name}.

        Room: resType 2, gwId = group_id. Isolated head: resType 1, gwId = meshId (form replayed on
        real hardware; contract §4.1 says devId - known divergence).
        """
        kind = target.get("type")
        ident = str(target.get("id"))
        if kind == "room":
            for home in await self.async_get_homes():
                for g in await self.async_list_groups(home["gid"]):
                    if str(g["group_id"]) == ident:
                        return {
                            "res_id": ident,
                            "res_type": RES_TYPE_GROUP,
                            "pid": g.get("pid"),
                            "gw_id": ident,
                            "name": g.get("name"),
                        }
        elif kind == "device":
            for home in await self.async_get_homes():
                raw = await self._request(
                    "tuya.m.my.group.device.list", "1.0", None, extra_params={"gid": home["gid"]}
                )
                for d in raw or []:
                    if isinstance(d, dict) and d.get("devId") == ident:
                        dev = decode_device(d)
                        return {
                            "res_id": ident,
                            "res_type": RES_TYPE_DEVICE,
                            "pid": dev.product_id,
                            "gw_id": dev.mesh_id or ident,
                            "name": dev.name,
                        }
        raise UnknownTargetError(f"target not found: {target!r}")

    async def async_get_schedule(self, target: dict) -> dict:
        """Weekly schedule of a target, decoded (contract §3.2). Read-only."""
        res = await self._resolve_target(target)
        data = await self.async_read_week_timer(
            res["res_id"], res["pid"] or "", res_type=res["res_type"], gw_id=res["gw_id"]
        )
        return schedule.Schedule(
            target={"type": str(target.get("type")), "id": str(target.get("id"))},
            name=res.get("name"),
            days=schedule.decode_week_timer(data),
            read_at=datetime.now().astimezone().isoformat(timespec="seconds"),
        ).to_dict()

    async def async_set_schedule_days(self, target: dict, days: dict) -> dict:
        """Validate (R1) then write the given days (delete+create per day, via write_week_day)."""
        errors = schedule.validate_days(days)
        if errors:
            raise ScheduleValidationError(errors)
        res = await self._resolve_target(target)
        written: list[str] = []
        for day, ranges in days.items():
            if day not in schedule.DAYS:
                continue
            await self.async_write_week_day(
                res["res_id"],
                res["pid"] or "",
                schedule.day_to_loop(day),
                schedule.to_periods(ranges),
                res_type=res["res_type"],
                gw_id=res["gw_id"],
            )
            written.append(day)
        return {"target": target, "days_written": written}

    async def async_restore_schedule(self, target: dict) -> dict:
        """Back to schedule (R4): rewrite all SEVEN days, then publish `at_home` LAST."""
        res = await self._resolve_target(target)
        current = await self.async_get_schedule(target)
        days = current["days"]
        for day in schedule.DAYS:
            await self.async_write_week_day(
                res["res_id"],
                res["pid"] or "",
                schedule.day_to_loop(day),
                schedule.to_periods(days.get(day) or []),
                res_type=res["res_type"],
                gw_id=res["gw_id"],
            )
        if res["res_type"] == RES_TYPE_GROUP:
            await self.async_group_publish(res["res_id"], {"2": "at_home"})
        else:
            await self.async_write_setting(
                res["res_id"], res["pid"] or "", {"2": "at_home"}, gw_id=res["gw_id"]
            )
        return {"target": target, "restored": True}

    async def async_validate(self) -> list[dict]:
        """Login + a control read (homes). Raises CloudAuthError/CloudError on failure."""
        await self.async_login()
        return await self.async_get_homes()

    async def async_close(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None
