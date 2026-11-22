"""Low-level HTTP client of the Tuya mobile API (Danfoss Ally app), fully outside the app.

Built on ``sign`` (HMAC-SHA256) + ``postdata`` (AES-GCM). Endpoint per datacenter (EU/US/CN/IN).
The ``User-Agent: Thing-UA=...`` header is mandatory. Material required:
- SESSION: ``sid`` and ``ecode`` from the login (stable as long as the app stays logged in).
- INSTALLATION: ``sign_key``, ``deviceId``, ``chKey`` (captured once, stable).

Reverse engineering and login flow verified on real hardware (see danfoss-ally-mcp/tuya/
client_http.py). Synchronous calls (requests): the HA integration runs them in the executor.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid

import requests

from .postdata import build_request_body, decrypt_response

ENDPOINT_EU = "https://a1.tuyaeu.com/api.json"
ENDPOINT_US = "https://a1.tuyaus.com/api.json"
ENDPOINT_CN = "https://a1.tuyacn.com/api.json"
ENDPOINT_IN = "https://a1.tuyain.com/api.json"
DEFAULT_ENDPOINT = ENDPOINT_EU

ENDPOINTS = {
    "eu": ENDPOINT_EU,
    "us": ENDPOINT_US,
    "cn": ENDPOINT_CN,
    "in": ENDPOINT_IN,
}

USER_AGENT = "Thing-UA=APP/Android/1.4.3/SDK/6.2.0"
DEFAULT_CLIENT_ID = "7kypwcaqcvw3uvr7u95a"
DEFAULT_TTID = "sdk_international@7kypwcaqcvw3uvr7u95a"
DEFAULT_APP_VERSION = "1.4.3"
DEFAULT_LANG = "en_US"
DEFAULT_OS = "Android"


class TuyaApiError(RuntimeError):
    """Error returned by the cloud (envelope success=false or errorCode present)."""

    def __init__(self, code, msg, raw):
        super().__init__(f"{code}: {msg}")
        self.code = code
        self.msg = msg
        self.raw = raw


class TuyaHttpClient:
    """Low-level client of the ``m.*`` methods (read/write), fully outside the app."""

    def __init__(
        self,
        sign_key: str,
        sid: str,
        ecode: str,
        ch_key: str,
        device_id: str,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        client_id: str = DEFAULT_CLIENT_ID,
        ttid: str = DEFAULT_TTID,
        app_version: str = DEFAULT_APP_VERSION,
        lang: str = DEFAULT_LANG,
        os_name: str = DEFAULT_OS,
        timeout: float = 20.0,
    ):
        self.sign_key = sign_key
        self.sid = sid
        self.ecode = ecode
        self.ch_key = ch_key
        self.device_id = device_id
        self.endpoint = endpoint
        self.client_id = client_id
        self.ttid = ttid
        self.app_version = app_version
        self.lang = lang
        self.os_name = os_name
        self.timeout = timeout
        self.http = requests.Session()
        self.http.headers["User-Agent"] = USER_AGENT

    def _base_params(self, api: str, version: str, et: str) -> dict:
        return {
            "a": api,
            "v": version,
            "et": et,
            "os": self.os_name,
            "lang": self.lang,
            "clientId": self.client_id,
            "ttid": self.ttid,
            "appVersion": self.app_version,
            "deviceId": self.device_id,
            "sid": self.sid,
            "chKey": self.ch_key,
            "requestId": str(uuid.uuid4()),
            "time": str(int(time.time())),
        }

    def request(
        self,
        api: str,
        version: str,
        body=None,
        *,
        et: str = "3",
        raw: bool = False,
        extra_params: dict | None = None,
    ):
        """Send a signed+encrypted ``m.*`` request and return the decrypted ``result``.

        `extra_params`: unsigned URL params (outside SIGN_WHITELIST), e.g. ``gid``.
        Raises TuyaApiError if the cloud returns an error.
        """
        params = self._base_params(api, version, et)
        if extra_params:
            params.update({k: str(v) for k, v in extra_params.items()})
        request_id = params["requestId"]
        if body is not None and not isinstance(body, (str, bytes)):
            body = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        form = build_request_body(params, body, self.sign_key, self.ecode)
        resp = self.http.post(self.endpoint, data=form, timeout=self.timeout)
        resp.raise_for_status()
        env = resp.json()
        if env.get("errorCode") or env.get("success") is False:
            raise TuyaApiError(env.get("errorCode"), env.get("errorMsg"), env)
        result = env.get("result")
        if isinstance(result, str):  # et=3: encrypted result -> decrypt + gunzip
            inner = json.loads(decrypt_response(result, request_id, self.ecode, self.sign_key))
            if inner.get("success") is False or inner.get("errorCode"):
                raise TuyaApiError(inner.get("errorCode"), inner.get("errorMsg"), inner)
            return inner if raw else inner.get("result", inner)
        return env if raw else result

    def close(self):
        self.http.close()


# --- LOGIN / session bootstrap ----------------------------------------------------------------
# Email + password flow, confirmed by capture. 3 et=3 requests WITHOUT sid (pre-session) but WITH
# the installation chKey. chKey is INSTALLATION material (stable), not session material.


def md5_as_base64(s: str) -> str:
    """MD5Util.md5AsBase64 - misleading name: actually returns the MD5 in HEXADECIMAL (32 chars)."""
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def encrypt_password(password: str, public_key_decimal: str, exponent: str) -> str:
    """login passwd: HEX(RSA/ECB/PKCS1(md5_hex(password))). Public key = modulus in decimal."""
    from Crypto.Cipher import PKCS1_v1_5
    from Crypto.PublicKey import RSA

    rsa_key = RSA.construct((int(public_key_decimal), int(exponent)))
    ciph = PKCS1_v1_5.new(rsa_key).encrypt(md5_as_base64(password).encode("utf-8"))
    return ciph.hex()


def login(
    email: str,
    password: str,
    country_code: str,
    sign_key: str,
    device_id: str,
    ch_key: str,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    client_id: str = DEFAULT_CLIENT_ID,
    ttid: str = DEFAULT_TTID,
    app_version: str = DEFAULT_APP_VERSION,
    lang: str = DEFAULT_LANG,
    timeout: float = 20.0,
) -> dict:
    """Email/password LOGIN (flow confirmed by capture).

    Returns {sid, ecode, chKey, deviceId, uid}.
    `ch_key`, `sign_key`, `device_id` = installation material (captured once). Credentials are
    supplied at runtime and are never stored or logged by this function.
    """
    http = requests.Session()
    http.headers["User-Agent"] = USER_AGENT

    def _send(api, version, post):
        params = {
            "a": api,
            "v": version,
            "et": "3",
            "os": DEFAULT_OS,
            "lang": lang,
            "clientId": client_id,
            "ttid": ttid,
            "appVersion": app_version,
            "deviceId": device_id,
            "chKey": ch_key,
            "requestId": str(uuid.uuid4()),
            "time": str(int(time.time())),
        }
        request_id = params["requestId"]
        body = json.dumps(post, separators=(",", ":"), ensure_ascii=False)
        form = build_request_body(params, body, sign_key, "")  # pre-login: ecode=""
        r = http.post(endpoint, data=form, timeout=timeout)
        r.raise_for_status()
        env = r.json()
        if env.get("errorCode") or env.get("success") is False:
            raise TuyaApiError(env.get("errorCode"), env.get("errorMsg"), env)
        result = env.get("result")
        if isinstance(result, str):
            inner = json.loads(decrypt_response(result, request_id, "", sign_key))
            if inner.get("success") is False or inner.get("errorCode"):
                raise TuyaApiError(inner.get("errorCode"), inner.get("errorMsg"), inner)
            return inner.get("result", inner)
        return result

    token_bean = _send(
        "thing.m.user.username.token.get",
        "2.0",
        {"countryCode": country_code, "isUid": False, "username": email},
    )
    token = token_bean["token"]
    public_key = token_bean["publicKey"]
    exponent = token_bean.get("exponent", "65537")
    passwd = encrypt_password(password, public_key, exponent)
    user = _send(
        "thing.m.user.email.password.login",
        "3.0",
        {
            "countryCode": country_code,
            "email": email,
            "passwd": passwd,
            "options": '{"group": 1,"mfaCode": ""}',
            "token": token,
            "ifencrypt": 1,
        },
    )
    return {
        "sid": user.get("sid"),
        "ecode": user.get("ecode"),
        "chKey": ch_key,
        "deviceId": device_id,
        "uid": user.get("uid"),
        # partnerIdentity: needed to build the Tuya cloud MQTT username/user-topic (the DP-publish
        # path for isolated heads). Server-assigned, stable per account; may be absent on very old
        # responses, hence .get(). See docs/cloud-mqtt-publish.md §5.
        "partnerIdentity": user.get("partnerIdentity"),
    }
