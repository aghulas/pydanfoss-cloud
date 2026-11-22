"""Tuya mobile API signature (Danfoss Ally) - reverse-engineered, verified (see danfoss-ally-mcp).

    sign = HMAC_SHA256(key, canonical)                         (hex, 64 chars)
    canonical = "||".join("<k>=<v>" for k in sorted(params))   # sorted keys
    postData (in the canonical) = post_data_md5(wire value)    # not the cleartext body

The HMAC key is composed: "com.danfoss.ally_<deviceId hex-colon>_<appSecret1>_<appSecret2>".
These are SECRETS: supplied at runtime, never hard-coded or logged.
"""

from __future__ import annotations

import hashlib
import hmac as _hmac

# Keys kept in the signed canonical (authoritative list from the decompiled SDK).
SIGN_WHITELIST = frozenset(
    (
        "a",
        "v",
        "lat",
        "lon",
        "lang",
        "deviceId",
        "appVersion",
        "ttid",
        "isH5",
        "h5Token",
        "os",
        "clientId",
        "postData",
        "time",
        "requestId",
        "et",
        "n4h5",
        "sid",
        "chKey",
        "sp",
    )
)


def canonical_string(params: dict) -> str:
    """Canonical string: `key=value` sorted, joined by `||`, limited to SIGN_WHITELIST.

    The ``postData`` field must ALREADY contain ``post_data_md5(body)``, not the raw body.
    """
    return "||".join(
        f"{k}={params[k]}"
        for k in sorted(params)
        if k in SIGN_WHITELIST and params[k] is not None and params[k] != ""
    )


def canonical_from_request(params: dict, post_data_wire: str | bytes | None) -> str:
    """Canonical to sign, from the URL params and the wire `postData` value.

    Rule (including et=3): ``postData = post_data_md5(<wire postData value>)``. For et=3, the wire
    value is the encrypted blob, so NOT the md5 of the cleartext body.
    """
    from .postdata import post_data_md5  # local import: avoids any cycle

    p = dict(params)
    if post_data_wire is not None and post_data_wire != "" and post_data_wire != b"":
        p["postData"] = post_data_md5(post_data_wire)
    else:
        p.pop("postData", None)
    return canonical_string(p)


def sign_canonical(canonical: str, key: str) -> str:
    """HMAC-SHA256(key, canonical) in hexadecimal - the core of the algorithm."""
    return _hmac.new(key.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def build_key(
    device_id_colon: str, app_secret1: str, app_secret2: str, package: str = "com.danfoss.ally"
) -> str:
    """Rebuild the HMAC key from its components (deviceId hex-colon + 2 appSecrets)."""
    return "_".join([package, device_id_colon, app_secret1, app_secret2])
