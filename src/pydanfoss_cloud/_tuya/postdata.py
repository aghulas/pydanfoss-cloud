"""Encryption of the `postData` field of the Tuya mobile API (Danfoss Ally) - reverse-engineered.

For `m.*` requests (et=3), postData is not cleartext:
1. SIGNATURE: the canonical's `postData` field = ``swap_sign_string(md5_hex(body))``.
2. WIRE: ``postData`` = Base64(nonce(12) ∥ AES-128-GCM(key).encrypt(body) ∥ tag(16)).

AES key = HMAC_SHA256(requestId, sign_key + "_" + ecode).hexdigest()[:16] (ecode empty pre-login).
The response (et=3) is encrypted with the same key then gzipped. sign_key and ecode are SECRETS.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import hmac as _hmac
import os

from Crypto.Cipher import AES

GCM_NONCE_LEN = 12
GCM_TAG_LEN = 16


def swap_sign_string(s: str) -> str:
    """Swap the 8-hex blocks of an md5: B0 B1 B2 B3 -> B1 B0 B3 B2 (involution)."""
    b0, b1, b2, b3 = s[0:8], s[8:16], s[16:24], s[24:32]
    return b1 + b0 + b3 + b2


def post_data_md5(body: str | bytes) -> str:
    """Value of the `postData` field in the signed canonical = swap_sign_string(md5_hex(body))."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    return swap_sign_string(hashlib.md5(body).hexdigest())


def derive_encrypto_key(request_id: str, ecode: str, sign_key: str) -> bytes:
    """AES-128 key (16 ASCII bytes): HMAC_SHA256(requestId, sign_key[+ "_" + ecode])[:16].

    Pre-login (ecode empty/None), the hashed message is the sign_key alone.
    """
    msg = (sign_key if not ecode else sign_key + "_" + ecode).encode("utf-8")
    full = _hmac.new(request_id.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return full[:16].encode("ascii")


def encrypt_post_data(
    body: str | bytes, request_id: str, ecode: str, sign_key: str, nonce: bytes | None = None
) -> str:
    """Encrypt the cleartext body -> sent `postData` value (Base64 of nonce∥ciphertext∥tag)."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    if nonce is None:
        nonce = os.urandom(GCM_NONCE_LEN)
    if len(nonce) != GCM_NONCE_LEN:
        raise ValueError("nonce must be 12 bytes")
    key = derive_encrypto_key(request_id, ecode, sign_key)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=GCM_TAG_LEN)
    ciphertext, tag = cipher.encrypt_and_digest(body)
    return base64.b64encode(nonce + ciphertext + tag).decode("ascii")


def decrypt_post_data(b64: str, request_id: str, ecode: str, sign_key: str) -> bytes:
    """Inverse of encrypt_post_data: Base64(nonce∥ct∥tag) -> cleartext body (bytes)."""
    blob = base64.b64decode(b64)
    nonce = blob[:GCM_NONCE_LEN]
    tag = blob[-GCM_TAG_LEN:]
    ciphertext = blob[GCM_NONCE_LEN:-GCM_TAG_LEN]
    key = derive_encrypto_key(request_id, ecode, sign_key)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=GCM_TAG_LEN)
    return cipher.decrypt_and_verify(ciphertext, tag)


def build_request_body(
    url_params: dict,
    body: str | bytes | None,
    sign_key: str,
    ecode: str,
    nonce: bytes | None = None,
) -> dict:
    """Assemble the POST body (form-urlencoded) of an m.* request (et=3), fully offline.

    Returns url_params + encrypted postData (if a body) + sign. The signature covers the postData
    as sent (ciphertext), not the cleartext body.
    """
    from .sign import canonical_from_request, sign_canonical

    params = {k: v for k, v in url_params.items() if v is not None}
    request_id = params.get("requestId")
    if not request_id:
        raise ValueError("url_params must contain 'requestId'")
    out = dict(params)
    wire = None
    if body is not None and body != "" and body != b"":
        wire = encrypt_post_data(body, request_id, ecode, sign_key, nonce=nonce)
        out["postData"] = wire
    canonical = canonical_from_request(params, wire)
    out["sign"] = sign_canonical(canonical, sign_key)
    return out


def decrypt_response(
    b64_result: str, request_id: str, ecode: str, sign_key: str, gzipped: bool = True
) -> bytes:
    """Decrypt the `result` field of an encrypted response (et=3): GCM then gunzip."""
    raw = decrypt_post_data(b64_result, request_id, ecode, sign_key)
    if gzipped:
        try:
            return gzip.decompress(raw)
        except (OSError, EOFError):
            return raw
    return raw
