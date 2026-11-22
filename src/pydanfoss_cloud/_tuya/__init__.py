"""Tuya "path 1a" stack vendored from danfoss-ally-mcp (reverse-engineered, verified on real hw).

Three pure modules (stdlib + requests + pycryptodome), without the app or frida:
- ``sign``: HMAC-SHA256 signature of the canonical.
- ``postdata``: AES-GCM encryption of the postData (request and response).
- ``http``: low-level HTTP client of the ``m.*`` methods and the login flow.

To extract eventually into a shared library with the MCP connector (roadmap v0.6).
Never log sign_key, sid, ecode or chKey.
"""
