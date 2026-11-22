"""Message center of the Danfoss Ally app (the Tuya message center), read only.

Route and format: docs/message-center.md. ``smartlife.m.msg.list`` v2.0 returns every message of
the account, newest first, and ignores any filter/paging parameter; this module turns its raw
elements into ``DanfossMessage`` values and decides what counts as a device alert.

Identifiers: the message id is kept (dedup key) and the source devId is kept (to map the message
to its HA device), but neither is ever exposed in an entity attribute, an event or the
diagnostics (see ``public_dict``). The account uid, home id, scene id and URLs are dropped here.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# Wire method and version of the message list (thing.m.msg.list in the app's SDK).
MSG_LIST_API = "smartlife.m.msg.list"
MSG_LIST_VERSION = "2.0"

CATEGORY_ALERT = "alert"
CATEGORY_HOME = "home"
CATEGORY_SYSTEM = "system"
CATEGORY_OTHER = "other"
CATEGORIES = (CATEGORY_ALERT, CATEGORY_HOME, CATEGORY_SYSTEM, CATEGORY_OTHER)

# msgType values seen on the real account (docs/message-center.md): 4 = low battery (an alarm),
# -1 = home message (device added/removed, firmware update), 0 = system (new login).
_MSG_TYPE_HOME = -1
_MSG_TYPE_SYSTEM = 0


@dataclass(frozen=True, slots=True)
class DanfossMessage:
    """One message of the app's message center."""

    message_id: str
    time: int  # epoch seconds (UTC)
    msg_type: int | None
    type_label: str | None
    code: str | None
    title: str | None
    content: str | None
    source_id: str | None  # devId of the device concerned (never exposed)
    read_status: int | None  # raw ``readStatus``: always 0 so far, NOT a read flag (unused)
    big_msg_type: int | None  # raw ``bigMsgType``: 1 on device alarms

    @property
    def alarm(self) -> bool:
        return self.big_msg_type == 1

    @property
    def category(self) -> str:
        """``alert`` (device alarm: low battery, offline…), ``home``, ``system`` or ``other``.

        Tuya alarms carry a positive ``msgType`` (4 = low battery) and ``bigMsgType == 1``; either
        is enough, so an alarm template not seen yet (offline, fault) still counts as an alert.
        New logins (``msgType`` 0, which include this integration's own) and home messages (-1)
        are never alerts.
        """
        if self.alarm or (self.msg_type is not None and self.msg_type > 0):
            return CATEGORY_ALERT
        if self.msg_type == _MSG_TYPE_HOME:
            return CATEGORY_HOME
        if self.msg_type == _MSG_TYPE_SYSTEM:
            return CATEGORY_SYSTEM
        return CATEGORY_OTHER

    @property
    def is_alert(self) -> bool:
        return self.category == CATEGORY_ALERT

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.time, tz=UTC)

    def public_dict(self, device_name: str | None = None) -> dict[str, Any]:
        """The message without any identifier, for attributes / events.

        ``msg_type`` / ``big_msg_type`` are the raw cloud values, so a user can filter on a type
        this module does not name yet. No read flag: ``readStatus`` does not mean "read".
        """
        return {
            "time": self.timestamp.isoformat(),
            "category": self.category,
            "msg_type": self.msg_type,
            "big_msg_type": self.big_msg_type,
            "type": self.type_label,
            "code": self.code,
            "title": self.title or self.type_label,
            "message": self.content,
            "device": device_name,
        }


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def parse_message(raw: Any) -> DanfossMessage | None:
    """One raw element -> ``DanfossMessage``; None when it has no usable id or time.

    ``idStr`` is preferred: ``id`` is an integer above 2^53, which some JSON stacks round.
    """
    if not isinstance(raw, dict):
        return None
    message_id = _str(raw.get("idStr"))
    if message_id is None:
        rid = _int(raw.get("id"))
        message_id = str(rid) if rid is not None else None
    when = _int(raw.get("time"))
    if message_id is None or when is None or when <= 0:
        return None
    return DanfossMessage(
        message_id=message_id,
        time=when,
        msg_type=_int(raw.get("msgType")),
        type_label=_str(raw.get("msgTypeContent")),
        code=_str(raw.get("msgCode")),
        title=_str(raw.get("msgTitle")),
        content=_str(raw.get("msgContent")),
        source_id=_str(raw.get("msgSrcId")),
        read_status=_int(raw.get("readStatus")),
        big_msg_type=_int(raw.get("bigMsgType")),
    )


def parse_messages(raw: Any) -> list[DanfossMessage]:
    """Raw ``msg.list`` answer -> messages, newest first, duplicates removed.

    Tolerates the paged shape (``{"datas": [...]}``) of the later versions, and drops malformed
    elements rather than failing the whole list.
    """
    items = raw.get("datas") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        return []
    seen: set[str] = set()
    out: list[DanfossMessage] = []
    for item in items:
        msg = parse_message(item)
        if msg is None or msg.message_id in seen:
            continue
        seen.add(msg.message_id)
        out.append(msg)
    out.sort(key=lambda m: m.time, reverse=True)
    return out


def id_digest(message_id: str) -> str:
    """Stable digest of a message id, so the stored mark holds no cloud identifier."""
    return hashlib.sha256(message_id.encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class MessageMark:
    """High-water mark of the message center: newest ``time`` seen and the (digested) ids at that
    exact second (several messages can share one, e.g. three heads removed together).

    New = strictly newer than ``time``, or at ``time`` with an id not in ``ids``. ``readStatus``
    is never used: it is 0 on every message and we never call ``read.*``.
    """

    time: int
    ids: frozenset[str]

    @classmethod
    def of(cls, messages: list[DanfossMessage]) -> MessageMark | None:
        if not messages:
            return None
        newest = max(m.time for m in messages)
        return cls(newest, frozenset(id_digest(m.message_id) for m in messages if m.time == newest))

    def is_new(self, msg: DanfossMessage) -> bool:
        return msg.time > self.time or (
            msg.time == self.time and id_digest(msg.message_id) not in self.ids
        )

    def advance(self, messages: list[DanfossMessage]) -> MessageMark:
        """The mark after ``messages`` were read (never moves backwards)."""
        newer = MessageMark.of([m for m in messages if m.time >= self.time])
        if newer is None or newer.time < self.time:
            return self
        if newer.time == self.time:
            return MessageMark(self.time, self.ids | newer.ids)
        return newer

    def as_dict(self) -> dict[str, Any]:
        return {"time": self.time, "ids": sorted(self.ids)}

    @classmethod
    def from_dict(cls, raw: Any) -> MessageMark | None:
        if not isinstance(raw, dict):
            return None
        when = _int(raw.get("time"))
        ids = raw.get("ids")
        if when is None or not isinstance(ids, list):
            return None
        return cls(when, frozenset(str(i) for i in ids))
