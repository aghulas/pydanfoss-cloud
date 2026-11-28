"""Room linkage of a boiler relay written by the standalone bridge (``danfoss2mqtt``).

A boiler relay (``014G2400``) closes when one of its **linked** rooms asks for heat. Which rooms
drive it is account configuration, set in the app; ``m.life.solution.wkf.reference.edit`` links or
unlinks one room per call (``CloudClient.async_relay_bind``). Linking or unlinking a room changes
the boiler's heat demand, so the bridge's write is **closed by default** (``RelayLinksGate``) and
opens per the supervised first-write procedure: on one designated relay first (``trial_relay``),
then on every relay once that trial passed.

``/set`` contract (``<base>/<relay>/set``)::

    {"room_link": {"room_id": "<Tuya room id from bridge/relays>", "linked": true}}

Pure logic only (no I/O): the bridge daemon does the reads and the write.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .client import CloudWriteError

ROOM_LINK_KEY = "room_link"


@dataclass(frozen=True)
class RelayLinksGate:
    """Whether the bridge writes a relay's room linkage, and on which relays.

    ``enabled`` False (the default): no linkage is written, ``room_link`` stays refused.
    ``trial_relay`` set: only that relay (by device id) can be written — the supervised first write.
    """

    enabled: bool = False
    trial_relay: str | None = None

    def open_for(self, dev_id: str) -> bool:
        return self.enabled and (not self.trial_relay or self.trial_relay == dev_id)


@dataclass(frozen=True)
class RoomLink:
    """A validated ``room_link`` request: link (``linked`` True) or unlink one room."""

    room_id: str
    linked: bool


def parse_room_link(raw: Any) -> RoomLink:
    """Validate the ``room_link`` value of a ``/set`` payload.

    Raises ``CloudWriteError`` unless it is ``{"room_id": <non-empty str or int>, "linked": bool}``
    (a string "true" or a number is refused: a heat-demand change is never guessed).
    """
    if not isinstance(raw, Mapping):
        raise CloudWriteError(f"room_link expects an object, got {type(raw).__name__}")
    room_id = raw.get("room_id")
    if isinstance(room_id, bool) or not isinstance(room_id, str | int) or str(room_id) == "":
        raise CloudWriteError("room_link.room_id must be a non-empty room id")
    linked = raw.get("linked")
    if not isinstance(linked, bool):
        raise CloudWriteError(f"room_link.linked expects a boolean, got {linked!r}")
    return RoomLink(room_id=str(room_id).strip(), linked=linked)


def check_room_link(request: RoomLink, rooms: list[dict[str, Any]]) -> bool:
    """Check ``request`` against a FRESH read of the relay's rooms; True when a write is needed.

    Returns False when the room is already in the asked state (nothing to send). Raises
    ``CloudWriteError`` when the room is not one the relay lists, or when the request would unlink
    the relay's last linked room (the boiler would then never be asked for heat: done in the app,
    on purpose, not from a dashboard).
    """
    room = next((r for r in rooms if str(r.get("room_id")) == request.room_id), None)
    if room is None:
        raise CloudWriteError("room is not one of this relay's eligible rooms")
    if bool(room.get("linked")) == request.linked:
        return False
    if not request.linked and sum(1 for r in rooms if r.get("linked")) <= 1:
        raise CloudWriteError("refused: it is the relay's last linked room")
    return True


def link_confirmed(request: RoomLink, rooms: list[dict[str, Any]]) -> bool:
    """True when a read of the relay's rooms shows the room in the asked state."""
    return any(
        str(r.get("room_id")) == request.room_id and bool(r.get("linked")) == request.linked
        for r in rooms
    )
