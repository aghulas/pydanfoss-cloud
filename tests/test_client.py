"""Unit tests for CloudClient read helpers that don't hit the network (methods patched)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from pydanfoss_cloud.client import CloudClient
from pydanfoss_cloud.rooms import Room


def _client() -> CloudClient:
    return CloudClient(
        username="u",
        password="p",
        country_code="33",
        sign_key="sk",
        device_id="d" * 44,
        ch_key="ck",
    )


async def test_async_list_room_groups_combines_groups_and_members() -> None:
    client = _client()
    client.async_get_homes = AsyncMock(  # type: ignore[method-assign]
        return_value=[{"gid": "home1", "name": "Home 1"}, {"gid": "home2", "name": "Home 2"}]
    )

    async def _groups(gid: str | int) -> list[dict]:
        if gid == "home1":
            return [{"group_id": 5, "name": "Chapelle", "head_count": 2, "pid": "pidC"}]
        return [{"group_id": 9, "name": "Bureau", "head_count": 1, "pid": "pidB"}]

    async def _members(gid: str | int) -> dict[str, list[str]]:
        if gid == "home1":
            return {"5": ["devA", "devB"]}
        return {"9": ["devC"]}

    client.async_list_groups = AsyncMock(side_effect=_groups)  # type: ignore[method-assign]
    client.async_group_members = AsyncMock(side_effect=_members)  # type: ignore[method-assign]

    rooms = await client.async_list_room_groups()
    assert rooms == [
        Room(group_id="5", name="Chapelle", pid="pidC", home_gid="home1", members=("devA", "devB")),
        Room(group_id="9", name="Bureau", pid="pidB", home_gid="home2", members=("devC",)),
    ]


async def test_async_list_room_groups_group_without_members() -> None:
    client = _client()
    client.async_get_homes = AsyncMock(return_value=[{"gid": "h", "name": "H"}])  # type: ignore[method-assign]
    client.async_list_groups = AsyncMock(  # type: ignore[method-assign]
        return_value=[{"group_id": 7, "name": None, "pid": None}]
    )
    client.async_group_members = AsyncMock(return_value={})  # type: ignore[method-assign]

    rooms = await client.async_list_room_groups()
    # No members recorded -> empty tuple; name falls back to the group id string.
    assert rooms == [Room(group_id="7", name="7", pid=None, home_gid="h", members=())]
