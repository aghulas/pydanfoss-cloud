"""Synthetic tests for the coordinator room store (no network, no real site data)."""

from __future__ import annotations

import pytest

from pydanfoss_cloud.room_store import RoomStore, RoomStoreError, normalize_address


def test_normalize_address_strips_prefix_and_case() -> None:
    assert normalize_address("0xABCD") == "abcd"
    assert normalize_address("  ABcd ") == "abcd"
    assert normalize_address("0x00124b00") == "00124b00"


def test_create_and_get(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    room = store.create("Living", "zigbee2mqtt", members=["0xAAAA", "0xBBBB"], room_id="r1")
    assert room.room_id == "r1"
    assert room.members == ("0xAAAA", "0xBBBB")
    assert store.get("r1").name == "Living"
    assert len(store) == 1


def test_create_requires_name_and_prefix(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    with pytest.raises(RoomStoreError):
        store.create("", "zigbee2mqtt")
    with pytest.raises(RoomStoreError):
        store.create("Living", "")


def test_create_generates_unique_id(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    a = store.create("A", "zigbee2mqtt")
    b = store.create("B", "zigbee2mqtt")
    assert a.room_id != b.room_id
    with pytest.raises(RoomStoreError):
        store.create("C", "zigbee2mqtt", room_id=a.room_id)


def test_members_deduplicated_by_normalized_address(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    room = store.create("Living", "zigbee2mqtt", members=["0xAAAA", "aaaa", "0xBBBB"])
    assert room.members == ("0xAAAA", "0xBBBB")


def test_single_owner_invariant_on_create(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    with pytest.raises(RoomStoreError, match="already belong"):
        store.create("Kitchen", "zigbee2mqtt", members=["0xaaaa"], room_id="r2")


def test_single_owner_invariant_on_set_members(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    store.create("Kitchen", "zigbee2mqtt", members=["0xBBBB"], room_id="r2")
    with pytest.raises(RoomStoreError, match="already belong"):
        store.set_members("r2", ["0xAAAA"])
    # Re-setting a room's own member is allowed.
    assert store.set_members("r1", ["0xAAAA", "0xCCCC"]).members == ("0xAAAA", "0xCCCC")


def test_rename_set_sensor_delete(tmp_path) -> None:
    store = RoomStore(tmp_path / "rooms.json")
    store.create("Living", "zigbee2mqtt", members=["0xAAAA"], room_id="r1")
    assert store.rename("r1", "Lounge").name == "Lounge"
    with pytest.raises(RoomStoreError):
        store.rename("r1", "")
    assert store.set_sensor("r1", "0xSENSOR").sensor == "0xSENSOR"
    assert store.set_sensor("r1", None).sensor is None
    store.delete("r1")
    assert len(store) == 0
    with pytest.raises(RoomStoreError):
        store.get("r1")
    with pytest.raises(RoomStoreError):
        store.delete("r1")


def test_persistence_round_trip(tmp_path) -> None:
    path = tmp_path / "rooms.json"
    store = RoomStore(path)
    store.create(
        "Living", "zigbee2mqtt", members=["0xAAAA"], sensor="0xS1", home_id="home-9", room_id="r1"
    )
    assert path.exists()

    reloaded = RoomStore(path).load()
    room = reloaded.get("r1")
    assert room.name == "Living"
    assert room.members == ("0xAAAA",)
    assert room.sensor == "0xS1"
    assert room.home_id == "home-9"


def test_absent_or_empty_file_is_empty_store(tmp_path) -> None:
    assert len(RoomStore(tmp_path / "nope.json").load()) == 0
    empty = tmp_path / "empty.json"
    empty.write_text("   ", encoding="utf-8")
    assert len(RoomStore(empty).load()) == 0


def test_pathless_store_does_not_persist() -> None:
    store = RoomStore()
    store.create("Living", "zigbee2mqtt")  # save() is a no-op, must not raise
    assert len(store) == 1
