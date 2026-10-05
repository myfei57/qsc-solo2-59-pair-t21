"""掉电恢复：原子文档、隔离、重建策略与并发可见性。"""

from __future__ import annotations

import json
import os
import threading

import pytest

from batterystore.errors import DocumentCorruptError
from batterystore.store.jsonfile import JsonStore


def _write_garbage(store: JsonStore, doc_id: str, raw: bytes) -> str:
    path = store.path_of(doc_id)
    with open(path, "wb") as handle:
        handle.write(raw)
    return path


def test_corrupt_document_is_quarantined_not_parsed(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("platform_state", {"ok": True})
    path = _write_garbage(store, "platform_state", b'{"magic":"BSV1","crc32":"x","pay')

    with pytest.raises(DocumentCorruptError) as info:
        store.read("platform_state")
    assert info.value.reason == "json-decode-error"
    assert not os.path.isfile(path)
    assert os.path.isfile(info.value.quarantine_path)
    assert os.path.isfile(info.value.quarantine_path + ".meta.json")
    assert store.exists("platform_state") is False
    assert store.quarantined()[0]["reason"] == "json-decode-error"


def test_checksum_mismatch_is_corrupt(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("doc", {"v": 1})
    path = store.path_of("doc")
    with open(path, "r", encoding="utf-8") as handle:
        envelope = json.load(handle)
    envelope["payload"] = {"v": 2}  # 改内容不改校验
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(envelope, handle)

    with pytest.raises(DocumentCorruptError) as info:
        store.read("doc")
    assert info.value.reason == "checksum-mismatch"


def test_tmp_residue_swept_on_open(tmp_path):
    root = tmp_path / "state"
    store = JsonStore(str(root))
    store.write("a", {"x": 1})
    with open(root / ".doctmp.a.zzz.tmp", "wb") as handle:
        handle.write(b"half")
    JsonStore(str(root))
    assert not (root / ".doctmp.a.zzz.tmp").exists()


def test_read_or_default_after_corruption(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("a", {"x": 1})
    _write_garbage(store, "a", b"{broken")
    assert store.read_or_default("a", {"default": True}) == {"default": True}
    assert len(store.quarantined()) == 1


def test_rebuild_requires_quarantine_and_preserves_evidence(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("a", {"x": 1})

    # 正式文件还在：拒绝原地重建。
    with pytest.raises(Exception):
        store.rebuild("a", {"x": 2})

    _write_garbage(store, "a", b"{broken")
    with pytest.raises(DocumentCorruptError):
        store.read("a")

    path = store.rebuild("a", {"x": 9, "rebuilt": True})
    assert store.read("a") == {"x": 9, "rebuilt": True}
    assert os.path.isfile(path)
    # 坏原件仍在隔离区，没有被覆盖。
    assert len(store.quarantined()) == 1


def test_concurrent_writers_and_reader_never_see_half(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    bad: list = []
    stop = threading.Event()

    def reader() -> None:
        while not stop.is_set():
            value = store.read("counter")
            if value is not None and not isinstance(value.get("v"), int):
                bad.append(value)

    thread = threading.Thread(target=reader)
    thread.start()
    for i in range(150):
        store.write("counter", {"v": i})
    stop.set()
    thread.join()
    assert bad == []
    assert store.read("counter") == {"v": 149}


def test_legacy_plain_json_still_reads(tmp_path):
    root = tmp_path / "state"
    os.makedirs(root)
    with open(root / "old.json", "w", encoding="utf-8") as handle:
        json.dump({"legacy": True}, handle)
    store = JsonStore(str(root))
    assert store.read("old") == {"legacy": True}
