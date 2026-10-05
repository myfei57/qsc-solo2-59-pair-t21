"""文件型持久化：原子文档、校验封口流水、掉电残缺与坏段处理。"""

from __future__ import annotations

import json
import os
import threading

import pytest

from batterystore.errors import CorruptStorageError, StorageError
from batterystore.store.index import summarize_documents
from batterystore.store.journal import REBUILD_POLICY, JsonlJournal
from batterystore.store.jsonfile import JsonStore


# --------------------------------------------------------------- JsonStore


def test_json_store_roundtrip(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    path = store.write("platform_state", {"operations": 2})
    assert store.exists("platform_state")
    assert store.read("platform_state") == {"operations": 2}
    assert store.path_of("platform_state") == path
    assert store.list_ids() == ["platform_state"]
    assert summarize_documents(store)[0]["doc_id"] == "platform_state"
    assert store.remove("platform_state") is True
    assert store.read("platform_state") is None
    assert store.remove("platform_state") is False


def test_json_store_is_quarantined_not_crashed(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("doc", {"a": 1})
    with open(store.path_of("doc"), "w", encoding="utf-8") as handle:
        handle.write('{"a":')  # 半截文档

    with pytest.raises(CorruptStorageError) as caught:
        store.read("doc")
    # 明确结论 + 证据路径，坏原件原样保留。
    assert caught.value.quarantine_path
    assert os.path.isfile(caught.value.quarantine_path)
    assert not store.exists("doc")
    assert store.inspect("doc")["state"] == "quarantined"
    # 隔离后再读就是普通缺失，不再二次抛错。
    assert store.read("doc") is None


def test_json_store_rebuild_is_explicit_and_preserves_evidence(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    store.write("doc", {"v": 1})
    with open(store.path_of("doc"), "w", encoding="utf-8") as handle:
        handle.write("{坏")
    with pytest.raises(CorruptStorageError):
        store.read("doc")
    # 活文件还健康时禁止重建（防覆盖好数据）。
    store.write("other", {"v": 2})
    with pytest.raises(StorageError):
        store.recover_document("other", {"v": 3})
    # 显式拿有效内容重建损坏文档。
    path = store.recover_document("doc", {"v": 9})
    assert store.read("doc") == {"v": 9}
    assert os.path.isfile(path)
    report = store.quarantine_report()
    reasons = [entry["reason"] for entry in report]
    assert any("损坏" in reason for reason in reasons)
    assert any(reason == "rebuilt" for reason in reasons)
    # 坏原件仍在隔离区，没有被覆盖。
    quarantine_dir = os.path.join(store.root, ".quarantine")
    assert any(name.endswith("doc.json") for name in os.listdir(quarantine_dir))


def test_stale_tempfile_from_power_loss_is_quarantined(tmp_path):
    root = str(tmp_path / "state")
    os.makedirs(root)
    with open(os.path.join(root, ".doc.leftover.tmp"), "w", encoding="utf-8") as handle:
        handle.write('{"半截":')
    store = JsonStore(root)
    assert not any(name.endswith(".tmp") for name in os.listdir(root))
    assert store.quarantine_report()
    # 临时文件残留不影响正常文档读写。
    assert store.write("ok", {"x": 1}).endswith("ok.json")
    assert store.read("ok") == {"x": 1}


# --------------------------------------------------------------- 滚段与封口


def test_journal_rolls_segments(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=2)
    journal.append({"i": 0})
    journal.append({"i": 1})
    journal.append({"i": 2})
    assert journal.count() == 3
    assert journal.topic == "audit"
    assert journal.active_segment() == 1
    assert len(journal.segments()) == 2
    assert journal.read_segment(0) == [{"i": 0}, {"i": 1}]
    assert journal.read_all()[-1] == {"i": 2}
    assert journal.read_segment(9) == []


def test_clean_restart_keeps_active_segment(tmp_path):
    root = str(tmp_path / "state")
    first = JsonlJournal(root, "audit", segment_limit=2)
    for i in range(3):
        first.append({"i": i})
    # 模拟干净停机后重启：活动段每帧完整，不应被误判成残缺段。
    second = JsonlJournal(root, "audit", segment_limit=2)
    assert second.health()["partial_segments"] == []
    assert second.active_segment() == 1
    assert second.read_all() == [{"i": 0}, {"i": 1}, {"i": 2}]
    second.append({"i": 3})
    assert second.read_all()[-1] == {"i": 3}


def test_active_segment_torn_tail_is_marked_partial(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    for i in range(3):
        journal.append({"i": i})
    active = journal.segments()[1]
    raw = open(active, "rb").read()
    with open(active, "wb") as handle:
        handle.write(raw[:-3])  # 掉电：最后一帧写了一半

    recovered = JsonlJournal(root, "audit", segment_limit=2)
    partials = recovered.health()["partial_segments"]
    assert [(item["index"], item["records"]) for item in partials] == [(1, 1)]
    # 残缺段单独标记，不能悄悄当完整段。
    assert recovered.read_partial(1) == [{"i": 2}]
    assert 1 not in [item["index"] for item in recovered.health()["segments"]]
    # 流水滚段能接着写，新写入不复用残缺段号。
    recovered.append({"i": 3})
    assert recovered.active_segment() == 2
    assert recovered.read_all() == [{"i": 0}, {"i": 1}, {"i": 3}]
    # 显式回放仍包含残缺前缀。
    assert recovered.replay()["records"] == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
    # 原件与元数据都留在 .partial/。
    partial_dir = os.path.join(root, ".partial")
    names = os.listdir(partial_dir)
    assert any(name.endswith(".jsonl") for name in names)
    assert any(name.endswith(".partial.json") for name in names)


def test_roll_boundary_torn_seal_is_partial_even_when_next_segment_exists(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    for i in range(3):
        journal.append({"i": i})  # 段0已封口，段1持有 i=2
    seg0 = journal._segment_path(0)
    lines = open(seg0, "rb").read().split(b"\n")
    # 段0的封口帧只写进一半：掉电发生在滚段边界上。
    with open(seg0, "wb") as handle:
        handle.write(b"\n".join(lines[:2]) + b"\n" + lines[2][:8])

    recovered = JsonlJournal(root, "audit", segment_limit=2)
    partials = recovered.health()["partial_segments"]
    assert len(partials) == 1
    assert partials[0]["index"] == 0
    assert "封口" in partials[0]["reason"]
    assert partials[0]["records"] == 2
    # 后一段不受影响，继续作为活动段。
    assert recovered.active_segment() == 1
    assert recovered.read_all() == [{"i": 2}]
    assert recovered.replay()["records"] == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert not recovered.health()["corrupt"]


def test_empty_active_segment_after_power_loss_is_partial(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    journal.append({"z": 1})
    with open(journal._segment_path(1), "wb"):
        pass  # 段文件创建出来但一帧都没写成
    recovered = JsonlJournal(root, "audit", segment_limit=2)
    assert [(p["index"], p["records"]) for p in recovered.health()["partial_segments"]] == [(1, 0)]
    assert recovered.read_all() == [{"z": 1}]
    recovered.append({"z": 2})
    assert recovered.read_all() == [{"z": 1}, {"z": 2}]


# --------------------------------------------------------------- 损坏处理


def _corrupt_first_segment(root: str, frame_index: int) -> None:
    target = os.path.join(root, "audit-00000.jsonl")
    lines = open(target, "rb").read().split(b"\n")
    frame = json.loads(lines[frame_index])
    frame["v"] = {"k": 99}  # 改业务值不更新 crc
    lines[frame_index] = json.dumps(frame, ensure_ascii=False).encode()
    with open(target, "wb") as handle:
        handle.write(b"\n".join(lines))


def test_corrupt_segment_is_quarantined_by_default(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    for i in range(6):
        journal.append({"k": i})
    _corrupt_first_segment(root, 0)

    recovered = JsonlJournal(root, "audit", segment_limit=2)
    health = recovered.health()
    assert health["corrupt"]
    damaged = health["quarantined_segments"][0]
    assert damaged["index"] == 0
    assert os.path.isfile(damaged["archived"])
    # 完整流水拒绝返回缺段数据。
    with pytest.raises(CorruptStorageError):
        recovered.read_all()
    with pytest.raises(CorruptStorageError):
        recovered.read_segment(0)
    # 后续完整段仍可读，回放给出健康结论而不是把服务带崩。
    assert recovered.read_segment(1) == [{"k": 2}, {"k": 3}]
    replay = recovered.replay()
    assert replay["records"] == [{"k": 2}, {"k": 3}, {"k": 4}, {"k": 5}]
    assert replay["health"]["corrupt"]


def test_corrupt_segment_rebuild_policy_truncates_to_valid_prefix(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    for i in range(6):
        journal.append({"k": i})
    _corrupt_first_segment(root, 1)  # 第2帧坏，有效前缀只有第1帧

    recovered = JsonlJournal(root, "audit", segment_limit=2, on_damage=REBUILD_POLICY)
    health = recovered.health()
    assert not health["corrupt"]
    damaged = health["quarantined_segments"][0]
    # 允许重建的条件：仅截断到最后一个校验通过的帧，且证据原件保留。
    assert damaged["records_valid"] == 1
    assert os.path.isfile(damaged["archived"])
    assert os.path.isfile(damaged["rebuilt_path"])
    assert recovered.read_segment(0) == [{"k": 0}]
    # 后续段原样保留，重建段可继续滚。
    assert recovered.read_all() == [{"k": 0}, {"k": 2}, {"k": 3}, {"k": 4}, {"k": 5}]
    recovered.append({"k": 9})
    assert recovered.read_all()[-1] == {"k": 9}


def test_invalid_damage_policy_rejected(tmp_path):
    with pytest.raises(StorageError):
        JsonlJournal(str(tmp_path), "audit", on_damage="delete-everything")


# --------------------------------------------------------------- 并发


def test_concurrent_readers_always_see_complete_frames(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=37)
    stop = False
    errors: list[Exception] = []

    def reader() -> None:
        while not stop:
            try:
                for record in journal.read_all():
                    assert isinstance(record, dict) and "n" in record
            except CorruptStorageError:
                errors.append(RuntimeError("活流水不应报损坏"))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for thread in threads:
        thread.start()
    for n in range(2000):
        journal.append({"n": n})
    stop = True
    for thread in threads:
        thread.join()
    assert not errors
    assert len(journal.read_all()) == 2000
