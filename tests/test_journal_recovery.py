"""掉电恢复：分段流水的封端、残缺段识别、隔离与续写。"""

from __future__ import annotations

import json
import os
import threading

import pytest

from batterystore.errors import SegmentCorruptError
from batterystore.store.framing import MAGIC
from batterystore.store.journal import JsonlJournal
from batterystore.store.segment import (
    STATE_ACTIVE,
    STATE_PARTIAL,
    STATE_SEALED,
    encode_record,
    encode_seal,
)


def _record_line(payload: dict, seq: int, limit: int) -> bytes:
    return (encode_record(payload, seq, limit) + "\n").encode("utf-8")


def _states(journal: JsonlJournal) -> dict[int, str]:
    return {item["segment"]: item["state"] for item in journal.segment_states()}


def test_boundary_power_loss_marks_last_segment_partial(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    journal.append({"i": 0})
    journal.append({"i": 1})  # 写满；模拟封端帧落盘前掉电。
    del journal

    reopened = JsonlJournal(root, "audit", segment_limit=2)
    partials = reopened.partial_segments()
    assert len(partials) == 1
    assert partials[0]["segment"] == 0
    assert partials[0]["reason"] == "boundary-torn"
    assert partials[0]["valid_count"] == 2
    assert os.path.isfile(partials[0]["quarantined_path"])
    assert _states(reopened) == {0: STATE_PARTIAL}
    # 残缺段不能出现在"完整段"列表里。
    assert reopened.segments() == []


def test_append_after_partial_opens_new_segment(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    journal.append({"i": 0})
    journal.append({"i": 1})
    del journal

    reopened = JsonlJournal(root, "audit", segment_limit=2)
    path = reopened.append({"i": 2})  # 必须续写进新段，不复用残缺的段 0。
    assert reopened.active_segment() == 1
    assert path.endswith("audit-00001.jsonl")
    assert reopened.count() == 3
    assert reopened.read_segment(0) == [{"i": 0}, {"i": 1}]
    assert reopened.read_all() == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_torn_half_frame_without_newline_is_partial(tmp_path):
    root = tmp_path / "state"
    os.makedirs(root)
    with open(root / "log-00000.jsonl", "wb") as handle:
        handle.write(_record_line({"a": 1}, 0, 3))
        handle.write(_record_line({"a": 2}, 1, 3))
        handle.write(b'{"magic":"BSV1","f":"rec","i":2,"n":3,"cr')  # 半帧，无换行

    journal = JsonlJournal(str(root), "log", segment_limit=3)
    partials = journal.partial_segments()
    assert len(partials) == 1
    assert partials[0]["reason"] == "torn-tail"
    assert journal.read_segment(0) == [{"a": 1}, {"a": 2}]


def test_checksum_mismatch_quarantines_and_keeps_prefix(tmp_path):
    root = tmp_path / "state"
    os.makedirs(root)
    with open(root / "log-00000.jsonl", "wb") as handle:
        handle.write(_record_line({"x": 1}, 0, 3))
        bad = json.dumps(
            {"magic": MAGIC, "f": "rec", "i": 1, "n": 3, "crc32": "00000000", "payload": {"x": 2}}
        )
        handle.write((bad + "\n").encode("utf-8"))

    journal = JsonlJournal(str(root), "log", segment_limit=3)
    partials = journal.partial_segments()
    assert len(partials) == 1
    assert partials[0]["reason"] == "checksum-mismatch"
    assert journal.read_segment(0) == [{"x": 1}]


def test_clean_shutdown_keeps_underfull_segment_active(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=4)
    journal.append({"k": 1})
    del journal

    reopened = JsonlJournal(root, "audit", segment_limit=4)
    assert reopened.partial_segments() == []
    assert _states(reopened) == {0: STATE_ACTIVE}
    reopened.append({"k": 2})
    assert reopened.active_segment() == 0
    assert reopened.count() == 2


def test_sealed_segments_are_complete(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=2)
    for i in range(5):
        journal.append({"i": i})
    states = _states(journal)
    assert states[0] == STATE_SEALED
    assert states[1] == STATE_SEALED
    assert states[2] == STATE_ACTIVE
    assert len(journal.segments()) == 3


def test_repeated_restart_keeps_single_quarantine_copy(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    journal.append({"i": 0})
    journal.append({"i": 1})
    del journal
    JsonlJournal(root, "audit", segment_limit=2)
    JsonlJournal(root, "audit", segment_limit=2)
    qdir = os.path.join(root, ".quarantine")
    evidence = [n for n in os.listdir(qdir) if not n.endswith(".meta.json")]
    assert len(evidence) == 1


def test_runtime_corruption_on_read_raises_and_quarantines(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=4)
    journal.append({"v": 1})
    with open(journal._segment_path(0), "ab") as handle:
        handle.write(b"garbage-tail\n")
    with pytest.raises(SegmentCorruptError):
        journal.read_segment(0)
    assert len(journal.partial_segments()) == 1


def test_concurrent_appends_and_reads_stay_consistent(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=7)
    errors: list[Exception] = []
    done = threading.Event()

    def writer(base: int) -> None:
        try:
            for i in range(80):
                journal.append({"w": base, "i": i})
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    def reader() -> None:
        try:
            while not done.is_set():
                journal.read_all()
                journal.segment_states()
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    rt = threading.Thread(target=reader)
    rt.start()
    threads = [threading.Thread(target=writer, args=(b,)) for b in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    done.set()
    rt.join()

    assert errors == []
    assert journal.count() == 320
    assert len(journal.read_all()) == 320
    assert journal.partial_segments() == []


def test_legacy_plain_jsonl_reads(tmp_path):
    root = tmp_path / "state"
    os.makedirs(root)
    with open(root / "old-00000.jsonl", "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"n": 1}) + "\n")
        handle.write(json.dumps({"n": 2}) + "\n")
    journal = JsonlJournal(str(root), "old", segment_limit=512)
    assert journal.read_all() == [{"n": 1}, {"n": 2}]
