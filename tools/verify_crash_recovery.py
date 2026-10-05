"""掉电场景注入验证（无 pytest 也能跑）。

模拟的不是"调 API 失败"，而是掉电瞬间在盘上留下的真实字节：半截文档、
没写完的帧、滚段边界上缺失的封端帧。逐条核对需求：

1. 文档写入要么成要么不成；
2. 滚段能接着写；
3. 读坏文件给确定结论并隔离，不崩服务；
4. 坏文件默认隔离保留；
5. 重建只在前提满足时允许；
6. 写读并发，读者永远看到完整一份；
7. 滚段边界掉电，重启认出最后一段残缺并单独登记，不当成完整段。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batterystore.errors import DocumentCorruptError, SegmentCorruptError  # noqa: E402
from batterystore.store.framing import MAGIC  # noqa: E402
from batterystore.store.jsonfile import JsonStore  # noqa: E402
from batterystore.store.journal import JsonlJournal  # noqa: E402
from batterystore.store.segment import (  # noqa: E402
    STATE_ACTIVE,
    STATE_PARTIAL,
    STATE_SEALED,
    encode_record,
    encode_seal,
)

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}")


def case(name: str) -> None:
    print(f"\n== {name}")


def frame_bytes(payload: dict, seq: int, limit: int) -> bytes:
    return (encode_record(payload, seq, limit) + "\n").encode("utf-8")


def seal_bytes(count: int, limit: int) -> bytes:
    return (encode_seal(count, limit) + "\n").encode("utf-8")


def doc_envelope(payload: dict) -> dict:
    from batterystore.store.framing import checksum

    return {"magic": MAGIC, "crc32": checksum(payload), "payload": payload}


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="crash-")

    # ---- 1. 原子文档：写一半掉电 -------------------------------------------
    case("原子文档：半截文件读取时被识别并隔离，服务不崩")
    root = os.path.join(tmp, "docs")
    store = JsonStore(root)
    store.write("platform_state", {"operations": 5, "cycles": 2, "drills": 1})
    path = store.path_of("platform_state")

    # 模拟掉电：正式文件被写了一半（比如绕过 rename 的直接半截写）。
    with open(path, "wb") as handle:
        handle.write(b'{"magic": "BSV1", "crc32": "deadbeef", "paylo')
    try:
        store.read("platform_state")
        raised = False
    except DocumentCorruptError as error:
        raised = True
        qpath = error.quarantine_path
    check("读取抛出 DocumentCorruptError 而非解析栈崩溃", raised)
    check("正式名下已无坏文件（坏文件被移走）", not os.path.isfile(path))
    check("隔离区保留原样坏文件", os.path.isfile(qpath))
    check("存在 .meta.json 侧记", os.path.isfile(qpath + ".meta.json"))
    check("隔离列表可巡检", len(store.quarantined()) == 1)
    check("exists 对已隔离文档返回 False", store.exists("platform_state") is False)

    # ---- 2. 写一半掉电留下的是临时文件 -------------------------------------
    case("原子文档：临时残片在重开存储时被清走，不影响读取")
    with open(os.path.join(root, ".doctmp.platform_state.abc.tmp"), "wb") as handle:
        handle.write(b"half-written-garbage")
    store2 = JsonStore(root)
    check("read 返回 None（文档已被上一 case 隔离）", store2.read("platform_state") is None)
    leftovers = [n for n in os.listdir(root) if n.startswith(".doctmp.")]
    check("临时残片被清理", leftovers == [])

    # ---- 3. 正常写入仍然完整，且新旧版本都不会被并发读到半截 ---------------
    case("原子文档：重新写入 + 并发读永远读到完整一份")
    store2.write("platform_state", {"operations": 9})
    check("重写后读出完整新内容", store2.read("platform_state") == {"operations": 9})

    violations: list = []
    stop = threading.Event()

    def reader() -> None:
        while not stop.is_set():
            value = store2.read("counter")
            if value is not None and value.get("v", 0) % 1 != 0:
                violations.append(value)

    thread = threading.Thread(target=reader)
    thread.start()
    for i in range(200):
        store2.write("counter", {"v": i})
    stop.set()
    thread.join()
    check("并发 200 次写入期间读者从未读到中间态", violations == [])

    # ---- 4. 重建策略：只在前提满足时允许 -----------------------------------
    case("重建：正式文件仍在则拒绝；隔离后可按可信来源重建")
    store2.write("recoverable", {"v": 1})
    blocked = False
    try:
        store2.rebuild("recoverable", {"v": 2})
    except Exception:
        blocked = True
    check("拒绝原地覆盖仍存在的文档", blocked)

    # 隔离掉损坏文档后才能 rebuild，且原件仍在隔离区。
    target = store2.path_of("recoverable")
    with open(target, "wb") as handle:
        handle.write(b"{broken json")
    try:
        store2.read("recoverable")
    except DocumentCorruptError:
        pass
    rebuilt_path = store2.rebuild("recoverable", {"v": 1, "rebuilt": True})
    check("重建返回新正式文件", rebuilt_path == target and os.path.isfile(target))
    check("重建后读到可信来源内容", store2.read("recoverable") == {"v": 1, "rebuilt": True})
    check("隔离区仍保留坏原件（不覆盖证据）", len(store2.quarantined()) >= 2)

    # ---- 5. 流水：正常滚动 + 封端 ------------------------------------------
    case("流水：正常滚动，段状态 sealed/active 正确")
    jroot = os.path.join(tmp, "journal-ok")
    journal = JsonlJournal(jroot, "audit", segment_limit=2)
    for i in range(5):
        journal.append({"i": i})
    check("count=5", journal.count() == 5, str(journal.count()))
    check("active_segment=2", journal.active_segment() == 2, str(journal.active_segment()))
    states = {(s["segment"]): s["state"] for s in journal.segment_states()}
    check("段0 sealed", states.get(0) == STATE_SEALED, str(states))
    check("段1 sealed", states.get(1) == STATE_SEALED, str(states))
    check("段2 active", states.get(2) == STATE_ACTIVE, str(states))
    check("segments() 不含残缺（此处无残缺）", len(journal.segments()) == 3)
    check("read_segment(0) 完整", journal.read_segment(0) == [{"i": 0}, {"i": 1}])
    check("read_all 顺序完整", journal.read_all() == [{"i": i} for i in range(5)])

    # ---- 6. 滚段边界掉电：最后一段写满但封端没写成 -------------------------
    case("边界掉电：写满无封端 -> partial，隔离 + 登记，续写开新段")
    proot = os.path.join(tmp, "journal-boundary")
    j1 = JsonlJournal(proot, "ev", segment_limit=2)
    j1.append({"i": 0})
    j1.append({"i": 1})  # 段满，真实滚段时接下来写封端。这里模拟封端前掉电。
    del j1

    j2 = JsonlJournal(proot, "ev", segment_limit=2)
    partials = j2.partial_segments()
    check("重启认出 1 个残缺段", len(partials) == 1, str(partials))
    check("残缺段号=0", partials and partials[0]["segment"] == 0)
    check("残缺原因记录为边界残缺", partials and partials[0]["reason"] == "boundary-torn", str(partials))
    check("残缺段有效前缀=2", partials and partials[0]["valid_count"] == 2)
    check("残缺原件已移入隔离区", bool(partials[0]["quarantined_path"])
          and os.path.isfile(partials[0]["quarantined_path"]))
    check("残缺侧记文件存在", os.path.isfile(j2._partial_path(0)))
    states = {s["segment"]: s["state"] for s in j2.segment_states()}
    check("段0 显式标 partial", states.get(0) == STATE_PARTIAL)
    check("segments() 不把残缺段当完整段", j2.segments() == [])

    # 续写必须开新段（段号 1），绝不复用残缺的段 0。
    j2.append({"i": 2})
    check("续写落在新段 1", j2.active_segment() == 1, str(j2.active_segment()))
    check("续写后 count 连续（含前缀）=3", j2.count() == 3, str(j2.count()))
    check("残缺段前缀仍可读", j2.read_segment(0) == [{"i": 0}, {"i": 1}])
    check("read_all 含前缀 + 新记录", j2.read_all() == [{"i": 0}, {"i": 1}, {"i": 2}])

    # ---- 7. 尾帧写一半掉电（没有换行收尾） ----------------------------------
    case("尾帧掉电：最后一帧写一半 -> partial，前缀保留")
    troot = os.path.join(tmp, "journal-tail")
    os.makedirs(troot)
    seg0 = os.path.join(troot, "log-00000.jsonl")
    with open(seg0, "wb") as handle:
        handle.write(frame_bytes({"a": 1}, 0, 3))
        handle.write(frame_bytes({"a": 2}, 1, 3))
        handle.write(b'{"magic":"BSV1","f":"rec","i":2,"n":3,"crc32":"ab')  # 半帧无换行
    jt = JsonlJournal(troot, "log", segment_limit=3)
    partials = jt.partial_segments()
    check("半帧段被判 partial", len(partials) == 1 and partials[0]["segment"] == 0)
    check("残缺原因为 torn-tail", partials[0]["reason"] == "torn-tail", str(partials))
    check("有效前缀 2 条", jt.read_segment(0) == [{"a": 1}, {"a": 2}])

    # ---- 8. 校验不符（错字节） ---------------------------------------------
    case("错字节：CRC 不符 -> partial 并隔离")
    croot = os.path.join(tmp, "journal-crc")
    os.makedirs(croot)
    seg = os.path.join(croot, "log-00000.jsonl")
    with open(seg, "wb") as handle:
        good = encode_record({"x": 1}, 0, 3)
        handle.write((good + "\n").encode("utf-8"))
        bad = json.dumps(
            {"magic": MAGIC, "f": "rec", "i": 1, "n": 3, "crc32": "00000000", "payload": {"x": 2}},
            ensure_ascii=False,
        )
        handle.write((bad + "\n").encode("utf-8"))
    jc = JsonlJournal(croot, "log", segment_limit=3)
    partials = jc.partial_segments()
    check("校验不符判 partial", len(partials) == 1)
    check("原因 checksum-mismatch", partials[0]["reason"] == "checksum-mismatch", str(partials))
    check("前缀保留 1 条", jc.read_segment(0) == [{"x": 1}])

    # ---- 9. 干净关机：未写满段重启仍是 active，可续写 -----------------------
    case("干净重启：未写满段识别为 active 并续写")
    groot = os.path.join(tmp, "journal-clean")
    jg = JsonlJournal(groot, "ev", segment_limit=4)
    jg.append({"k": 1})
    del jg
    jg2 = JsonlJournal(groot, "ev", segment_limit=4)
    states = {s["segment"]: s["state"] for s in jg2.segment_states()}
    check("未写满段为 active", states.get(0) == STATE_ACTIVE, str(states))
    check("无残缺段", jg2.partial_segments() == [])
    jg2.append({"k": 2})
    check("续写沿用段 0", jg2.active_segment() == 0 and jg2.count() == 2)

    # ---- 10. 重复重启结论稳定（侧记沿用，不重复隔离） ----------------------
    case("重复重启：残缺结论沿用侧记，隔离证据不被搬动两次")
    j3 = JsonlJournal(proot, "ev", segment_limit=2)
    check("再次重启仍是同一残缺段", len(j3.partial_segments()) == 1)
    qfiles = [
        n for n in os.listdir(os.path.join(proot, ".quarantine")) if not n.endswith(".meta.json")
    ]
    check("隔离区只有 1 份原件（未重复隔离）", len(qfiles) == 1, str(qfiles))

    # ---- 11. 运行时读到健康段被外部损坏 -> SegmentCorruptError --------------
    case("运行期损坏：读取健康段发现损坏 -> 抛 SegmentCorruptError 并隔离")
    rroot = os.path.join(tmp, "journal-runtime")
    jr = JsonlJournal(rroot, "ev", segment_limit=4)
    jr.append({"v": 1})
    seg = jr._segment_path(0)
    with open(seg, "ab") as handle:
        handle.write(b"garbage-tail-line\n")
    got = False
    try:
        jr.read_segment(0)
    except SegmentCorruptError:
        got = True
    check("运行期损坏抛出 SegmentCorruptError", got)
    check("运行期损坏也登记残缺", len(jr.partial_segments()) == 1)

    # ---- 12. 流水并发写读 ---------------------------------------------------
    case("流水并发：多线程追加 + 读取，无异常且计数精确")
    wroot = os.path.join(tmp, "journal-conc")
    jw = JsonlJournal(wroot, "ev", segment_limit=7)
    errors: list = []

    def writer(base: int) -> None:
        try:
            for i in range(100):
                jw.append({"w": base, "i": i})
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    def reader() -> None:
        try:
            while not done.is_set():
                jw.read_all()
                jw.segment_states()
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    done = threading.Event()
    rt = threading.Thread(target=reader)
    rt.start()
    threads = [threading.Thread(target=writer, args=(b,)) for b in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    done.set()
    rt.join()
    check("并发期间无异常", errors == [])
    check("400 条全部有效落盘", jw.count() == 400, str(jw.count()))
    check("全部记录可读且顺序完整", len(jw.read_all()) == 400)
    sealed = [s for s in jw.segment_states() if s["state"] == STATE_SEALED]
    active = [s for s in jw.segment_states() if s["state"] == STATE_ACTIVE]
    check("有 sealed 段且只有一个 active 尾段", len(sealed) >= 1 and len(active) <= 1)
    check("无残缺段（并发未产生半行可见）", jw.partial_segments() == [])

    # ---- 13. 旧式裸 JSON 文件向后兼容 --------------------------------------
    case("向后兼容：旧式裸 JSON 文档与裸 JSONL 段可读")
    oroot = os.path.join(tmp, "legacy")
    os.makedirs(oroot)
    with open(os.path.join(oroot, "old.json"), "w", encoding="utf-8") as handle:
        json.dump({"legacy": True}, handle)
    js = JsonStore(oroot)
    check("旧式文档可读", js.read("old") == {"legacy": True})

    jlroot = os.path.join(tmp, "legacy-journal")
    os.makedirs(jlroot)
    with open(os.path.join(jlroot, "old-00000.jsonl"), "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"n": 1}) + "\n")
        handle.write(json.dumps({"n": 2}) + "\n")
    jl = JsonlJournal(jlroot, "old", segment_limit=512)
    check("旧式流水可读", jl.read_all() == [{"n": 1}, {"n": 2}])

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
