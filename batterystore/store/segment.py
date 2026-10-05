"""流水段的帧封装与完整性判定。

每段是一个 JSONL 文件，每一行是一个自描述帧：

* 数据帧 ``{"f":"rec","i":<段内序号>,"n":<段满条数>,"crc32":...,"payload":{...}}``
* 封端帧 ``{"f":"seal","i":<段内条数>,"n":<段满条数>,"crc32":...}``（只标记边界）

CRC32 按 payload 的规范化 JSON 计算，掉电半截、错字节都会在解析阶段之前被
识别出来。段状态完全由文件内容判定，不依赖额外清单（清单本身也可能在掉电时
没写完）：

* ``sealed``        存在有效封端帧，段完整，内容到此为止；
* ``active``        无封端帧，但这是最后一段且未写满，正常在写，允许继续追加；
* ``partial``       无封端帧且写满（滚段边界掉电，封端帧没写成功），或尾帧
                    残缺／校验不符，或更高段号已存在——掉电留痕，单独记为残缺段，
                    绝不能当成完整的一段，也不能在原文件上接着写。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .framing import MAGIC, is_intact

FRAME_REC = "rec"
FRAME_SEAL = "seal"

STATE_SEALED = "sealed"
STATE_ACTIVE = "active"
STATE_PARTIAL = "partial"
STATE_PENDING_TAIL = "pending-tail"


@dataclass(frozen=True)
class SegmentReport:
    """一段文件的体检结果。"""

    index: int
    state: str
    valid_records: list[Any]
    valid_count: int
    sealed_count: int | None
    reason: str = ""
    bad_line: int = 0


def encode_record(payload: Any, seq: int, segment_limit: int) -> str:
    """把一条业务记录封装成数据帧文本（不含换行）。"""

    frame = {
        "magic": MAGIC,
        "f": FRAME_REC,
        "i": seq,
        "n": segment_limit,
        "crc32": _payload_crc(payload),
        "payload": payload,
    }
    return json.dumps(frame, ensure_ascii=False, sort_keys=True)


def encode_seal(count: int, segment_limit: int) -> str:
    """生成封端帧文本（不含换行）。"""

    frame = {"magic": MAGIC, "f": FRAME_SEAL, "i": count, "n": segment_limit}
    return json.dumps(frame, ensure_ascii=False, sort_keys=True)


def inspect_segment(
    index: int,
    raw: bytes | str,
    segment_limit: int,
    *,
    is_last: bool,
    live_active: bool = False,
) -> SegmentReport:
    """逐帧体检一段内容，返回状态与校验通过的有效记录。

    判定不依赖"是否干净关机"标志：从盘上字节就能推出结论。

    ``live_active`` 表示这是对**运行中在写段**的在线读取（而非重启体检）。此时
    尾部出现一条没有换行收尾的行，属于"并发 append 还没落完"，记为
    ``pending-tail``（读者忽略它即可），不能当成损坏去隔离；重启体检绝不传该
    标志，所以掉电残帧仍严格判 ``partial``。
    """

    lines = raw.split(b"\n") if isinstance(raw, bytes) else raw.split("\n")
    valid: list[Any] = []
    sealed_count: int | None = None
    expected_seq = 0

    # 显式按 "\n" 切分：末尾若非空串，说明最后一行没有换行收尾。完整帧落盘带
    # 换行，因此它要么是掉电写了一半，要么是并发 append 未落完。
    unterminated = bool(lines) and bool(lines[-1])
    pending_tail = False

    for line_no, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line:
            continue
        is_tail = line_no == len(lines)
        try:
            frame = json.loads(line)
        except json.JSONDecodeError:
            if is_tail and unterminated and live_active:
                pending_tail = True
                continue
            # 末尾无换行收尾的半行，无论 JSON 能否解析都是掉电写了一半。
            reason = "torn-tail" if is_tail and unterminated else "torn-frame"
            return _partial(index, valid, sealed_count, reason, line_no)
        if not isinstance(frame, dict) or frame.get("magic") != MAGIC or frame.get("f") not in (
            FRAME_REC,
            FRAME_SEAL,
        ):
            # 升级前的旧式裸 JSONL 行：无法校验，按有效但未封端的记录放行。
            if _looks_legacy(frame):
                valid.append(frame)
                expected_seq += 1
                continue
            return _partial(index, valid, sealed_count, "bad-frame", line_no)

        if frame["f"] == FRAME_SEAL:
            if is_tail and unterminated:
                if live_active:
                    # 封端帧正在写：读者按未封端处理，下一轮读即一致。
                    pending_tail = True
                    continue
                # 重启视角：封端帧写了一半，边界动作未完成，必须记残缺。
                return _partial(index, valid, None, "seal-torn", line_no)
            if int(frame.get("i", -1)) != len(valid):
                return _partial(index, valid, sealed_count, "bad-seal", line_no)
            sealed_count = len(valid)
            continue

        if is_tail and unterminated:
            if live_active:
                # 帧 JSON 完整但换行还没落：尚未提交，不纳入有效记录。
                pending_tail = True
                continue
            return _partial(index, valid, sealed_count, "torn-tail", line_no)
        payload = frame.get("payload")
        if int(frame.get("i", -1)) != expected_seq:
            return _partial(index, valid, sealed_count, "sequence-gap", line_no)
        if not is_intact(payload, str(frame.get("crc32", ""))):
            return _partial(index, valid, sealed_count, "checksum-mismatch", line_no)
        valid.append(payload)
        expected_seq += 1

    if sealed_count is not None:
        if sealed_count != len(valid):
            return _partial(index, valid, sealed_count, "seal-count-mismatch")
        return SegmentReport(index, STATE_SEALED, valid, len(valid), sealed_count)

    # 无封端帧。重启体检视角：写满即说明滚段边界上掉电（边界条件必须记残缺）。
    # 在线读取视角：写满但封端帧还没落，只是 append 与 seal 之间的一个瞬间，
    # 读完整记录即可，绝不能因此隔离一个健康段。
    if len(valid) >= segment_limit and not live_active:
        return _partial(index, valid, None, "boundary-torn")
    if not is_last and not live_active:
        return _partial(index, valid, None, "unsealed-nonlast")
    if pending_tail:
        return SegmentReport(index, STATE_PENDING_TAIL, valid, len(valid), None)
    return SegmentReport(index, STATE_ACTIVE, valid, len(valid), None)


def _partial(
    index: int,
    valid: list[Any],
    sealed_count: int | None,
    reason: str,
    bad_line: int = 0,
) -> SegmentReport:
    return SegmentReport(index, STATE_PARTIAL, valid, len(valid), sealed_count, reason, bad_line)


def _looks_legacy(frame: Any) -> bool:
    return not isinstance(frame, dict) or (
        "magic" not in frame and "f" not in frame and "crc32" not in frame
    )


def _payload_crc(payload: Any) -> str:
    from .framing import checksum

    return checksum(payload)
