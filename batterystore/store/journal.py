"""分段 JSONL 流水（带校验与封端）。

流水只追加不重写，每 ``segment_limit`` 条记录滚成一段
``<topic>-00000.jsonl``、``<topic>-00001.jsonl`` …。每段末尾写一个封端帧，
段的完整性由段内帧（CRC32 + 序号 + 封端）直接判定，见
:mod:`batterystore.store.segment`。

滚段边界上掉电的处理是本模块的核心：重启体检时，写满却没有封端帧、或尾帧
残缺的段会被判定为 ``partial``——**原件原样移入隔离区留证，单独登记成残缺段，
绝不悄悄当成完整的一段，也绝不在原文件上续写**；新记录写进下一个新段。残缺段
中校验通过的前缀记录仍然可读（它们本身是完整的），通过 :meth:`read_segment`
返回，段状态在 :meth:`segment_states` 里显式标成 partial。

写读并发：append／roll 与所有读路径共用同一把可重入锁，读者要么看到调用前的
完整一份，要么看到调用后的完整一份，不会读到 append 写了一半的行（半行没有
换行收尾，即便被看到也会在体检阶段判残）。
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

from ..errors import SegmentCorruptError, StorageError
from .quarantine import quarantine_file
from .segment import (
    STATE_ACTIVE,
    STATE_PARTIAL,
    STATE_SEALED,
    SegmentReport,
    encode_record,
    encode_seal,
    inspect_segment,
)
from .syncio import fsync_dir

MAX_SEGMENTS = 4096
PARTIAL_SUFFIX = ".partial.json"


class JsonlJournal:
    """按段落写入、能识别残缺段的 JSONL 流水。"""

    def __init__(self, root: str, topic: str, segment_limit: int = 512) -> None:
        self._root = os.path.abspath(root)
        self._topic = topic
        self._segment_limit = max(1, segment_limit)
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        # 每段一份体检结论，是计数与续写位置的唯一事实来源。
        self._reports: dict[int, SegmentReport] = {}
        self._partials: dict[int, dict] = {}
        self._count = 0
        self._segment = 0
        self._recover()

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def segment_limit(self) -> int:
        return self._segment_limit

    def _segment_path(self, index: int) -> str:
        return os.path.join(self._root, f"{self._topic}-{index:05d}.jsonl")

    def _partial_path(self, index: int) -> str:
        return os.path.join(self._root, f"{self._topic}-{index:05d}{PARTIAL_SUFFIX}")

    # ------------------------------------------------------------------ 启动体检

    def _recover(self) -> None:
        """扫描全部段，识别并隔离残缺段，重建内存计数与续写位置。

        段号必须连续；遇到"活文件与残缺侧记都不存在"的空缺即到流水尽头。
        """

        existing: list[int] = []
        for index in range(MAX_SEGMENTS):
            if os.path.isfile(self._segment_path(index)) or os.path.isfile(self._partial_path(index)):
                existing.append(index)
                continue
            break

        last_index = existing[-1] if existing else -1
        reports: dict[int, SegmentReport] = {}
        for index in existing:
            side = self._partial_path(index)
            if os.path.isfile(side):
                # 上一次启动已经隔离登记过的残缺段：沿用结论，不再重复隔离。
                reports[index] = self._load_partial(side)
                continue
            report = self._inspect_path(index, self._segment_path(index), is_last=(index == last_index))
            if report.state == STATE_PARTIAL:
                reports[index] = self._quarantine_partial(report)
            else:
                reports[index] = report

        self._reports = reports
        self._partials = {
            index: {
                "segment": index,
                "state": STATE_PARTIAL,
                "reason": reports[index].reason,
                "valid_count": reports[index].valid_count,
                "bad_line": reports[index].bad_line,
                "quarantined_path": self._side_quarantine_path(self._partial_path(index)),
            }
            for index in reports
            if reports[index].state == STATE_PARTIAL
        }
        self._count = sum(report.valid_count for report in reports.values())
        self._segment = self._next_append_index(last_index, reports)

    def _inspect_path(self, index: int, path: str, *, is_last: bool) -> SegmentReport:
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except OSError as error:
            raise StorageError(f"读取流水段 {path} 失败: {error}", component="store") from error
        return inspect_segment(index, raw, self._segment_limit, is_last=is_last)

    def _quarantine_partial(self, report: SegmentReport) -> SegmentReport:
        """残缺段原件移入隔离区留证，并写残缺侧记供下次启动沿用同一结论。"""

        live = self._segment_path(report.index)
        side = self._partial_path(report.index)
        location = ""
        if os.path.isfile(live):
            qpath, _meta = quarantine_file(
                self._root,
                live,
                reason=f"partial:{report.reason}",
                detail=(
                    f"段 {report.index} 残缺：{report.reason}"
                    + (f"（第 {report.bad_line} 行）" if report.bad_line else "")
                    + f"，有效前缀 {report.valid_count} 条"
                ),
                extra={
                    "kind": "journal-partial",
                    "topic": self._topic,
                    "segment": report.index,
                    "partial_reason": report.reason,
                    "valid_count": report.valid_count,
                    "bad_line": report.bad_line,
                },
            )
            location = qpath

        meta = {
            "segment": report.index,
            "topic": self._topic,
            "state": STATE_PARTIAL,
            "reason": report.reason,
            "valid_count": report.valid_count,
            "bad_line": report.bad_line,
            "quarantined_path": location,
            "valid_records": report.valid_records,
        }
        tmp = side + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(meta, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, side)
        fsync_dir(self._root)
        return report

    def _load_partial(self, side_path: str) -> SegmentReport:
        try:
            with open(side_path, "r", encoding="utf-8") as handle:
                meta = json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            raise StorageError(f"读取残缺侧记 {side_path} 失败: {error}", component="store") from error
        return SegmentReport(
            int(meta["segment"]),
            STATE_PARTIAL,
            list(meta.get("valid_records", [])),
            int(meta.get("valid_count", 0)),
            None,
            str(meta.get("reason", "partial")),
            int(meta.get("bad_line", 0)),
        )

    def _side_quarantine_path(self, side_path: str) -> str:
        try:
            with open(side_path, "r", encoding="utf-8") as handle:
                return str(json.load(handle).get("quarantined_path", ""))
        except (OSError, json.JSONDecodeError):
            return ""

    @staticmethod
    def _next_append_index(last_index: int, reports: dict[int, SegmentReport]) -> int:
        """残缺段、封好段都开新段；只有未写满的 active 段沿用原段号续写。"""

        if last_index < 0:
            return 0
        if reports[last_index].state == STATE_ACTIVE:
            return last_index
        return last_index + 1

    # ------------------------------------------------------------------ 写入

    def append(self, record: Any) -> str:
        """追加一条记录；必要时先封端旧段再开新段，返回写入的段路径。

        顺序是"先封端、再写新段第一条"，任一步掉电，重启体检都能把边界认对。
        """

        with self._lock:
            index = self._segment
            path = self._segment_path(index)
            report = self._reports.get(index)
            seq = report.valid_count if report else 0
            try:
                if seq >= self._segment_limit:
                    self._seal(index)
                    index += 1
                    path = self._segment_path(index)
                    seq = 0
                with open(path, "a", encoding="utf-8") as handle:
                    handle.write(encode_record(record, seq, self._segment_limit) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as error:
                raise StorageError(f"追加流水失败: {error}", component="store") from error

            if index != self._segment:
                self._segment = index
            target = self._reports.get(index)
            count = (target.valid_count if target else 0) + 1
            # 活段只在内存里记条数（避免大流水常驻）；内容回放统一走盘。
            self._reports[index] = SegmentReport(index, STATE_ACTIVE, [], count, None)
            self._count += 1
            return path

    def _seal(self, index: int) -> None:
        report = self._reports[index]
        path = self._segment_path(index)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(encode_seal(report.valid_count, self._segment_limit) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        fsync_dir(self._root)
        self._reports[index] = SegmentReport(
            index, STATE_SEALED, list(report.valid_records), report.valid_count, report.valid_count
        )

    # ------------------------------------------------------------------ 读取

    def segments(self) -> list[str]:
        """返回完整（sealed）与在写（active）段的文件路径，按段号升序。

        残缺段不出现在这里——它们不是"完整的一段"；要看残缺段用
        :meth:`partial_segments`。
        """

        with self._lock:
            return [
                self._segment_path(index)
                for index in sorted(self._reports)
                if self._reports[index].state != STATE_PARTIAL
                and os.path.isfile(self._segment_path(index))
            ]

    def segment_states(self) -> list[dict]:
        """返回每一段（含残缺段）的状态、有效条数与残缺原因。"""

        with self._lock:
            result = []
            for index in sorted(self._reports):
                report = self._reports[index]
                item: dict[str, Any] = {
                    "segment": index,
                    "state": report.state,
                    "valid_count": report.valid_count,
                }
                if report.state == STATE_PARTIAL:
                    item.update(self._partials.get(index, {}))
                else:
                    item["path"] = self._segment_path(index)
                result.append(item)
            return result

    def partial_segments(self) -> list[dict]:
        """返回全部残缺段的登记（段号、原因、有效前缀条数、隔离原件路径）。"""

        with self._lock:
            return [self._partials[index] for index in sorted(self._partials)]

    def read_segment(self, index: int) -> list[dict]:
        """读取指定段。

        残缺段返回其中校验通过的前缀记录（可能为空或不足一段）；段是否残缺请
        配合 :meth:`segment_states` 判断，调用方必须明确自己读到的是残缺段。
        若读取时发现一个本应健康的段已经损坏（外部篡改／介质错误），隔离原件并
        抛出 :class:`~batterystore.errors.SegmentCorruptError`，给出确定结论。
        """

        with self._lock:
            report = self._reports.get(index)
            if report is None:
                return []
            if report.state == STATE_PARTIAL:
                return list(report.valid_records)
            path = self._segment_path(index)
            try:
                with open(path, "rb") as handle:
                    fresh = inspect_segment(
                        index,
                        handle.read(),
                        self._segment_limit,
                        is_last=(index == self._segment),
                        live_active=(index == self._segment),
                    )
            except OSError as error:
                raise StorageError(f"读取流水段 {index} 失败: {error}", component="store") from error
            if fresh.state == STATE_PARTIAL:
                return self._handle_live_corruption(fresh)
            # active / pending-tail（并发 append 未落完）/ sealed 都是健康段。
            return fresh.valid_records

    def _handle_live_corruption(self, report: SegmentReport) -> list[dict]:
        """读取时新发现的段损坏：隔离、登记残缺、抛出确定结论。"""

        self._quarantine_partial(report)
        self._reports[report.index] = report
        self._partials[report.index] = {
            "segment": report.index,
            "state": STATE_PARTIAL,
            "reason": report.reason,
            "valid_count": report.valid_count,
            "bad_line": report.bad_line,
            "quarantined_path": self._side_quarantine_path(self._partial_path(report.index)),
        }
        raise SegmentCorruptError(
            f"流水段 {report.index} 已损坏（{report.reason}），原件已隔离",
            path=self._segment_path(report.index),
            quarantine_path=self._partials[report.index]["quarantined_path"],
            reason=report.reason,
        )

    def read_all(self) -> list[dict]:
        """按段号顺序读取全部**有效**记录（含残缺段的有效前缀）。"""

        with self._lock:
            records: list[dict] = []
            for index in sorted(self._reports):
                report = self._reports[index]
                if report.state == STATE_PARTIAL:
                    records.extend(report.valid_records)
                else:
                    records.extend(self.read_segment(index))
            return records

    def count(self) -> int:
        """返回有效记录累计条数（残缺尾不计入）。"""

        with self._lock:
            return self._count

    def active_segment(self) -> int:
        """返回当前正在写入的段号。"""

        with self._lock:
            return self._segment
