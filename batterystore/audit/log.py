"""运行审计。

``AuditLog`` 是事件总线的全量订阅者：组件只发布事件，日志把它们落成流水并
保留内存副本。控制台读内存副本，跨重启回放读落盘流水；两份数据都来自同一个
:class:`AuditRecord` 结构，字段口径一致。
"""

from __future__ import annotations

import threading

from ..clock import Clock
from ..eventbus import Event, EventBus
from ..store.journal import JsonlJournal
from .mark import index_operations
from .query import query_entries
from .record import AuditRecord
from .report import build_report
from .snapshot import AuditSnapshot
from .summary import summarize


class AuditLog:
    """运行记录：内存副本 + 分段落盘。"""

    def __init__(self, clock: Clock, bus: EventBus, journal: JsonlJournal, memory_limit: int = 4096) -> None:
        self._clock = clock
        self._journal = journal
        self._entries: list[AuditRecord] = []
        self._memory_limit = max(1, memory_limit)
        self._lock = threading.RLock()
        bus.subscribe("*", self._on_event)

    @property
    def journal(self) -> JsonlJournal:
        return self._journal

    def _on_event(self, event: Event) -> None:
        severity = "notice"
        payload = dict(event.payload)
        if "severity" in payload:
            severity = str(payload.pop("severity"))
        station = str(payload.pop("station", ""))
        message = str(payload.pop("message", ""))
        self.record(
            AuditRecord.create(
                self._clock.now(), station, event.component, event.action, severity, message, **payload
            )
        )

    def record(self, record: AuditRecord) -> dict:
        """记一条流水（内存 + 落盘），返回其字典形式。"""

        with self._lock:
            self._entries.append(record)
            if len(self._entries) > self._memory_limit:
                del self._entries[: len(self._entries) - self._memory_limit]
            self._journal.append(record.as_dict())
        return record.as_dict()

    def entries(self, limit: int = 100) -> list[dict]:
        """返回最近的记录（最新的在前）。"""

        with self._lock:
            selected = list(self._entries)[-limit:]
        return [entry.as_dict() for entry in reversed(selected)]

    def query(
        self,
        *,
        component: str | None = None,
        severity: str | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """按条件过滤记录。"""

        with self._lock:
            selected = list(self._entries)
        return query_entries(selected, component=component, severity=severity, action=action, limit=limit)

    def tail(self, limit: int = 20) -> list[dict]:
        """返回最近的记录（按时间升序）。"""

        with self._lock:
            selected = list(self._entries)[-limit:]
        return [entry.as_dict() for entry in selected]

    def alarms(self) -> list[dict]:
        """返回全部告警档记录。"""

        with self._lock:
            selected = [entry for entry in self._entries if entry.is_alarm()]
        return [entry.as_dict() for entry in selected]

    def records(self, limit: int = 200) -> list[AuditRecord]:
        """返回最近的记录对象（按时间升序），供时间线构造使用。"""

        with self._lock:
            return list(self._entries)[-limit:]

    def operations(self, limit: int = 20) -> list[dict]:
        """返回按操作标识归并的区间。"""

        return index_operations(self.records(self._memory_limit), limit)

    def summary(self) -> dict:
        """返回审计汇总计数。"""

        with self._lock:
            entries = list(self._entries)
        report = summarize(entries)
        report["journal_count"] = self._journal.count()
        report["active_segment"] = self._journal.active_segment()
        health = self._journal.health()
        report["journal_corrupt"] = health["corrupt"]
        report["partial_segments"] = len(health["partial_segments"])
        return report

    def report(self) -> dict:
        """返回带时间跨度的审计报告。"""

        with self._lock:
            entries = list(self._entries)
        return build_report(entries)

    def snapshot(self) -> AuditSnapshot:
        """返回审计状态快照。"""

        with self._lock:
            entries = list(self._entries)
        return AuditSnapshot(
            summary=summarize(entries),
            report=build_report(entries, recent=5),
            operations=index_operations(entries, limit=10),
            journal_count=self._journal.count(),
            active_segment=self._journal.active_segment(),
            segments=len(self._journal.segments()),
        )

    def journal_health(self) -> dict:
        """返回流水恢复/损坏健康结论，供控制台展示。"""

        return self._journal.health()

    def replay_from_journal(self, limit: int = 100_000) -> list[dict]:
        """从落盘流水回放记录，供跨重启恢复使用。

        走显式回放：残缺段有效前缀参与，损坏隔离段不参与；健康结论可通过
        :meth:`journal_health` 单独取，回放本身不会因坏段抛异常。
        """

        return self._journal.replay()["records"][-limit:]

    def replay_all(self) -> list[dict]:
        """严格读取完整活段流水；存在损坏空档时给出明确的损坏结论。

        与 :meth:`replay_from_journal` 的区别：后者用于启动恢复，容忍残缺段前缀
        并跳过隔离段；本方法供"我就要一份完整流水"的调用方，宁可报错也不给
        缺段数据。
        """

        return self._journal.read_all()

    def replay_segment(self, index: int) -> list[dict]:
        """按段号回放一段流水。"""

        return self._journal.read_segment(index)
