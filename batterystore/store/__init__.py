"""文件型持久化：原子 JSON 文档与带校验/封口的分段 JSONL 流水。"""

from __future__ import annotations

from .index import summarize_documents
from .journal import (
    JsonlJournal,
    QUARANTINE_POLICY,
    REBUILD_POLICY,
)
from .jsonfile import JsonStore

__all__ = [
    "JsonStore",
    "JsonlJournal",
    "QUARANTINE_POLICY",
    "REBUILD_POLICY",
    "summarize_documents",
]
